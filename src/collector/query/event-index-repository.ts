/**
 * @section imports:externals
 */

import { readdir, readFile } from "node:fs/promises";
import * as path from "node:path";
import { gunzipSync } from "node:zlib";

/**
 * @section imports:internals
 */

import { EventIndexError } from "../errors/event-index-error.ts";
import type { StoredEvent } from "../types/stored-event.ts";
import type { EventIndexCandidate, EventIndexFile, EventRangeQuery, EventSelectionQuery, SymbolMarketTypeBoundsQuery } from "./types/event-index-types.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

type EventIndexRepositoryOptions = { folder: string };

type EventSelectionResult = { event: StoredEvent; candidate: EventIndexCandidate };
type SymbolMarketTypeBounds = { minIngestedAt: number | null; maxIngestedAt: number | null };

export class EventIndexRepository {
  /**
   * @section private:attributes
   */

  private isLoaded: boolean;

  /**
   * @section protected:attributes
   */

  // empty

  /**
   * @section private:properties
   */

  private readonly folder: string;
  private readonly candidates: EventIndexCandidate[];
  private readonly eventsByPartPath: Map<string, StoredEvent[]>;
  private readonly loadedIndexFilePaths: Set<string>;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: EventIndexRepositoryOptions) {
    this.folder = options.folder;
    this.candidates = [];
    this.eventsByPartPath = new Map<string, StoredEvent[]>();
    this.loadedIndexFilePaths = new Set<string>();
    this.isLoaded = false;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: EventIndexRepositoryOptions): EventIndexRepository {
    const repository = new EventIndexRepository(options);
    return repository;
  }

  /**
   * @section private:methods
   */

  private async collectIndexFiles(dirPath: string, collector: string[]): Promise<void> {
    const entries = await readdir(dirPath, { withFileTypes: true });

    for (const entry of entries) {
      const fullPath = path.join(dirPath, entry.name);
      if (entry.isDirectory()) {
        await this.collectIndexFiles(fullPath, collector);
      } else {
        const isIndexFile = entry.isFile() && entry.name.endsWith(".index.json");
        if (isIndexFile) {
          collector.push(fullPath);
        }
      }
    }
  }

  private toManifestRootFolder(): string {
    const manifestRoot = path.join(this.folder, "manifests");
    return manifestRoot;
  }

  private parseIndexFile(raw: string): EventIndexFile {
    const parsed = JSON.parse(raw) as EventIndexFile;
    return parsed;
  }

  private async loadIndicesIfNeeded(): Promise<void> {
    if (!this.isLoaded) {
      await this.refreshIndices();
      this.isLoaded = true;
    }
  }

  private sortCandidatesByDistance(filtered: EventIndexCandidate[], timestamp: number): EventIndexCandidate[] {
    const sorted = [...filtered].sort((left, right) => {
      const order = this.compareCandidateDistance(left, right, timestamp);
      return order;
    });
    return sorted;
  }

  private matchesQuery(candidate: EventIndexCandidate, query: EventSelectionQuery): boolean {
    const sourceMatches = candidate.source === query.source;
    const eventTypeMatches = candidate.eventType === query.eventType;
    const providerMatches = query.provider === undefined || candidate.provider === query.provider;
    const symbolMatches = query.symbol === undefined || candidate.symbol === query.symbol;
    const marketTypeMatches = query.marketType === undefined || candidate.marketType === query.marketType;
    const marketSlugMatches = query.marketSlug === undefined || candidate.marketSlug === query.marketSlug;
    const marketStartAtMatches = query.marketStartAt === undefined || candidate.marketStartAt === query.marketStartAt;
    const marketEventIndexMatches = query.marketEventIndex === undefined || candidate.marketEventIndex === query.marketEventIndex;
    const assetIdMatches = query.assetId === undefined || candidate.assetId === query.assetId;
    const isMatch =
      sourceMatches &&
      eventTypeMatches &&
      providerMatches &&
      symbolMatches &&
      marketTypeMatches &&
      marketSlugMatches &&
      marketStartAtMatches &&
      marketEventIndexMatches &&
      assetIdMatches;
    return isMatch;
  }

  private compareCandidateDistance(left: EventIndexCandidate, right: EventIndexCandidate, timestamp: number): number {
    const leftDelta = Math.abs(left.ingestedAt - timestamp);
    const rightDelta = Math.abs(right.ingestedAt - timestamp);
    let order = leftDelta - rightDelta;

    if (order === 0) {
      const leftIsPast = left.ingestedAt <= timestamp;
      const rightIsPast = right.ingestedAt <= timestamp;
      const leftPriority = leftIsPast ? 0 : 1;
      const rightPriority = rightIsPast ? 0 : 1;
      order = leftPriority - rightPriority;
    }

    if (order === 0) {
      order = left.sequence - right.sequence;
    }

    return order;
  }

  private selectClosestCandidate(query: EventSelectionQuery): EventIndexCandidate | null {
    const filtered = this.candidates.filter((candidate) => {
      const match = this.matchesQuery(candidate, query);
      return match;
    });
    const sorted = this.sortCandidatesByDistance(filtered, query.timestamp);
    const selected = sorted[0] ?? null;
    const withinDistance = selected !== null ? Math.abs(selected.ingestedAt - query.timestamp) <= query.maxDistanceMs : false;
    const candidate = withinDistance ? selected : null;
    return candidate;
  }

  private compareCandidateOrder(left: EventIndexCandidate, right: EventIndexCandidate): number {
    const byTime = left.ingestedAt - right.ingestedAt;
    const bySequence = byTime === 0 ? left.sequence - right.sequence : byTime;
    const byLine = bySequence === 0 ? left.lineIndex - right.lineIndex : bySequence;
    return byLine;
  }

  private parsePartEvents(fileBytes: Buffer): StoredEvent[] {
    const decompressed = gunzipSync(fileBytes).toString("utf8");
    const lines = decompressed
      .split("\n")
      .map((line) => {
        return line.trim();
      })
      .filter((line) => {
        return line.length > 0;
      });
    const events = lines.map((line) => {
      const parsed = JSON.parse(line) as StoredEvent;
      return parsed;
    });
    return events;
  }

  private async loadPartEvents(partPath: string): Promise<StoredEvent[]> {
    let events = this.eventsByPartPath.get(partPath) ?? null;
    if (events === null) {
      const bytes = await readFile(partPath);
      const parsedEvents = this.parsePartEvents(bytes);
      this.eventsByPartPath.set(partPath, parsedEvents);
      events = parsedEvents;
    }
    return events;
  }

  private isSymbolMarketMatch(candidate: EventIndexCandidate, query: SymbolMarketTypeBoundsQuery): boolean {
    const isCryptoMatch = candidate.source === "crypto" && candidate.symbol === query.symbol;
    const isPolymarketMatch = candidate.source === "polymarket" && candidate.symbol === query.symbol && candidate.marketType === query.marketType;
    const isMatch = isCryptoMatch || isPolymarketMatch;
    return isMatch;
  }

  private matchesRangeQuery(candidate: EventIndexCandidate, query: EventRangeQuery): boolean {
    const inRange = candidate.ingestedAt >= query.startTimestamp && candidate.ingestedAt < query.endTimestampExclusive;
    const symbolMarketMatch = this.isSymbolMarketMatch(candidate, { symbol: query.symbol, marketType: query.marketType });
    const matches = inRange && symbolMarketMatch;
    return matches;
  }

  private async toEventsFromCandidates(candidates: EventIndexCandidate[]): Promise<StoredEvent[]> {
    const sortedCandidates = [...candidates].sort((left, right) => {
      const order = this.compareCandidateOrder(left, right);
      return order;
    });
    const events: StoredEvent[] = [];
    for (const candidate of sortedCandidates) {
      const partEvents = await this.loadPartEvents(candidate.partPath);
      const event = partEvents[candidate.lineIndex] ?? null;
      if (event) {
        events.push(event);
      }
    }
    return events;
  }

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public async refreshIndices(): Promise<void> {
    try {
      const manifestRoot = this.toManifestRootFolder();
      const indexFiles: string[] = [];
      await this.collectIndexFiles(manifestRoot, indexFiles);
      const sortedIndexFiles = [...indexFiles].sort((left, right) => {
        const order = left.localeCompare(right);
        return order;
      });
      for (const indexFilePath of sortedIndexFiles) {
        const isLoaded = this.loadedIndexFilePaths.has(indexFilePath);
        if (!isLoaded) {
          const raw = await readFile(indexFilePath, "utf8");
          const indexFile = this.parseIndexFile(raw);
          this.candidates.push(...indexFile.candidates);
          this.loadedIndexFilePaths.add(indexFilePath);
        }
      }
    } catch (error: unknown) {
      const errorCode = (error as NodeJS.ErrnoException).code ?? "";
      const isMissingManifestFolder = errorCode === "ENOENT";
      if (!isMissingManifestFolder) {
        throw EventIndexError.fromCause(`failed loading index files from folder=${this.folder}`, error);
      }
    }
  }

  public async findClosestEvent(query: EventSelectionQuery): Promise<EventSelectionResult | null> {
    let result: EventSelectionResult | null = null;
    try {
      await this.loadIndicesIfNeeded();
      await this.refreshIndices();
      const candidate = this.selectClosestCandidate(query);

      if (candidate) {
        const events = await this.loadPartEvents(candidate.partPath);
        const event = events[candidate.lineIndex] ?? null;
        if (event) {
          result = { event, candidate };
        }
      }
    } catch (error: unknown) {
      throw EventIndexError.fromCause(`failed selecting event for eventType=${query.eventType} source=${query.source}`, error);
    }
    return result;
  }

  public async findEventsInRange(query: EventRangeQuery): Promise<StoredEvent[]> {
    let result: StoredEvent[] = [];
    try {
      await this.loadIndicesIfNeeded();
      await this.refreshIndices();
      const matchingCandidates = this.candidates.filter((candidate) => {
        const match = this.matchesRangeQuery(candidate, query);
        return match;
      });
      result = await this.toEventsFromCandidates(matchingCandidates);
    } catch (error: unknown) {
      throw EventIndexError.fromCause(
        `failed selecting events in range symbol=${query.symbol} marketType=${query.marketType} start=${query.startTimestamp} endExclusive=${query.endTimestampExclusive}`,
        error
      );
    }
    return result;
  }

  public async findBoundsForSymbolMarketType(query: SymbolMarketTypeBoundsQuery): Promise<SymbolMarketTypeBounds> {
    let result: SymbolMarketTypeBounds = { minIngestedAt: null, maxIngestedAt: null };
    try {
      await this.loadIndicesIfNeeded();
      await this.refreshIndices();
      const matchingCandidates = this.candidates.filter((candidate) => {
        const match = this.isSymbolMarketMatch(candidate, query);
        return match;
      });
      for (const candidate of matchingCandidates) {
        const nextMin = result.minIngestedAt === null ? candidate.ingestedAt : Math.min(result.minIngestedAt, candidate.ingestedAt);
        const nextMax = result.maxIngestedAt === null ? candidate.ingestedAt : Math.max(result.maxIngestedAt, candidate.ingestedAt);
        result = { minIngestedAt: nextMin, maxIngestedAt: nextMax };
      }
    } catch (error: unknown) {
      throw EventIndexError.fromCause(`failed selecting bounds for symbol=${query.symbol} marketType=${query.marketType}`, error);
    }
    return result;
  }

  /**
   * @section static:methods
   */

  // empty
}
