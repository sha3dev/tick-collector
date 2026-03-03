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
import type { EventIndexCandidate, EventIndexFile, EventSelectionQuery } from "./types/event-index-types.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

type EventIndexRepositoryOptions = { folder: string };

type EventSelectionResult = { event: StoredEvent; candidate: EventIndexCandidate };

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
      try {
        const manifestRoot = this.toManifestRootFolder();
        const indexFiles: string[] = [];
        await this.collectIndexFiles(manifestRoot, indexFiles);

        for (const indexFilePath of indexFiles) {
          const raw = await readFile(indexFilePath, "utf8");
          const indexFile = this.parseIndexFile(raw);
          this.candidates.push(...indexFile.candidates);
        }

        this.isLoaded = true;
      } catch (error: unknown) {
        const errorCode = (error as NodeJS.ErrnoException).code ?? "";
        if (errorCode === "ENOENT") {
          this.isLoaded = true;
        } else {
          throw EventIndexError.fromCause(`failed loading index files from folder=${this.folder}`, error);
        }
      }
    }
  }

  private matchesQuery(candidate: EventIndexCandidate, query: EventSelectionQuery): boolean {
    const sourceMatches = candidate.source === query.source;
    const eventTypeMatches = candidate.eventType === query.eventType;
    const providerMatches = query.provider === undefined || candidate.provider === query.provider;
    const symbolMatches = query.symbol === undefined || candidate.symbol === query.symbol;
    const marketSlugMatches = query.marketSlug === undefined || candidate.marketSlug === query.marketSlug;
    const assetIdMatches = query.assetId === undefined || candidate.assetId === query.assetId;
    const isMatch = sourceMatches && eventTypeMatches && providerMatches && symbolMatches && marketSlugMatches && assetIdMatches;
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
    const sorted = [...filtered].sort((left, right) => {
      const order = this.compareCandidateDistance(left, right, query.timestamp);
      return order;
    });
    const selected = sorted[0] ?? null;
    const withinDistance = selected !== null ? Math.abs(selected.ingestedAt - query.timestamp) <= query.maxDistanceMs : false;
    const candidate = withinDistance ? selected : null;
    return candidate;
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

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public async findClosestEvent(query: EventSelectionQuery): Promise<EventSelectionResult | null> {
    let result: EventSelectionResult | null = null;
    try {
      await this.loadIndicesIfNeeded();
      const candidate = this.selectClosestCandidate(query);

      if (candidate) {
        const events = await this.loadPartEvents(candidate.partPath);
        const event = events[candidate.lineIndex] ?? null;
        if (event) {
          result = { event, candidate };
        }
      }
    } catch (error: unknown) {
      throw EventIndexError.fromCause(`failed selecting event for marketSlug=${query.marketSlug ?? "n/a"} eventType=${query.eventType}`, error);
    }
    return result;
  }

  /**
   * @section static:methods
   */

  // empty
}
