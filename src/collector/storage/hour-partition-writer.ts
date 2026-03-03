/**
 * @section imports:externals
 */

import { randomUUID } from "node:crypto";
import { writeFile } from "node:fs/promises";
import { gzipSync } from "node:zlib";

/**
 * @section imports:internals
 */

import { StorageWriteError } from "../errors/storage-write-error.ts";
import type { EventIndexCandidate, EventIndexFile } from "../query/types/event-index-types.ts";
import type { GzipRotatingWriterOptions, PartManifest } from "../types/storage-types.ts";
import type { StoredEvent } from "../types/stored-event.ts";
import { IndexManifestStore } from "./index-manifest-store.ts";
import { RestartRecoveryService } from "./restart-recovery-service.ts";

/**
 * @section consts
 */

const HOUR_TO_MS = 60 * 60 * 1000;

/**
 * @section types
 */

type ActivePartState = {
  runId: string;
  hourBucketStartAt: number;
  partSequence: number;
  partPath: string;
  indexPath: string;
  manifestPath: string;
  bytes: number;
  minIngestedAt: number;
  maxIngestedAt: number;
  eventCount: number;
  sources: Set<string>;
  eventTypes: Set<string>;
  nextLineIndex: number;
  indexCandidates: EventIndexCandidate[];
};

type HourPartitionWriterOptions = GzipRotatingWriterOptions & {
  restartRecoveryService?: RestartRecoveryService;
  indexManifestStore?: IndexManifestStore;
  runId?: string;
};

export class HourPartitionWriter {
  /**
   * @section private:attributes
   */

  private flushTimer: NodeJS.Timeout | null;

  /**
   * @section protected:attributes
   */

  // empty

  /**
   * @section private:properties
   */

  private readonly options: GzipRotatingWriterOptions;
  private readonly pendingEvents: StoredEvent[];
  private readonly restartRecoveryService: RestartRecoveryService;
  private readonly indexManifestStore: IndexManifestStore;
  private readonly runId: string;
  private activePart: ActivePartState | null;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: HourPartitionWriterOptions) {
    this.options = options;
    this.pendingEvents = [];
    this.restartRecoveryService = options.restartRecoveryService ?? RestartRecoveryService.create({ outputDir: options.outputDir });
    this.indexManifestStore = options.indexManifestStore ?? IndexManifestStore.create();
    this.runId = options.runId ?? randomUUID().slice(0, 8);
    this.activePart = null;
    this.flushTimer = null;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: HourPartitionWriterOptions): HourPartitionWriter {
    const writer = new HourPartitionWriter(options);
    return writer;
  }

  /**
   * @section private:methods
   */

  private toHourBucketStartAt(timestamp: number): number {
    const hourBucketStartAt = Math.floor(timestamp / HOUR_TO_MS) * HOUR_TO_MS;
    return hourBucketStartAt;
  }

  private toChunk(events: StoredEvent[]): Buffer {
    const lines = events.map((event) => {
      const line = JSON.stringify(event);
      return line;
    });
    const payload = lines.join("\n") + "\n";
    const compressed = gzipSync(payload);
    return compressed;
  }

  private buildManifest(activePart: ActivePartState, isClosed: boolean): PartManifest {
    const sources = Array.from(activePart.sources.values()).sort((left, right) => left.localeCompare(right));
    const eventTypes = Array.from(activePart.eventTypes.values()).sort((left, right) => left.localeCompare(right));
    const manifest: PartManifest = {
      file: activePart.partPath,
      indexFile: activePart.indexPath,
      runId: activePart.runId,
      partSequence: activePart.partSequence,
      hourBucketStartAt: activePart.hourBucketStartAt,
      isClosed,
      minIngestedAt: activePart.minIngestedAt,
      maxIngestedAt: activePart.maxIngestedAt,
      eventCount: activePart.eventCount,
      sources,
      eventTypes,
      createdAt: new Date().toISOString()
    };
    return manifest;
  }

  private buildIndexFile(activePart: ActivePartState): EventIndexFile {
    const indexFile: EventIndexFile = { candidates: activePart.indexCandidates };
    return indexFile;
  }

  private async openPart(ingestedAt: number): Promise<void> {
    const hourBucketStartAt = this.toHourBucketStartAt(ingestedAt);
    const partSequence = await this.restartRecoveryService.nextPartSequence({ hourBucketStartAt });
    const paths = await this.indexManifestStore.buildPartFilePaths({ outputDir: this.options.outputDir, hourBucketStartAt, runId: this.runId, partSequence });
    this.activePart = {
      runId: this.runId,
      hourBucketStartAt,
      partSequence,
      partPath: paths.partPath,
      indexPath: paths.indexPath,
      manifestPath: paths.manifestPath,
      bytes: 0,
      minIngestedAt: ingestedAt,
      maxIngestedAt: ingestedAt,
      eventCount: 0,
      sources: new Set<string>(),
      eventTypes: new Set<string>(),
      nextLineIndex: 0,
      indexCandidates: []
    };
  }

  private async flushPartMetadata(isClosed: boolean): Promise<void> {
    if (this.activePart) {
      const manifest = this.buildManifest(this.activePart, isClosed);
      const indexFile = this.buildIndexFile(this.activePart);
      await this.indexManifestStore.persistPartMetadata({
        manifestPath: this.activePart.manifestPath,
        indexPath: this.activePart.indexPath,
        manifest,
        indexFile
      });
    }
  }

  private async closeActivePart(): Promise<void> {
    if (this.activePart) {
      await this.flushPartMetadata(true);
      this.activePart = null;
    }
  }

  private async ensurePartForTimestamp(ingestedAt: number, nextChunkBytes: number): Promise<void> {
    const eventHourBucketStartAt = this.toHourBucketStartAt(ingestedAt);
    const hasActivePart = this.activePart !== null;
    if (!hasActivePart) {
      await this.openPart(ingestedAt);
    }
    const activeHourBucketStartAt = this.activePart?.hourBucketStartAt ?? eventHourBucketStartAt;
    const changedHour = activeHourBucketStartAt !== eventHourBucketStartAt;
    if (changedHour) {
      await this.closeActivePart();
      await this.openPart(ingestedAt);
    }
    const hasSpace = (this.activePart?.bytes ?? 0) + nextChunkBytes <= this.options.maxPartBytes || (this.activePart?.eventCount ?? 0) === 0;
    if (!hasSpace) {
      await this.closeActivePart();
      await this.openPart(ingestedAt);
    }
  }

  private updatePartStats(events: StoredEvent[]): void {
    if (this.activePart) {
      for (const event of events) {
        this.activePart.minIngestedAt = Math.min(this.activePart.minIngestedAt, event.ingestedAt);
        this.activePart.maxIngestedAt = Math.max(this.activePart.maxIngestedAt, event.ingestedAt);
        this.activePart.eventCount += 1;
        this.activePart.sources.add(event.source);
        this.activePart.eventTypes.add(event.eventType);
      }
    }
  }

  private appendIndexCandidates(events: StoredEvent[]): void {
    if (this.activePart) {
      for (const event of events) {
        const candidate: EventIndexCandidate = {
          partPath: this.activePart.partPath,
          ingestedAt: event.ingestedAt,
          sequence: event.sequence,
          lineIndex: this.activePart.nextLineIndex,
          source: event.source,
          eventType: event.eventType,
          ...(event.provider !== undefined ? { provider: event.provider } : {}),
          ...(event.symbol !== undefined ? { symbol: event.symbol } : {}),
          ...(event.marketType !== undefined ? { marketType: event.marketType } : {}),
          ...(event.marketSlug !== undefined ? { marketSlug: event.marketSlug } : {}),
          ...(event.marketStartAt !== undefined ? { marketStartAt: event.marketStartAt } : {}),
          ...(event.marketEventIndex !== undefined ? { marketEventIndex: event.marketEventIndex } : {}),
          ...(event.assetId !== undefined ? { assetId: event.assetId } : {})
        };
        this.activePart.indexCandidates.push(candidate);
        this.activePart.nextLineIndex += 1;
      }
    }
  }

  private async writeEventsChunk(events: StoredEvent[]): Promise<void> {
    const firstEvent = events[0] ?? null;
    if (firstEvent) {
      const chunk = this.toChunk(events);
      await this.ensurePartForTimestamp(firstEvent.ingestedAt, chunk.length);
      if (this.activePart) {
        await writeFile(this.activePart.partPath, chunk, { flag: "a" });
        this.activePart.bytes += chunk.length;
        this.updatePartStats(events);
        this.appendIndexCandidates(events);
        await this.flushPartMetadata(false);
      }
    }
  }

  private splitByHour(events: StoredEvent[]): StoredEvent[][] {
    const groups: StoredEvent[][] = [];
    let currentGroup: StoredEvent[] = [];
    let currentHourBucketStartAt: number | null = null;
    for (const event of events) {
      const eventHourBucketStartAt = this.toHourBucketStartAt(event.ingestedAt);
      const changedHour = currentHourBucketStartAt !== null && currentHourBucketStartAt !== eventHourBucketStartAt;
      if (changedHour && currentGroup.length > 0) {
        groups.push(currentGroup);
        currentGroup = [];
      }
      currentGroup.push(event);
      currentHourBucketStartAt = eventHourBucketStartAt;
    }
    if (currentGroup.length > 0) {
      groups.push(currentGroup);
    }
    return groups;
  }

  private async flushPendingEvents(): Promise<void> {
    const hasPendingEvents = this.pendingEvents.length > 0;
    if (hasPendingEvents) {
      const events = [...this.pendingEvents].sort((left, right) => {
        const byTime = left.ingestedAt - right.ingestedAt;
        const bySequence = byTime === 0 ? left.sequence - right.sequence : byTime;
        return bySequence;
      });
      this.pendingEvents.length = 0;
      const groups = this.splitByHour(events);
      for (const group of groups) {
        await this.writeEventsChunk(group);
      }
    }
  }

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public async start(): Promise<void> {
    this.flushTimer = setInterval(() => {
      void this.flush().catch((error: unknown) => {
        throw StorageWriteError.fromCause("failed flushing hour partition writer", error);
      });
    }, this.options.flushIntervalMs);
  }

  public append(event: StoredEvent): void {
    this.pendingEvents.push(event);
  }

  public async flush(): Promise<void> {
    try {
      await this.flushPendingEvents();
    } catch (error: unknown) {
      throw StorageWriteError.fromCause("failed to flush pending events", error);
    }
  }

  public async stop(): Promise<void> {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
    await this.flush();
    await this.closeActivePart();
  }

  /**
   * @section static:methods
   */

  // empty
}

export type { HourPartitionWriterOptions };
