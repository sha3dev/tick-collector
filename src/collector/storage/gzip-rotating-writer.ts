/**
 * @section imports:externals
 */

import { mkdir, writeFile } from "node:fs/promises";
import * as path from "node:path";
import { gzipSync } from "node:zlib";

/**
 * @section imports:internals
 */

import { StorageWriteError } from "../errors/storage-write-error.ts";
import type { StoredEvent } from "../types/stored-event.ts";
import type { GzipRotatingWriterOptions, PartManifest } from "../types/storage-types.ts";

/**
 * @section consts
 */

const BYTE_UNITS = 1024;

/**
 * @section types
 */

type ActivePartState = {
  partPath: string;
  manifestPath: string;
  bytes: number;
  minIngestedAt: number;
  maxIngestedAt: number;
  eventCount: number;
  sources: Set<string>;
  eventTypes: Set<string>;
};

export class GzipRotatingWriter {
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
  private activePart: ActivePartState | null;
  private partCounter: number;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: GzipRotatingWriterOptions) {
    this.options = options;
    this.pendingEvents = [];
    this.activePart = null;
    this.partCounter = 0;
    this.flushTimer = null;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: GzipRotatingWriterOptions): GzipRotatingWriter {
    const writer = new GzipRotatingWriter(options);
    return writer;
  }

  /**
   * @section private:methods
   */

  private toTimeParts(ingestedAt: number): { year: string; month: string; day: string; hour: string } {
    const date = new Date(ingestedAt);
    const year = String(date.getUTCFullYear());
    const month = String(date.getUTCMonth() + 1).padStart(2, "0");
    const day = String(date.getUTCDate()).padStart(2, "0");
    const hour = String(date.getUTCHours()).padStart(2, "0");
    const parts = { year, month, day, hour };
    return parts;
  }

  private buildPartPaths(ingestedAt: number): { partPath: string; manifestPath: string } {
    const parts = this.toTimeParts(ingestedAt);
    const fileName = `part-${String(this.partCounter).padStart(8, "0")}.ndjson.gz`;
    const manifestFileName = fileName.replace(".ndjson.gz", ".manifest.json");
    const journalDir = path.join(this.options.outputDir, "journal", parts.year, parts.month, parts.day, parts.hour);
    const manifestsDir = path.join(this.options.outputDir, "manifests", parts.year, parts.month, parts.day, parts.hour);
    const partPath = path.join(journalDir, fileName);
    const manifestPath = path.join(manifestsDir, manifestFileName);
    const paths = { partPath, manifestPath };
    return paths;
  }

  private buildManifest(activePart: ActivePartState): PartManifest {
    const sources = Array.from(activePart.sources.values()).sort((left, right) => left.localeCompare(right));
    const eventTypes = Array.from(activePart.eventTypes.values()).sort((left, right) => left.localeCompare(right));
    const manifest: PartManifest = {
      file: activePart.partPath,
      minIngestedAt: activePart.minIngestedAt,
      maxIngestedAt: activePart.maxIngestedAt,
      eventCount: activePart.eventCount,
      sources,
      eventTypes,
      createdAt: new Date().toISOString()
    };
    return manifest;
  }

  private async openPart(ingestedAt: number): Promise<void> {
    this.partCounter += 1;
    const paths = this.buildPartPaths(ingestedAt);
    const journalDir = path.dirname(paths.partPath);
    const manifestsDir = path.dirname(paths.manifestPath);
    await mkdir(journalDir, { recursive: true });
    await mkdir(manifestsDir, { recursive: true });
    this.activePart = {
      partPath: paths.partPath,
      manifestPath: paths.manifestPath,
      bytes: 0,
      minIngestedAt: ingestedAt,
      maxIngestedAt: ingestedAt,
      eventCount: 0,
      sources: new Set<string>(),
      eventTypes: new Set<string>()
    };
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

  private async flushPartManifest(): Promise<void> {
    if (this.activePart) {
      const manifest = this.buildManifest(this.activePart);
      const serialized = JSON.stringify(manifest, null, 2);
      await writeFile(this.activePart.manifestPath, serialized, "utf8");
    }
  }

  private async rotateIfNeeded(nextChunkBytes: number, ingestedAt: number): Promise<void> {
    if (!this.activePart) {
      await this.openPart(ingestedAt);
    }

    if (this.activePart) {
      const nextBytes = this.activePart.bytes + nextChunkBytes;
      const shouldRotate = nextBytes > this.options.maxPartBytes && this.activePart.eventCount > 0;
      if (shouldRotate) {
        await this.flushPartManifest();
        await this.openPart(ingestedAt);
      }
    }
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

  private async flushPendingEvents(): Promise<void> {
    const hasPending = this.pendingEvents.length > 0;
    if (hasPending) {
      const events = [...this.pendingEvents];
      this.pendingEvents.length = 0;
      const compressed = this.toChunk(events);
      const chunkBytes = compressed.length;
      const firstEvent = events[0];
      if (firstEvent) {
        await this.rotateIfNeeded(chunkBytes, firstEvent.ingestedAt);
        if (this.activePart) {
          await writeFile(this.activePart.partPath, compressed, { flag: "a" });
          this.activePart.bytes += chunkBytes;
          this.updatePartStats(events);
        }
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
        throw StorageWriteError.fromCause("failed flushing gzip writer", error);
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
      throw StorageWriteError.fromCause("failed to flush pending lines to gzip part", error);
    }
  }

  public async stop(): Promise<void> {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }

    await this.flush();
    await this.flushPartManifest();
  }

  public static bytesToMegabytes(bytes: number): number {
    const megabytes = bytes / BYTE_UNITS / BYTE_UNITS;
    return megabytes;
  }

  /**
   * @section static:methods
   */

  // empty
}
