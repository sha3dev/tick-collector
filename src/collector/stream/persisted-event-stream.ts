/**
 * @section imports:externals
 */

import { readdir, readFile } from "node:fs/promises";
import * as path from "node:path";
import { gunzipSync } from "node:zlib";

/**
 * @section imports:internals
 */

import type { StoredEvent } from "../types/stored-event.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

type PersistedEventStreamOptions = { folder: string; minIngestedAtExclusive?: number };

export class PersistedEventStream {
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
  private readonly minIngestedAtExclusive: number | null;
  private readonly events: StoredEvent[];
  private currentIndex: number;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: PersistedEventStreamOptions) {
    this.folder = options.folder;
    this.minIngestedAtExclusive = options.minIngestedAtExclusive ?? null;
    this.events = [];
    this.currentIndex = 0;
    this.isLoaded = false;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: PersistedEventStreamOptions): PersistedEventStream {
    const stream = new PersistedEventStream(options);
    return stream;
  }

  /**
   * @section private:methods
   */

  private async collectGzipFiles(dirPath: string, collector: string[]): Promise<void> {
    const entries = await readdir(dirPath, { withFileTypes: true });

    for (const entry of entries) {
      const fullPath = path.join(dirPath, entry.name);
      if (entry.isDirectory()) {
        await this.collectGzipFiles(fullPath, collector);
      } else {
        const isGzip = entry.isFile() && entry.name.endsWith(".ndjson.gz");
        if (isGzip) {
          collector.push(fullPath);
        }
      }
    }
  }

  private parseEvents(fileBytes: Buffer): StoredEvent[] {
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

  private filterByTimestamp(events: StoredEvent[]): StoredEvent[] {
    const filteredEvents = events.filter((event) => {
      let keep = true;

      if (this.minIngestedAtExclusive !== null) {
        keep = event.ingestedAt > this.minIngestedAtExclusive;
      }

      return keep;
    });
    return filteredEvents;
  }

  private sortChronologically(events: StoredEvent[]): StoredEvent[] {
    const sorted = [...events].sort((left, right) => {
      const ingestedOrder = left.ingestedAt - right.ingestedAt;
      const sequenceOrder = left.sequence - right.sequence;
      const order = ingestedOrder === 0 ? sequenceOrder : ingestedOrder;
      return order;
    });
    return sorted;
  }

  private async loadIfNeeded(): Promise<void> {
    if (!this.isLoaded) {
      const files: string[] = [];
      await this.collectGzipFiles(this.folder, files);
      const sortedFiles = [...files].sort((left, right) => {
        return left.localeCompare(right);
      });

      for (const filePath of sortedFiles) {
        const fileBytes = await readFile(filePath);
        const parsedEvents = this.parseEvents(fileBytes);
        const filteredEvents = this.filterByTimestamp(parsedEvents);
        const sortedEvents = this.sortChronologically(filteredEvents);
        this.events.push(...sortedEvents);
      }

      const globallySorted = this.sortChronologically(this.events);
      this.events.length = 0;
      this.events.push(...globallySorted);
      this.isLoaded = true;
    }
  }

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public async readNext(): Promise<StoredEvent | null> {
    await this.loadIfNeeded();
    const event = this.events[this.currentIndex] ?? null;

    if (event) {
      this.currentIndex += 1;
    }

    return event;
  }

  /**
   * @section static:methods
   */

  // empty
}

export type { PersistedEventStreamOptions };
