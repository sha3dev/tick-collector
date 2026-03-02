/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import { GzipRotatingWriter } from "./gzip-rotating-writer.ts";
import type { StoredEvent } from "../types/stored-event.ts";
import type { GzipRotatingWriterOptions } from "../types/storage-types.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

// empty

export class EventStorageService {
  /**
   * @section private:attributes
   */

  // empty

  /**
   * @section protected:attributes
   */

  // empty

  /**
   * @section private:properties
   */

  private readonly writer: GzipRotatingWriter;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(writer: GzipRotatingWriter) {
    this.writer = writer;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: GzipRotatingWriterOptions): EventStorageService {
    const writer = GzipRotatingWriter.create(options);
    const service = new EventStorageService(writer);
    return service;
  }

  /**
   * @section private:methods
   */

  // empty

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public async start(): Promise<void> {
    await this.writer.start();
  }

  public async append(event: StoredEvent): Promise<void> {
    this.writer.append(event);
  }

  public async appendMany(events: StoredEvent[]): Promise<void> {
    for (const event of events) {
      this.writer.append(event);
    }
  }

  public async flush(): Promise<void> {
    await this.writer.flush();
  }

  public async stop(): Promise<void> {
    await this.writer.stop();
  }

  /**
   * @section static:methods
   */

  // empty
}
