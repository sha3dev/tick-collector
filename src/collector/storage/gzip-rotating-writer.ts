/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import type { GzipRotatingWriterOptions } from "../types/storage-types.ts";
import { HourPartitionWriter } from "./hour-partition-writer.ts";

/**
 * @section consts
 */

const BYTE_UNITS = 1024;

/**
 * @section types
 */

// empty

export class GzipRotatingWriter extends HourPartitionWriter {
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

  // empty

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: GzipRotatingWriterOptions) {
    super(options);
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

  // empty

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  // empty

  public static bytesToMegabytes(bytes: number): number {
    const megabytes = bytes / BYTE_UNITS / BYTE_UNITS;
    return megabytes;
  }

  /**
   * @section static:methods
   */

  // empty
}
