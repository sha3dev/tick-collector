/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import CONFIG from "../../config.ts";
import { MarketDataPointReader } from "../query/market-data-point-reader.ts";
import type { MarketDataPoint } from "../query/types/market-data-point.ts";
import type { ReadDataPointOptions } from "../query/types/read-data-point-options.ts";
import type { ReadDataPointRangeOptions } from "../query/types/read-data-point-range-options.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

type PersistedEventStreamOptions = { folder: string; reader?: MarketDataPointReader };

export class PersistedEventStream {
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

  private readonly reader: MarketDataPointReader;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: PersistedEventStreamOptions) {
    this.reader =
      options.reader ??
      MarketDataPointReader.create({
        folder: options.folder,
        defaultSources: [...CONFIG.READER.defaultSources],
        defaultMaxDistanceMs: CONFIG.READER.maxDistanceMs,
        defaultOrderbookLevels: CONFIG.READER.orderbookLevels
      });
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
  // empty

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public async read(options: ReadDataPointOptions): Promise<MarketDataPoint | null> {
    const datapoint = await this.reader.read(options);
    return datapoint;
  }

  public async readRange(options: ReadDataPointRangeOptions): Promise<MarketDataPoint[]> {
    const datapoints = await this.reader.readRange(options);
    return datapoints;
  }

  /**
   * @section static:methods
   */

  // empty
}

export type { PersistedEventStreamOptions };
