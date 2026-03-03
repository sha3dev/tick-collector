/**
 * @section imports:externals
 */

// empty

/**
 * @section imports:internals
 */

import CONFIG from "../../config.ts";
import { MarketDataPointReader } from "../query/market-data-point-reader.ts";
import { ContinuousWindowEventIterator } from "./continuous-window-event-iterator.ts";
import { WindowEventReader } from "./window-event-reader.ts";
import type { MarketDataPoint } from "../query/types/market-data-point.ts";
import type { ReadDataPointOptions } from "../query/types/read-data-point-options.ts";
import type { ReadDataPointRangeOptions } from "../query/types/read-data-point-range-options.ts";
import type { CreateWindowIteratorOptions } from "./types/create-window-iterator-options.ts";
import type { WindowEventIterator } from "./types/window-event-iterator.ts";

/**
 * @section consts
 */

// empty

/**
 * @section types
 */

type PersistedEventStreamOptions = { folder: string; reader?: MarketDataPointReader; windowReader?: WindowEventReader };

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
  private readonly windowReader: WindowEventReader;

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
    this.windowReader = options.windowReader ?? WindowEventReader.create({ folder: options.folder });
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

  public createWindowIterator(options: CreateWindowIteratorOptions): WindowEventIterator {
    const iteratorOptions = {
      reader: this.windowReader,
      symbol: options.symbol,
      marketType: options.marketType,
      ...(options.startTimestamp !== undefined ? { startTimestamp: options.startTimestamp } : {}),
      ...(options.pollIntervalMs !== undefined ? { pollIntervalMs: options.pollIntervalMs } : {}),
      ...(options.signal !== undefined ? { signal: options.signal } : {})
    };
    const iterator = ContinuousWindowEventIterator.create(iteratorOptions);
    return iterator;
  }

  /**
   * @section static:methods
   */

  // empty
}

export type { PersistedEventStreamOptions };
