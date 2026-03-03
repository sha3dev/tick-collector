/**
 * @section imports:externals
 */

import type { CryptoMarketWindow, CryptoSymbol } from "@sha3/polymarket";

/**
 * @section imports:internals
 */

import type { CreateWindowIteratorOptions } from "./types/create-window-iterator-options.ts";
import type { WindowIteratorAvailability } from "./types/window-iterator-availability.ts";
import type { WindowIteratorNextResult } from "./types/window-iterator-next-result.ts";
import type { WindowEventReader } from "./window-event-reader.ts";

/**
 * @section consts
 */

const DEFAULT_POLL_INTERVAL_MS = 1000;

/**
 * @section types
 */

type ContinuousWindowEventIteratorOptions = {
  reader: WindowEventReader;
  symbol: CryptoSymbol;
  marketType: CryptoMarketWindow;
  startTimestamp?: number;
  pollIntervalMs?: number;
  signal?: AbortSignal;
};

export class ContinuousWindowEventIterator {
  /**
   * @section private:attributes
   */

  private isInitialized: boolean;

  /**
   * @section protected:attributes
   */

  // empty

  /**
   * @section private:properties
   */

  private readonly reader: WindowEventReader;
  private readonly symbol: CryptoSymbol;
  private readonly marketType: CryptoMarketWindow;
  private readonly startTimestamp: number | null;
  private readonly pollIntervalMs: number;
  private readonly signal: AbortSignal | null;
  private cursorWindowStartAt: number;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: ContinuousWindowEventIteratorOptions) {
    this.reader = options.reader;
    this.symbol = options.symbol;
    this.marketType = options.marketType;
    this.startTimestamp = options.startTimestamp ?? null;
    this.pollIntervalMs = options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
    this.signal = options.signal ?? null;
    this.cursorWindowStartAt = 0;
    this.isInitialized = false;
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: ContinuousWindowEventIteratorOptions): ContinuousWindowEventIterator {
    const iterator = new ContinuousWindowEventIterator(options);
    return iterator;
  }

  /**
   * @section private:methods
   */

  private toDoneResult(): WindowIteratorNextResult {
    const result: WindowIteratorNextResult = { done: true, value: null };
    return result;
  }

  private isAborted(): boolean {
    const aborted = this.signal?.aborted ?? false;
    return aborted;
  }

  private async ensureInitialized(): Promise<void> {
    if (!this.isInitialized) {
      const resolveOptions =
        this.startTimestamp === null
          ? { symbol: this.symbol, marketType: this.marketType }
          : { symbol: this.symbol, marketType: this.marketType, startTimestamp: this.startTimestamp };
      this.cursorWindowStartAt = await this.reader.resolveInitialWindowStart(resolveOptions);
      this.isInitialized = true;
    }
  }

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public async next(): Promise<WindowIteratorNextResult> {
    let result: WindowIteratorNextResult = this.toDoneResult();
    const abortedAtStart = this.isAborted();
    if (!abortedAtStart) {
      await this.ensureInitialized();
      const waitOptions =
        this.signal === null
          ? { marketType: this.marketType, windowStartAt: this.cursorWindowStartAt, pollIntervalMs: this.pollIntervalMs }
          : { marketType: this.marketType, windowStartAt: this.cursorWindowStartAt, pollIntervalMs: this.pollIntervalMs, signal: this.signal };
      const isClosed = await this.reader.waitUntilWindowClosed(waitOptions);
      const abortedAfterWait = this.isAborted();
      const canEmit = isClosed && !abortedAfterWait;
      if (canEmit) {
        const batch = await this.reader.readWindowBatch({ symbol: this.symbol, marketType: this.marketType, windowStartAt: this.cursorWindowStartAt });
        const windowMs = this.reader.toWindowMilliseconds(this.marketType);
        this.cursorWindowStartAt += windowMs;
        result = { done: false, value: batch };
      }
    }
    return result;
  }

  public async getAvailability(): Promise<WindowIteratorAvailability> {
    await this.ensureInitialized();
    const availability = await this.reader.getAvailability({ symbol: this.symbol, marketType: this.marketType, cursorWindowStartAt: this.cursorWindowStartAt });
    return availability;
  }

  /**
   * @section static:methods
   */

  // empty
}

export type { ContinuousWindowEventIteratorOptions };
export type { CreateWindowIteratorOptions };
