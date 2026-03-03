/**
 * @section imports:externals
 */

import type { CryptoMarketWindow, CryptoSymbol } from "@sha3/polymarket";

/**
 * @section imports:internals
 */

import { EventIndexRepository } from "../query/event-index-repository.ts";
import type { StoredEvent } from "../types/stored-event.ts";
import type { WindowEventBatch } from "./types/window-event-batch.ts";
import type { WindowIteratorAvailability } from "./types/window-iterator-availability.ts";

/**
 * @section consts
 */

const MINUTES_TO_MS = 60_000;

/**
 * @section types
 */

type EventIndexRepositoryContract = {
  findEventsInRange: (query: { startTimestamp: number; endTimestampExclusive: number; symbol: string; marketType: string }) => Promise<StoredEvent[]>;
  findBoundsForSymbolMarketType: (query: { symbol: string; marketType: string }) => Promise<{ minIngestedAt: number | null; maxIngestedAt: number | null }>;
};

type WindowEventReaderOptions = {
  folder: string;
  indexRepository?: EventIndexRepositoryContract;
  clock?: () => number;
  sleep?: (delayMs: number) => Promise<void>;
};

type ReadWindowBatchOptions = { symbol: CryptoSymbol; marketType: CryptoMarketWindow; windowStartAt: number };
type ResolveInitialWindowStartOptions = { symbol: CryptoSymbol; marketType: CryptoMarketWindow; startTimestamp?: number };
type GetAvailabilityOptions = { symbol: CryptoSymbol; marketType: CryptoMarketWindow; cursorWindowStartAt: number };
type WaitUntilClosedOptions = { marketType: CryptoMarketWindow; windowStartAt: number; pollIntervalMs: number; signal?: AbortSignal };
type PolymarketSlugSeed = { slug: string; sortValue: number; ingestedAt: number };

export class WindowEventReader {
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

  private readonly indexRepository: EventIndexRepositoryContract;
  private readonly clock: () => number;
  private readonly sleep: (delayMs: number) => Promise<void>;

  /**
   * @section public:properties
   */

  // empty

  /**
   * @section constructor
   */

  public constructor(options: WindowEventReaderOptions) {
    this.indexRepository = options.indexRepository ?? EventIndexRepository.create({ folder: options.folder });
    this.clock = options.clock ?? Date.now;
    this.sleep =
      options.sleep ??
      (async (delayMs: number): Promise<void> => {
        await new Promise((resolve) => {
          setTimeout(resolve, delayMs);
        });
      });
  }

  /**
   * @section static:properties
   */

  // empty

  /**
   * @section factory
   */

  public static create(options: WindowEventReaderOptions): WindowEventReader {
    const reader = new WindowEventReader(options);
    return reader;
  }

  /**
   * @section private:methods
   */

  private toPolymarketSortValue(event: StoredEvent): number {
    const fallback = event.sequence;
    const value = event.marketEventIndex ?? fallback;
    return value;
  }

  private toPolymarketSlugKey(event: StoredEvent): string {
    const key = event.marketSlug ?? "na";
    return key;
  }

  private isWindowMember(event: StoredEvent, options: ReadWindowBatchOptions): boolean {
    const windowMs = this.toWindowMilliseconds(options.marketType);
    const windowEndAt = options.windowStartAt + windowMs;
    const inTimeRange = event.ingestedAt >= options.windowStartAt && event.ingestedAt < windowEndAt;
    const isCrypto = event.source === "crypto" && event.symbol === options.symbol;
    const marketStartMatch = event.marketStartAt === undefined || this.alignWindowStart(event.marketStartAt, options.marketType) === options.windowStartAt;
    const isPolymarket = event.source === "polymarket" && event.symbol === options.symbol && event.marketType === options.marketType && marketStartMatch;
    const match = inTimeRange && (isCrypto || isPolymarket);
    return match;
  }

  private selectWindowPolymarketSlug(events: StoredEvent[]): string | null {
    const polymarketEvents = events.filter((event) => {
      const isPolymarket = event.source === "polymarket";
      return isPolymarket;
    });
    const seeds: PolymarketSlugSeed[] = [];
    for (const event of polymarketEvents) {
      const hasSlug = event.marketSlug !== undefined;
      if (hasSlug) {
        seeds.push({ slug: event.marketSlug ?? "na", sortValue: this.toPolymarketSortValue(event), ingestedAt: event.ingestedAt });
      }
    }
    const sortedSeeds = [...seeds].sort((left, right) => {
      const byIndex = left.sortValue - right.sortValue;
      const byTime = byIndex === 0 ? left.ingestedAt - right.ingestedAt : byIndex;
      return byTime;
    });
    const targetSlug = sortedSeeds[0]?.slug ?? null;
    return targetSlug;
  }

  private keepConsistentPolymarketMarket(events: StoredEvent[]): StoredEvent[] {
    const targetSlug = this.selectWindowPolymarketSlug(events);
    const filtered = events.filter((event) => {
      const isCrypto = event.source === "crypto";
      const hasNoSlug = event.marketSlug === undefined;
      const keepPolymarket = event.source === "polymarket" && (hasNoSlug || event.marketSlug === targetSlug);
      const keep = isCrypto || keepPolymarket;
      return keep;
    });
    return filtered;
  }

  private sortEventsForWindow(events: StoredEvent[]): StoredEvent[] {
    const sorted = [...events].sort((left, right) => {
      const byTime = left.ingestedAt - right.ingestedAt;
      const bySource = byTime === 0 ? left.source.localeCompare(right.source) : byTime;
      const leftSlug = this.toPolymarketSlugKey(left);
      const rightSlug = this.toPolymarketSlugKey(right);
      const bySlug = bySource === 0 ? leftSlug.localeCompare(rightSlug) : bySource;
      const leftPolyIndex = this.toPolymarketSortValue(left);
      const rightPolyIndex = this.toPolymarketSortValue(right);
      const byPolyIndex = bySlug === 0 ? leftPolyIndex - rightPolyIndex : bySlug;
      const bySequence = byPolyIndex === 0 ? left.sequence - right.sequence : byPolyIndex;
      return bySequence;
    });
    return sorted;
  }

  private buildStats(events: StoredEvent[]): WindowEventBatch["stats"] {
    let cryptoEvents = 0;
    let polymarketEvents = 0;
    const polymarketSlugKeys = new Set<string>();
    for (const event of events) {
      if (event.source === "crypto") {
        cryptoEvents += 1;
      }
      if (event.source === "polymarket") {
        polymarketEvents += 1;
        const slugKey = this.toPolymarketSlugKey(event);
        polymarketSlugKeys.add(slugKey);
      }
    }
    const stats: WindowEventBatch["stats"] = { totalEvents: events.length, cryptoEvents, polymarketEvents, polymarketDistinctMarkets: polymarketSlugKeys.size };
    return stats;
  }

  private toDoneAvailability(cursorWindowStartAt: number): WindowIteratorAvailability {
    const availability: WindowIteratorAvailability = { availableWindows: 0, cursorWindowStartAt, latestClosedWindowStartAt: null };
    return availability;
  }

  /**
   * @section protected:methods
   */

  // empty

  /**
   * @section public:methods
   */

  public toWindowMilliseconds(marketType: CryptoMarketWindow): number {
    const minuteValue = Number(marketType.replace("m", ""));
    const windowMs = minuteValue * MINUTES_TO_MS;
    return windowMs;
  }

  public alignWindowStart(timestamp: number, marketType: CryptoMarketWindow): number {
    const windowMs = this.toWindowMilliseconds(marketType);
    const alignedStart = Math.floor(timestamp / windowMs) * windowMs;
    return alignedStart;
  }

  public getLatestClosedWindowStartAt(marketType: CryptoMarketWindow): number | null {
    const nowMs = this.clock();
    const windowMs = this.toWindowMilliseconds(marketType);
    const alignedStart = this.alignWindowStart(nowMs, marketType);
    const latestStart = alignedStart - windowMs;
    const hasClosedWindow = latestStart >= 0;
    const result = hasClosedWindow ? latestStart : null;
    return result;
  }

  public async resolveInitialWindowStart(options: ResolveInitialWindowStartOptions): Promise<number> {
    let cursorWindowStartAt = this.alignWindowStart(this.clock(), options.marketType);
    const hasProvidedStart = options.startTimestamp !== undefined;
    if (hasProvidedStart) {
      cursorWindowStartAt = this.alignWindowStart(options.startTimestamp ?? cursorWindowStartAt, options.marketType);
    } else {
      const bounds = await this.indexRepository.findBoundsForSymbolMarketType({ symbol: options.symbol, marketType: options.marketType });
      const hasBound = bounds.minIngestedAt !== null;
      if (hasBound) {
        cursorWindowStartAt = this.alignWindowStart(bounds.minIngestedAt ?? cursorWindowStartAt, options.marketType);
      }
    }
    return cursorWindowStartAt;
  }

  public async getAvailability(options: GetAvailabilityOptions): Promise<WindowIteratorAvailability> {
    let availability = this.toDoneAvailability(options.cursorWindowStartAt);
    const latestClosedWindowStartAt = this.getLatestClosedWindowStartAt(options.marketType);
    if (latestClosedWindowStartAt !== null) {
      const windowMs = this.toWindowMilliseconds(options.marketType);
      const delta = latestClosedWindowStartAt - options.cursorWindowStartAt;
      const hasAheadWindows = delta >= 0;
      const availableWindows = hasAheadWindows ? Math.floor(delta / windowMs) + 1 : 0;
      availability = { availableWindows, cursorWindowStartAt: options.cursorWindowStartAt, latestClosedWindowStartAt };
    }
    return availability;
  }

  public async waitUntilWindowClosed(options: WaitUntilClosedOptions): Promise<boolean> {
    let isClosed = false;
    let isAborted = options.signal?.aborted ?? false;
    while (!isClosed && !isAborted) {
      const latestClosedWindowStartAt = this.getLatestClosedWindowStartAt(options.marketType);
      const canEmit = latestClosedWindowStartAt !== null && latestClosedWindowStartAt >= options.windowStartAt;
      if (canEmit) {
        isClosed = true;
      } else {
        await this.sleep(options.pollIntervalMs);
        isAborted = options.signal?.aborted ?? false;
      }
    }
    return isClosed;
  }

  public async readWindowBatch(options: ReadWindowBatchOptions): Promise<WindowEventBatch> {
    const windowMs = this.toWindowMilliseconds(options.marketType);
    const windowEndAt = options.windowStartAt + windowMs;
    const rangeEvents = await this.indexRepository.findEventsInRange({
      startTimestamp: options.windowStartAt,
      endTimestampExclusive: windowEndAt,
      symbol: options.symbol,
      marketType: options.marketType
    });
    const members = rangeEvents.filter((event) => {
      const isMember = this.isWindowMember(event, options);
      return isMember;
    });
    // Keep one polymarket market per window to avoid mixing boundary carry-over events.
    const consistentMembers = this.keepConsistentPolymarketMarket(members);
    // Keep polymarket boundary transitions deterministic by ordering with slug + marketEventIndex.
    const events = this.sortEventsForWindow(consistentMembers);
    const batch: WindowEventBatch = {
      symbol: options.symbol,
      marketType: options.marketType,
      windowStartAt: options.windowStartAt,
      windowEndAt,
      events,
      stats: this.buildStats(events)
    };
    return batch;
  }

  /**
   * @section static:methods
   */

  // empty
}

export type { WindowEventReaderOptions };
