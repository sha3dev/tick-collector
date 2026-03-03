import * as assert from "node:assert/strict";
import { test } from "node:test";

import type { MarketDataPoint } from "../../../src/collector/query/types/market-data-point.ts";
import { PersistedEventStream } from "../../../src/collector/stream/persisted-event-stream.ts";

test("persisted event stream read delegates options to datapoint reader", async () => {
  let receivedSymbol: string | null = null;
  let receivedMarketType: string | null = null;
  let receivedTimestamp: number | null = null;
  const expected: MarketDataPoint = {
    timestamp: 123,
    symbol: "btc",
    marketType: "5m",
    marketStartAt: 120,
    cryptoPricesBySource: { binance: 10, chainlink: 11 },
    polymarket: { upPrice: 0.6, downPrice: 0.4, orderbook: { bids: [], asks: [] } },
    exchangeOrderbooksBySource: { binance: { bids: [], asks: [] } },
    coverage: { missingFields: [], selectedEvents: [] }
  };
  const reader = {
    read: async (options: { timestamp: number; symbol: string; marketType: string }): Promise<MarketDataPoint> => {
      receivedTimestamp = options.timestamp;
      receivedSymbol = options.symbol;
      receivedMarketType = options.marketType;
      return expected;
    }
  } as never;
  const stream = PersistedEventStream.create({ folder: "data", reader });

  const datapoint = await stream.read({ timestamp: 123, symbol: "btc", marketType: "5m" });

  assert.equal(receivedTimestamp, 123);
  assert.equal(receivedSymbol, "btc");
  assert.equal(receivedMarketType, "5m");
  assert.equal(datapoint?.symbol, "btc");
});

test("persisted event stream read forwards optional overrides", async () => {
  let receivedOrderbookLevels: number | null = null;
  let receivedMaxDistanceMs: number | null = null;
  const reader = {
    read: async (options: { maxDistanceMs?: number; orderbookLevels?: number }): Promise<MarketDataPoint | null> => {
      receivedMaxDistanceMs = options.maxDistanceMs ?? null;
      receivedOrderbookLevels = options.orderbookLevels ?? null;
      return null;
    }
  } as never;
  const stream = PersistedEventStream.create({ folder: "data", reader });

  const datapoint = await stream.read({ timestamp: 100, symbol: "btc", marketType: "5m", maxDistanceMs: 5000, orderbookLevels: 7 });

  assert.equal(receivedMaxDistanceMs, 5000);
  assert.equal(receivedOrderbookLevels, 7);
  assert.equal(datapoint, null);
});

test("persisted event stream readRange delegates options to datapoint reader", async () => {
  let receivedStartTimestamp: number | null = null;
  let receivedEndTimestamp: number | null = null;
  let receivedStepMs: number | null = null;
  let receivedSymbol: string | null = null;
  let receivedMarketType: string | null = null;
  const reader = {
    read: async (): Promise<MarketDataPoint | null> => {
      return null;
    },
    readRange: async (options: {
      startTimestamp: number;
      endTimestamp: number;
      stepMs: number;
      symbol: string;
      marketType: string;
    }): Promise<MarketDataPoint[]> => {
      receivedStartTimestamp = options.startTimestamp;
      receivedEndTimestamp = options.endTimestamp;
      receivedStepMs = options.stepMs;
      receivedSymbol = options.symbol;
      receivedMarketType = options.marketType;
      return [];
    }
  } as never;
  const stream = PersistedEventStream.create({ folder: "data", reader });

  const datapoints = await stream.readRange({ startTimestamp: 100, endTimestamp: 300, stepMs: 100, symbol: "btc", marketType: "5m" });

  assert.equal(receivedStartTimestamp, 100);
  assert.equal(receivedEndTimestamp, 300);
  assert.equal(receivedStepMs, 100);
  assert.equal(receivedSymbol, "btc");
  assert.equal(receivedMarketType, "5m");
  assert.equal(datapoints.length, 0);
});

test("persisted event stream createWindowIterator emits one window per next call", async () => {
  const eventsByWindowStart = new Map<number, { eventId: string }[]>([
    [300_000, [{ eventId: "w1" }]],
    [600_000, [{ eventId: "w2" }]]
  ]);
  const windowReader = {
    resolveInitialWindowStart: async (): Promise<number> => {
      return 300_000;
    },
    waitUntilWindowClosed: async (): Promise<boolean> => {
      return true;
    },
    readWindowBatch: async (options: { windowStartAt: number; symbol: string; marketType: string }) => {
      const markerEvents = eventsByWindowStart.get(options.windowStartAt) ?? [];
      return {
        symbol: options.symbol,
        marketType: options.marketType,
        windowStartAt: options.windowStartAt,
        windowEndAt: options.windowStartAt + 300_000,
        events: markerEvents.map((event, index) => {
          return {
            eventId: event.eventId,
            source: "crypto" as const,
            eventType: "crypto.price",
            ingestedAt: options.windowStartAt + index,
            sequence: index + 1,
            symbol: options.symbol,
            provider: "binance",
            payload: {}
          };
        }),
        stats: { totalEvents: markerEvents.length, cryptoEvents: markerEvents.length, polymarketEvents: 0, polymarketDistinctMarkets: 0 }
      };
    },
    toWindowMilliseconds: (): number => {
      return 300_000;
    },
    getAvailability: async (options: { cursorWindowStartAt: number }) => {
      const availableWindows = options.cursorWindowStartAt <= 600_000 ? 2 : 1;
      return { availableWindows, cursorWindowStartAt: options.cursorWindowStartAt, latestClosedWindowStartAt: 900_000 };
    }
  } as never;
  const reader = { read: async (): Promise<null> => null, readRange: async (): Promise<[]> => [] } as never;
  const stream = PersistedEventStream.create({ folder: "data", reader, windowReader });
  const iterator = stream.createWindowIterator({ symbol: "btc", marketType: "5m" });

  const first = await iterator.next();
  const second = await iterator.next();

  assert.equal(first.done, false);
  assert.equal(first.value?.windowStartAt, 300_000);
  assert.equal(first.value?.events[0]?.eventId, "w1");
  assert.equal(second.done, false);
  assert.equal(second.value?.windowStartAt, 600_000);
  assert.equal(second.value?.events[0]?.eventId, "w2");
});

test("persisted event stream createWindowIterator availability and abort behavior", async () => {
  const controller = new AbortController();
  const windowReader = {
    resolveInitialWindowStart: async (): Promise<number> => {
      return 300_000;
    },
    waitUntilWindowClosed: async (): Promise<boolean> => {
      return true;
    },
    readWindowBatch: async () => {
      return {
        symbol: "btc",
        marketType: "5m",
        windowStartAt: 300_000,
        windowEndAt: 600_000,
        events: [],
        stats: { totalEvents: 0, cryptoEvents: 0, polymarketEvents: 0, polymarketDistinctMarkets: 0 }
      };
    },
    toWindowMilliseconds: (): number => {
      return 300_000;
    },
    getAvailability: async (options: { cursorWindowStartAt: number }) => {
      return {
        availableWindows: options.cursorWindowStartAt === 300_000 ? 2 : 1,
        cursorWindowStartAt: options.cursorWindowStartAt,
        latestClosedWindowStartAt: 600_000
      };
    }
  } as never;
  const reader = { read: async (): Promise<null> => null, readRange: async (): Promise<[]> => [] } as never;
  const stream = PersistedEventStream.create({ folder: "data", reader, windowReader });
  const iterator = stream.createWindowIterator({ symbol: "btc", marketType: "5m", signal: controller.signal });

  const before = await iterator.getAvailability();
  const first = await iterator.next();
  const after = await iterator.getAvailability();
  controller.abort();
  const done = await iterator.next();

  assert.equal(before.availableWindows, 2);
  assert.equal(first.done, false);
  assert.equal(after.availableWindows, 1);
  assert.equal(done.done, true);
  assert.equal(done.value, null);
});
