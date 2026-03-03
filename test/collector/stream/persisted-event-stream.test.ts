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
