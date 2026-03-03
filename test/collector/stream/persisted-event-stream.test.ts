import * as assert from "node:assert/strict";
import { test } from "node:test";

import type { MarketDataPoint } from "../../../src/collector/query/types/market-data-point.ts";
import { PersistedEventStream } from "../../../src/collector/stream/persisted-event-stream.ts";

test("persisted event stream read delegates options to datapoint reader", async () => {
  let receivedMarketSlug: string | null = null;
  let receivedTimestamp: number | null = null;
  const expected: MarketDataPoint = {
    timestamp: 123,
    marketSlug: "btc-updown-5m-1",
    symbol: "btc",
    cryptoPricesBySource: { binance: 10, chainlink: 11 },
    polymarket: { upPrice: 0.6, downPrice: 0.4, orderbook: { bids: [], asks: [] } },
    exchangeOrderbooksBySource: { binance: { bids: [], asks: [] } },
    coverage: { missingFields: [], selectedEvents: [] }
  };
  const reader = {
    read: async (options: { timestamp: number; marketSlug: string }): Promise<MarketDataPoint> => {
      receivedTimestamp = options.timestamp;
      receivedMarketSlug = options.marketSlug;
      return expected;
    }
  } as never;
  const stream = PersistedEventStream.create({ folder: "data", reader });

  const datapoint = await stream.read({ timestamp: 123, marketSlug: "btc-updown-5m-1" });

  assert.equal(receivedTimestamp, 123);
  assert.equal(receivedMarketSlug, "btc-updown-5m-1");
  assert.equal(datapoint?.marketSlug, "btc-updown-5m-1");
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

  const datapoint = await stream.read({ timestamp: 100, marketSlug: "market", maxDistanceMs: 5000, orderbookLevels: 7 });

  assert.equal(receivedMaxDistanceMs, 5000);
  assert.equal(receivedOrderbookLevels, 7);
  assert.equal(datapoint, null);
});

test("persisted event stream readRange delegates options to datapoint reader", async () => {
  let receivedStartTimestamp: number | null = null;
  let receivedEndTimestamp: number | null = null;
  let receivedStepMs: number | null = null;
  const reader = {
    read: async (): Promise<MarketDataPoint | null> => {
      return null;
    },
    readRange: async (options: { startTimestamp: number; endTimestamp: number; stepMs: number }): Promise<MarketDataPoint[]> => {
      receivedStartTimestamp = options.startTimestamp;
      receivedEndTimestamp = options.endTimestamp;
      receivedStepMs = options.stepMs;
      return [];
    }
  } as never;
  const stream = PersistedEventStream.create({ folder: "data", reader });

  const datapoints = await stream.readRange({ startTimestamp: 100, endTimestamp: 300, stepMs: 100, marketSlug: "market-1" });

  assert.equal(receivedStartTimestamp, 100);
  assert.equal(receivedEndTimestamp, 300);
  assert.equal(receivedStepMs, 100);
  assert.equal(datapoints.length, 0);
});
