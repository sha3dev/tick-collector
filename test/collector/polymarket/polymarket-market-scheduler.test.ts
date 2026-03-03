import * as assert from "node:assert/strict";
import { test } from "node:test";

import { PolymarketMarketScheduler } from "../../../src/collector/polymarket/polymarket-market-scheduler.ts";

type FakeMarketsService = {
  calls: { window: string; date: Date }[];
  loadCryptoWindowMarkets: (options: { date: Date; window: "5m" | "15m"; symbols: ("btc" | "eth" | "sol" | "xrp")[] }) => Promise<unknown[]>;
};

type FakeStreamService = {
  subscriptions: string[][];
  unsubscriptions: string[][];
  subscribe: (options: { assetIds: string[] }) => void;
  unsubscribe: (options: { assetIds: string[] }) => void;
};

function createFakeMarketsService(): FakeMarketsService {
  const calls: { window: string; date: Date }[] = [];
  const service: FakeMarketsService = {
    calls,
    loadCryptoWindowMarkets: async (options): Promise<unknown[]> => {
      calls.push({ window: options.window, date: options.date });
      return [
        {
          id: "1",
          slug: `${options.window}-slug`,
          question: "q",
          symbol: "btc" as const,
          conditionId: "c",
          outcomes: [],
          clobTokenIds: ["asset-1", "asset-2"],
          upTokenId: "asset-1",
          downTokenId: "asset-2",
          orderMinSize: 1,
          orderPriceMinTickSize: null,
          eventStartTime: "",
          endDate: "",
          start: new Date(),
          end: new Date(),
          raw: {}
        }
      ];
    }
  };
  return service;
}

function createFakeStreamService(): FakeStreamService {
  const subscriptions: string[][] = [];
  const unsubscriptions: string[][] = [];
  const service: FakeStreamService = {
    subscriptions,
    unsubscriptions,
    subscribe: (options): void => {
      subscriptions.push(options.assetIds);
    },
    unsubscribe: (options): void => {
      unsubscriptions.push(options.assetIds);
    }
  };
  return service;
}

test("polymarket market scheduler subscribes current windows and schedules next boundaries", async () => {
  const marketsService = createFakeMarketsService();
  const streamService = createFakeStreamService();
  const scheduledTimeouts: number[] = [];
  const scheduler = PolymarketMarketScheduler.create({
    marketsService: marketsService as never,
    streamService: streamService as never,
    symbols: ["btc", "eth", "sol", "xrp"],
    windows: ["5m", "15m"],
    clock: () => Date.UTC(2026, 0, 1, 0, 10, 30),
    setTimeoutFn: (_handler, timeoutMs) => {
      scheduledTimeouts.push(timeoutMs);
      return { unref: () => undefined } as unknown as NodeJS.Timeout;
    },
    clearTimeoutFn: () => undefined
  });

  await scheduler.start();
  await scheduler.stop();

  assert.equal(marketsService.calls.length, 2);
  assert.equal(streamService.subscriptions.length > 0, true);
  const firstSubscription = streamService.subscriptions[0] ?? [];
  assert.equal(firstSubscription.includes("asset-1"), true);
  const marketContext = scheduler.getMarketContext("asset-1");
  assert.equal(marketContext !== null, true);
  assert.equal(["5m", "15m"].includes(marketContext?.marketType ?? ""), true);
  assert.equal(["5m-slug", "15m-slug"].includes(marketContext?.marketSlug ?? ""), true);
  assert.equal(marketContext?.marketSide, "up");
  assert.equal(marketContext?.symbol, "btc");
  assert.equal(typeof marketContext?.marketStartAt, "number");
  assert.deepEqual(
    scheduledTimeouts.sort((left, right) => left - right),
    [270000, 270000]
  );
});

test("polymarket market scheduler unsubscribes stale markets and subscribes new ones on boundary refresh", async () => {
  let loadCount = 0;
  const streamService = createFakeStreamService();
  const handlers: (() => void)[] = [];
  const marketsService: FakeMarketsService = {
    calls: [],
    loadCryptoWindowMarkets: async (): Promise<unknown[]> => {
      loadCount += 1;
      const markets =
        loadCount === 1
          ? [
              {
                id: "1",
                slug: "market-a",
                question: "q",
                symbol: "btc" as const,
                conditionId: "c",
                outcomes: [],
                clobTokenIds: ["asset-a"],
                upTokenId: "asset-a",
                downTokenId: "asset-z",
                orderMinSize: 1,
                orderPriceMinTickSize: null,
                eventStartTime: "",
                endDate: "",
                start: new Date(),
                end: new Date(),
                raw: {}
              }
            ]
          : [
              {
                id: "2",
                slug: "market-b",
                question: "q",
                symbol: "btc" as const,
                conditionId: "c",
                outcomes: [],
                clobTokenIds: ["asset-b"],
                upTokenId: "asset-b",
                downTokenId: "asset-z",
                orderMinSize: 1,
                orderPriceMinTickSize: null,
                eventStartTime: "",
                endDate: "",
                start: new Date(),
                end: new Date(),
                raw: {}
              }
            ];
      return markets;
    }
  };
  const scheduler = PolymarketMarketScheduler.create({
    marketsService: marketsService as never,
    streamService: streamService as never,
    symbols: ["btc", "eth", "sol", "xrp"],
    windows: ["5m"],
    clock: () => Date.UTC(2026, 0, 1, 0, 10, 30),
    setTimeoutFn: (handler) => {
      handlers.push(handler);
      return { unref: () => undefined } as unknown as NodeJS.Timeout;
    },
    clearTimeoutFn: () => undefined
  });

  await scheduler.start();
  const boundaryHandler = handlers[0];
  if (boundaryHandler) {
    boundaryHandler();
  }
  await new Promise((resolve) => {
    setTimeout(resolve, 0);
  });
  await scheduler.stop();

  assert.equal(streamService.subscriptions[0]?.includes("asset-a"), true);
  assert.equal(streamService.unsubscriptions[0]?.includes("asset-a"), true);
  assert.equal(streamService.subscriptions[1]?.includes("asset-b"), true);
});
