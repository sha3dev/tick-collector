import * as assert from "node:assert/strict";
import { test } from "node:test";

import { PolymarketMarketScheduler } from "../../../src/collector/polymarket/polymarket-market-scheduler.ts";

class FakeMarketsService {
  public readonly calls: { window: string; date: Date }[] = [];

  public async loadCryptoWindowMarkets(options: { date: Date; window: "5m" | "15m"; symbols: ("btc" | "eth" | "sol" | "xrp")[] }) {
    this.calls.push({ window: options.window, date: options.date });
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
}

class FakeStreamService {
  public readonly subscriptions: string[][] = [];

  public subscribe(options: { assetIds: string[] }): void {
    this.subscriptions.push(options.assetIds);
  }
}

test("polymarket market scheduler subscribes current windows and schedules next boundaries", async () => {
  const marketsService = new FakeMarketsService();
  const streamService = new FakeStreamService();
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
  assert.equal(scheduler.getMarketSlug("asset-1") !== null, true);
  assert.deepEqual(
    scheduledTimeouts.sort((left, right) => left - right),
    [270000, 270000]
  );
});
