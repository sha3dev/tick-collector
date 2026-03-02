import * as assert from "node:assert/strict";
import { test } from "node:test";

import { CryptoFeedAdapter } from "../../../src/collector/crypto/crypto-feed-adapter.ts";
import { InvalidCollectorSourcesError } from "../../../src/collector/errors/invalid-collector-sources-error.ts";
import { PolymarketFeedAdapter } from "../../../src/collector/polymarket/polymarket-feed-adapter.ts";
import { CollectorApp } from "../../../src/collector/runtime/collector-app.ts";

type FakeStorage = { events: string[]; start: () => Promise<void>; stop: () => Promise<void> };

type FakeCoalescer = { events: string[]; flushReady: () => Promise<void>; flushAll: () => Promise<void> };

type FakeAdapter = { start: () => Promise<void>; stop: () => Promise<void> };

function createFakeStorage(): FakeStorage {
  const events: string[] = [];
  const fakeStorage: FakeStorage = {
    events,
    start: async (): Promise<void> => {
      events.push("storage-start");
    },
    stop: async (): Promise<void> => {
      events.push("storage-stop");
    }
  };
  return fakeStorage;
}

function createFakeCoalescer(): FakeCoalescer {
  const events: string[] = [];
  const fakeCoalescer: FakeCoalescer = {
    events,
    flushReady: async (): Promise<void> => {
      events.push("coalescer-flush-ready");
    },
    flushAll: async (): Promise<void> => {
      events.push("coalescer-flush-all");
    }
  };
  return fakeCoalescer;
}

function createFakeAdapter(events: string[], startLabel: string, stopLabel: string): FakeAdapter {
  const fakeAdapter: FakeAdapter = {
    start: async (): Promise<void> => {
      events.push(startLabel);
    },
    stop: async (): Promise<void> => {
      events.push(stopLabel);
    }
  };
  return fakeAdapter;
}

test("collector app starts and stops in expected order with optional adapters", async () => {
  const events: string[] = [];
  const storage = createFakeStorage();
  const coalescer = createFakeCoalescer();
  const polymarketAdapter = createFakeAdapter(events, "polymarket-start", "polymarket-stop");
  const app = new CollectorApp({
    storage: storage as never,
    coalescer: coalescer as never,
    cryptoAdapter: null,
    polymarketAdapter: polymarketAdapter as never,
    coalesceIntervalMs: 500,
    clock: () => Date.now()
  });

  await app.start();
  await app.stop();

  assert.deepEqual(storage.events, ["storage-start", "storage-stop"]);
  assert.equal(coalescer.events.includes("coalescer-flush-all"), true);
  assert.deepEqual(events, ["polymarket-start", "polymarket-stop"]);
});

test("collector app create fails when enabledSources has no effective sources", () => {
  assert.throws(() => {
    CollectorApp.create({
      outputDir: "data",
      flushIntervalMs: 60_000,
      maxGzipPartBytes: 10_000,
      symbols: ["btc", "eth", "sol", "xrp"],
      windows: ["5m", "15m"],
      enabledSources: [],
      coalesceIntervalMs: 500
    });
  }, InvalidCollectorSourcesError);
});

test("collector app create only builds selected adapters", () => {
  const originalCryptoCreate = CryptoFeedAdapter.create;
  const originalPolymarketCreate = PolymarketFeedAdapter.create;
  const calls: string[] = [];
  (CryptoFeedAdapter as unknown as { create: typeof CryptoFeedAdapter.create }).create = ((...args: Parameters<typeof CryptoFeedAdapter.create>) => {
    calls.push("crypto");
    return originalCryptoCreate(...args);
  }) as typeof CryptoFeedAdapter.create;
  (PolymarketFeedAdapter as unknown as { create: typeof PolymarketFeedAdapter.create }).create = ((
    ...args: Parameters<typeof PolymarketFeedAdapter.create>
  ) => {
    calls.push("polymarket");
    return originalPolymarketCreate(...args);
  }) as typeof PolymarketFeedAdapter.create;
  try {
    CollectorApp.create({
      outputDir: "data",
      flushIntervalMs: 60_000,
      maxGzipPartBytes: 10_000,
      symbols: ["btc", "eth", "sol", "xrp"],
      windows: ["5m", "15m"],
      enabledSources: ["polymarket"],
      coalesceIntervalMs: 500
    });
  } finally {
    (CryptoFeedAdapter as unknown as { create: typeof CryptoFeedAdapter.create }).create = originalCryptoCreate;
    (PolymarketFeedAdapter as unknown as { create: typeof PolymarketFeedAdapter.create }).create = originalPolymarketCreate;
  }

  assert.deepEqual(calls, ["polymarket"]);
});
