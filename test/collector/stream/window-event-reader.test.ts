import * as assert from "node:assert/strict";
import { test } from "node:test";

import { WindowEventReader } from "../../../src/collector/stream/window-event-reader.ts";
import type { StoredEvent } from "../../../src/collector/types/stored-event.ts";

type FakeRepository = {
  boundsResult: { minIngestedAt: number | null; maxIngestedAt: number | null };
  rangeByStart: Map<number, StoredEvent[]>;
  findBoundsForSymbolMarketType: (query: { symbol: string; marketType: string }) => Promise<{ minIngestedAt: number | null; maxIngestedAt: number | null }>;
  findEventsInRange: (query: { startTimestamp: number; endTimestampExclusive: number; symbol: string; marketType: string }) => Promise<StoredEvent[]>;
};

function createFakeRepository(): FakeRepository {
  const rangeByStart = new Map<number, StoredEvent[]>();
  const repository: FakeRepository = {
    boundsResult: { minIngestedAt: null, maxIngestedAt: null },
    rangeByStart,
    findBoundsForSymbolMarketType: async (): Promise<{ minIngestedAt: number | null; maxIngestedAt: number | null }> => {
      const result = repository.boundsResult;
      return result;
    },
    findEventsInRange: async (query): Promise<StoredEvent[]> => {
      const result = repository.rangeByStart.get(query.startTimestamp) ?? [];
      return result;
    }
  };
  return repository;
}

test("window event reader optional startTimestamp resolves to oldest available aligned window", async () => {
  const repository = createFakeRepository();
  repository.boundsResult = { minIngestedAt: 612_345, maxIngestedAt: 700_000 };
  const reader = WindowEventReader.create({ folder: "data", indexRepository: repository as never, clock: () => 9_999_999 });

  const initialStart = await reader.resolveInitialWindowStart({ symbol: "btc", marketType: "5m" });

  assert.equal(initialStart, 600_000);
});

test("window event reader availability counts closed windows from cursor", async () => {
  const repository = createFakeRepository();
  const reader = WindowEventReader.create({ folder: "data", indexRepository: repository as never, clock: () => 1_000_000 });

  const availability = await reader.getAvailability({ symbol: "btc", marketType: "5m", cursorWindowStartAt: 300_000 });

  assert.equal(availability.latestClosedWindowStartAt, 600_000);
  assert.equal(availability.availableWindows, 2);
});

test("window event reader emits empty windows when there are no events", async () => {
  const repository = createFakeRepository();
  const reader = WindowEventReader.create({ folder: "data", indexRepository: repository as never, clock: () => 1_000_000 });

  const batch = await reader.readWindowBatch({ symbol: "btc", marketType: "5m", windowStartAt: 300_000 });

  assert.equal(batch.events.length, 0);
  assert.equal(batch.stats.totalEvents, 0);
  assert.equal(batch.windowEndAt, 600_000);
});

test("window event reader keeps polymarket boundary event index=0 with new slug in next window", async () => {
  const repository = createFakeRepository();
  repository.rangeByStart.set(0, [
    {
      eventId: "old-last",
      source: "polymarket",
      eventType: "polymarket.price",
      ingestedAt: 299_999,
      sequence: 1,
      symbol: "btc",
      marketType: "5m",
      marketSlug: "slug-old",
      marketEventIndex: 99,
      payload: { price: 0.51 }
    }
  ]);
  repository.rangeByStart.set(300_000, [
    {
      eventId: "new-first",
      source: "polymarket",
      eventType: "polymarket.price",
      ingestedAt: 300_000,
      sequence: 2,
      symbol: "btc",
      marketType: "5m",
      marketSlug: "slug-new",
      marketEventIndex: 0,
      payload: { price: 0.49 }
    }
  ]);
  const reader = WindowEventReader.create({ folder: "data", indexRepository: repository as never, clock: () => 1_000_000 });

  const firstWindow = await reader.readWindowBatch({ symbol: "btc", marketType: "5m", windowStartAt: 0 });
  const secondWindow = await reader.readWindowBatch({ symbol: "btc", marketType: "5m", windowStartAt: 300_000 });

  assert.equal(firstWindow.events.length, 1);
  assert.equal(firstWindow.events[0]?.eventId, "old-last");
  assert.equal(secondWindow.events.length, 1);
  assert.equal(secondWindow.events[0]?.eventId, "new-first");
  assert.equal(secondWindow.stats.polymarketDistinctMarkets, 1);
});

test("window event reader never mixes polymarket 5m and 15m while keeping crypto symbol events", async () => {
  const repository = createFakeRepository();
  repository.rangeByStart.set(300_000, [
    {
      eventId: "crypto-btc",
      source: "crypto",
      eventType: "crypto.price",
      ingestedAt: 300_010,
      sequence: 1,
      symbol: "btc",
      provider: "binance",
      payload: { price: 100_000 }
    },
    {
      eventId: "poly-5m",
      source: "polymarket",
      eventType: "polymarket.price",
      ingestedAt: 300_020,
      sequence: 2,
      symbol: "btc",
      marketType: "5m",
      marketSlug: "slug-5m",
      marketEventIndex: 7,
      payload: { price: 0.6 }
    },
    {
      eventId: "poly-15m",
      source: "polymarket",
      eventType: "polymarket.price",
      ingestedAt: 300_030,
      sequence: 3,
      symbol: "btc",
      marketType: "15m",
      marketSlug: "slug-15m",
      marketEventIndex: 1,
      payload: { price: 0.4 }
    }
  ]);
  const reader = WindowEventReader.create({ folder: "data", indexRepository: repository as never, clock: () => 1_000_000 });

  const batch = await reader.readWindowBatch({ symbol: "btc", marketType: "5m", windowStartAt: 300_000 });

  assert.equal(batch.events.length, 2);
  assert.equal(batch.events[0]?.eventId, "crypto-btc");
  assert.equal(batch.events[1]?.eventId, "poly-5m");
  assert.equal(
    batch.events.some((event) => event.eventId === "poly-15m"),
    false
  );
  assert.equal(batch.stats.cryptoEvents, 1);
  assert.equal(batch.stats.polymarketEvents, 1);
});

test("window event reader excludes polymarket events whose marketStartAt maps to another window", async () => {
  const repository = createFakeRepository();
  repository.rangeByStart.set(300_000, [
    {
      eventId: "poly-correct-window",
      source: "polymarket",
      eventType: "polymarket.price",
      ingestedAt: 300_010,
      sequence: 1,
      symbol: "btc",
      marketType: "5m",
      marketStartAt: 300_000,
      marketSlug: "slug-correct",
      marketEventIndex: 0,
      payload: { price: 0.51 }
    },
    {
      eventId: "poly-previous-window-late",
      source: "polymarket",
      eventType: "polymarket.price",
      ingestedAt: 300_020,
      sequence: 2,
      symbol: "btc",
      marketType: "5m",
      marketStartAt: 0,
      marketSlug: "slug-old",
      marketEventIndex: 100,
      payload: { price: 0.49 }
    }
  ]);
  const reader = WindowEventReader.create({ folder: "data", indexRepository: repository as never, clock: () => 1_000_000 });

  const batch = await reader.readWindowBatch({ symbol: "btc", marketType: "5m", windowStartAt: 300_000 });

  assert.equal(batch.events.length, 1);
  assert.equal(batch.events[0]?.eventId, "poly-correct-window");
});

test("window event reader keeps one polymarket slug in same window using index/slug fallback", async () => {
  const repository = createFakeRepository();
  repository.rangeByStart.set(300_000, [
    {
      eventId: "poly-old-late",
      source: "polymarket",
      eventType: "polymarket.price",
      ingestedAt: 300_010,
      sequence: 9,
      symbol: "btc",
      marketType: "5m",
      marketSlug: "slug-old",
      marketEventIndex: 99,
      payload: { price: 0.47 }
    },
    {
      eventId: "poly-new-first",
      source: "polymarket",
      eventType: "polymarket.price",
      ingestedAt: 300_020,
      sequence: 10,
      symbol: "btc",
      marketType: "5m",
      marketSlug: "slug-new",
      marketEventIndex: 0,
      payload: { price: 0.53 }
    },
    {
      eventId: "poly-new-second",
      source: "polymarket",
      eventType: "polymarket.book",
      ingestedAt: 300_030,
      sequence: 11,
      symbol: "btc",
      marketType: "5m",
      marketSlug: "slug-new",
      marketEventIndex: 1,
      payload: { bids: [], asks: [] }
    }
  ]);
  const reader = WindowEventReader.create({ folder: "data", indexRepository: repository as never, clock: () => 1_000_000 });

  const batch = await reader.readWindowBatch({ symbol: "btc", marketType: "5m", windowStartAt: 300_000 });

  assert.equal(batch.events.length, 2);
  assert.equal(batch.events[0]?.eventId, "poly-new-first");
  assert.equal(batch.events[1]?.eventId, "poly-new-second");
  assert.equal(
    batch.events.some((event) => event.eventId === "poly-old-late"),
    false
  );
});
