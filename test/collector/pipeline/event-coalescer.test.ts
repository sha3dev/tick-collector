import * as assert from "node:assert/strict";
import { test } from "node:test";

import { EventCoalescer } from "../../../src/collector/pipeline/event-coalescer.ts";
import type { CoalescedWindowSummary } from "../../../src/collector/pipeline/types/coalesced-window-summary.ts";
import type { StoredEvent } from "../../../src/collector/types/stored-event.ts";

function buildEvent(options: {
  eventId: string;
  ingestedAt: number;
  sequence: number;
  eventType: string;
  provider?: string;
  symbol?: string;
  assetId?: string;
  source?: "crypto" | "polymarket";
}): StoredEvent {
  const source = options.source ?? "crypto";
  const event: StoredEvent = {
    eventId: options.eventId,
    source,
    eventType: options.eventType,
    ingestedAt: options.ingestedAt,
    exchangeTs: options.ingestedAt,
    sequence: options.sequence,
    ...(source === "crypto" || options.symbol !== undefined ? { symbol: options.symbol ?? "btc" } : {}),
    ...(source === "crypto" || options.provider !== undefined ? { provider: options.provider ?? "binance" } : {}),
    ...(source === "polymarket" || options.assetId !== undefined ? { assetId: options.assetId ?? "asset-a" } : {}),
    payload: {}
  };
  return event;
}

test("event coalescer keeps only last event per key in same bucket", async () => {
  const emitted: StoredEvent[] = [];
  const coalescer = EventCoalescer.create({
    intervalMs: 500,
    onEmitMany: async (events) => {
      emitted.push(...events);
    }
  });

  await coalescer.append(buildEvent({ eventId: "1", ingestedAt: 100, sequence: 1, eventType: "crypto.price" }));
  await coalescer.append(buildEvent({ eventId: "2", ingestedAt: 300, sequence: 2, eventType: "crypto.price" }));
  await coalescer.flushReady(600);

  assert.equal(emitted.length, 1);
  assert.equal(emitted[0]?.eventId, "2");
});

test("event coalescer keeps different keys in same bucket", async () => {
  const emitted: StoredEvent[] = [];
  const coalescer = EventCoalescer.create({
    intervalMs: 500,
    onEmitMany: async (events) => {
      emitted.push(...events);
    }
  });

  await coalescer.append(buildEvent({ eventId: "1", ingestedAt: 100, sequence: 1, eventType: "crypto.price", symbol: "btc" }));
  await coalescer.append(buildEvent({ eventId: "2", ingestedAt: 100, sequence: 2, eventType: "crypto.price", symbol: "eth" }));
  await coalescer.flushReady(600);

  assert.equal(emitted.length, 2);
});

test("event coalescer flushAll drains pending buckets", async () => {
  const emitted: StoredEvent[] = [];
  const coalescer = EventCoalescer.create({
    intervalMs: 500,
    onEmitMany: async (events) => {
      emitted.push(...events);
    }
  });

  await coalescer.append(buildEvent({ eventId: "1", ingestedAt: 1000, sequence: 1, eventType: "polymarket.price", source: "polymarket", assetId: "asset-a" }));
  await coalescer.append(buildEvent({ eventId: "2", ingestedAt: 1200, sequence: 2, eventType: "polymarket.book", source: "polymarket", assetId: "asset-a" }));
  await coalescer.flushAll();

  assert.equal(emitted.length, 2);
});

test("event coalescer emits window summary with counts by event type", async () => {
  const emitted: StoredEvent[] = [];
  const summaries: CoalescedWindowSummary[] = [];
  const coalescer = EventCoalescer.create({
    intervalMs: 500,
    onEmitMany: async (events) => {
      emitted.push(...events);
    },
    onWindowEmitted: (summary) => {
      summaries.push(summary);
    }
  });

  await coalescer.append(buildEvent({ eventId: "1", ingestedAt: 100, sequence: 1, eventType: "crypto.price" }));
  await coalescer.append(buildEvent({ eventId: "2", ingestedAt: 120, sequence: 2, eventType: "crypto.trade" }));
  await coalescer.append(buildEvent({ eventId: "3", ingestedAt: 130, sequence: 3, eventType: "crypto.price", symbol: "eth" }));
  await coalescer.flushReady(600);

  assert.equal(emitted.length, 3);
  assert.equal(summaries.length, 1);
  assert.equal(summaries[0]?.windowStartAt, 0);
  assert.equal(summaries[0]?.windowEndAt, 500);
  assert.equal(summaries[0]?.eventCount, 3);
  assert.deepEqual(summaries[0]?.eventTypeCounts, [
    { eventType: "crypto.price", count: 2 },
    { eventType: "crypto.trade", count: 1 }
  ]);
});

test("event coalescer emits closed window only once under concurrent flush", async () => {
  const emittedBatches: StoredEvent[][] = [];
  const summaries: CoalescedWindowSummary[] = [];
  let unblockEmission: (() => void) | null = null;
  const waitForEmission = new Promise<void>((resolve) => {
    unblockEmission = resolve;
  });
  const coalescer = EventCoalescer.create({
    intervalMs: 500,
    onEmitMany: async (events) => {
      emittedBatches.push(events);
      await waitForEmission;
    },
    onWindowEmitted: (summary) => {
      summaries.push(summary);
    }
  });

  await coalescer.append(buildEvent({ eventId: "1", ingestedAt: 100, sequence: 1, eventType: "crypto.price" }));
  const firstFlush = coalescer.flushReady(600);
  const secondFlush = coalescer.flushReady(600);
  const releaseEmission =
    unblockEmission ??
    (() => {
      // empty
    });
  releaseEmission();
  await Promise.all([firstFlush, secondFlush]);

  assert.equal(emittedBatches.length, 1);
  assert.equal(summaries.length, 1);
});
