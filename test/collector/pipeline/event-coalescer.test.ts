import * as assert from "node:assert/strict";
import { test } from "node:test";

import { EventCoalescer } from "../../../src/collector/pipeline/event-coalescer.ts";
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
