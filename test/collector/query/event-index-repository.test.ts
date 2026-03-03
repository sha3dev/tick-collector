import * as assert from "node:assert/strict";
import { mkdir, mkdtemp, writeFile } from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { test } from "node:test";
import { gzipSync } from "node:zlib";

import { EventIndexRepository } from "../../../src/collector/query/event-index-repository.ts";
import type { EventIndexFile } from "../../../src/collector/query/types/event-index-types.ts";
import type { StoredEvent } from "../../../src/collector/types/stored-event.ts";

function buildStoredEvent(options: { eventId: string; ingestedAt: number; sequence: number; price: number }): StoredEvent {
  const event: StoredEvent = {
    eventId: options.eventId,
    source: "crypto",
    eventType: "crypto.price",
    ingestedAt: options.ingestedAt,
    exchangeTs: options.ingestedAt,
    sequence: options.sequence,
    symbol: "btc",
    provider: "binance",
    payload: { price: options.price }
  };
  return event;
}

test("event index repository selects closest candidate and prefers past on tie", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "tick-collector-index-"));
  const manifestFolder = path.join(root, "manifests/2026/03/02/17");
  const journalFolder = path.join(root, "journal/2026/03/02/17");
  await mkdir(manifestFolder, { recursive: true });
  await mkdir(journalFolder, { recursive: true });

  const partPath = path.join(journalFolder, "part-00000001.ndjson.gz");
  const events = [
    buildStoredEvent({ eventId: "past", ingestedAt: 900, sequence: 1, price: 10 }),
    buildStoredEvent({ eventId: "future", ingestedAt: 1100, sequence: 2, price: 11 })
  ];
  const payload = `${events.map((event) => JSON.stringify(event)).join("\n")}\n`;
  await writeFile(partPath, gzipSync(payload));

  const index: EventIndexFile = {
    candidates: [
      { partPath, ingestedAt: 900, sequence: 1, lineIndex: 0, source: "crypto", eventType: "crypto.price", provider: "binance", symbol: "btc" },
      { partPath, ingestedAt: 1100, sequence: 2, lineIndex: 1, source: "crypto", eventType: "crypto.price", provider: "binance", symbol: "btc" }
    ]
  };
  await writeFile(path.join(manifestFolder, "part-00000001.index.json"), JSON.stringify(index), "utf8");

  const repository = EventIndexRepository.create({ folder: root });
  const selected = await repository.findClosestEvent({
    timestamp: 1000,
    source: "crypto",
    eventType: "crypto.price",
    provider: "binance",
    symbol: "btc",
    maxDistanceMs: 500
  });

  assert.equal(selected?.event.eventId, "past");
});

test("event index repository applies maxDistanceMs constraint", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "tick-collector-index-max-"));
  const manifestFolder = path.join(root, "manifests/2026/03/02/17");
  const journalFolder = path.join(root, "journal/2026/03/02/17");
  await mkdir(manifestFolder, { recursive: true });
  await mkdir(journalFolder, { recursive: true });

  const partPath = path.join(journalFolder, "part-00000001.ndjson.gz");
  const event = buildStoredEvent({ eventId: "far", ingestedAt: 0, sequence: 1, price: 10 });
  await writeFile(partPath, gzipSync(`${JSON.stringify(event)}\n`));
  const index: EventIndexFile = {
    candidates: [{ partPath, ingestedAt: 0, sequence: 1, lineIndex: 0, source: "crypto", eventType: "crypto.price", provider: "binance", symbol: "btc" }]
  };
  await writeFile(path.join(manifestFolder, "part-00000001.index.json"), JSON.stringify(index), "utf8");

  const repository = EventIndexRepository.create({ folder: root });
  const selected = await repository.findClosestEvent({
    timestamp: 1000,
    source: "crypto",
    eventType: "crypto.price",
    provider: "binance",
    symbol: "btc",
    maxDistanceMs: 10
  });

  assert.equal(selected, null);
});

test("event index repository finds events in strict [start,end) range", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "tick-collector-index-range-"));
  const manifestFolder = path.join(root, "manifests/2026/03/02/17");
  const journalFolder = path.join(root, "journal/2026/03/02/17");
  await mkdir(manifestFolder, { recursive: true });
  await mkdir(journalFolder, { recursive: true });

  const partPath = path.join(journalFolder, "part-00000001.ndjson.gz");
  const inRangeEvent = buildStoredEvent({ eventId: "in", ingestedAt: 1000, sequence: 1, price: 10 });
  const boundaryEvent = buildStoredEvent({ eventId: "boundary", ingestedAt: 1100, sequence: 2, price: 11 });
  const payload = `${[inRangeEvent, boundaryEvent].map((event) => JSON.stringify(event)).join("\n")}\n`;
  await writeFile(partPath, gzipSync(payload));

  const index: EventIndexFile = {
    candidates: [
      { partPath, ingestedAt: 1000, sequence: 1, lineIndex: 0, source: "crypto", eventType: "crypto.price", provider: "binance", symbol: "btc" },
      { partPath, ingestedAt: 1100, sequence: 2, lineIndex: 1, source: "crypto", eventType: "crypto.price", provider: "binance", symbol: "btc" }
    ]
  };
  await writeFile(path.join(manifestFolder, "part-00000001.index.json"), JSON.stringify(index), "utf8");

  const repository = EventIndexRepository.create({ folder: root });
  const events = await repository.findEventsInRange({ startTimestamp: 1000, endTimestampExclusive: 1100, symbol: "btc", marketType: "5m" });

  assert.equal(events.length, 1);
  assert.equal(events[0]?.eventId, "in");
});

test("event index repository finds bounds for symbol and marketType across crypto and polymarket", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "tick-collector-index-bounds-"));
  const manifestFolder = path.join(root, "manifests/2026/03/02/17");
  const journalFolder = path.join(root, "journal/2026/03/02/17");
  await mkdir(manifestFolder, { recursive: true });
  await mkdir(journalFolder, { recursive: true });

  const partPath = path.join(journalFolder, "part-00000001.ndjson.gz");
  const events: StoredEvent[] = [
    buildStoredEvent({ eventId: "crypto-old", ingestedAt: 900, sequence: 1, price: 10 }),
    {
      eventId: "poly-mid",
      source: "polymarket",
      eventType: "polymarket.price",
      ingestedAt: 1000,
      sequence: 2,
      symbol: "btc",
      marketType: "5m",
      marketSlug: "slug-a",
      marketEventIndex: 0,
      payload: { price: 0.5 }
    },
    {
      eventId: "poly-new",
      source: "polymarket",
      eventType: "polymarket.book",
      ingestedAt: 1200,
      sequence: 3,
      symbol: "btc",
      marketType: "5m",
      marketSlug: "slug-a",
      marketEventIndex: 1,
      payload: { bids: [], asks: [] }
    }
  ];
  const payload = `${events.map((event) => JSON.stringify(event)).join("\n")}\n`;
  await writeFile(partPath, gzipSync(payload));

  const index: EventIndexFile = {
    candidates: [
      { partPath, ingestedAt: 900, sequence: 1, lineIndex: 0, source: "crypto", eventType: "crypto.price", provider: "binance", symbol: "btc" },
      {
        partPath,
        ingestedAt: 1000,
        sequence: 2,
        lineIndex: 1,
        source: "polymarket",
        eventType: "polymarket.price",
        symbol: "btc",
        marketType: "5m",
        marketSlug: "slug-a",
        marketEventIndex: 0
      },
      {
        partPath,
        ingestedAt: 1200,
        sequence: 3,
        lineIndex: 2,
        source: "polymarket",
        eventType: "polymarket.book",
        symbol: "btc",
        marketType: "5m",
        marketSlug: "slug-a",
        marketEventIndex: 1
      }
    ]
  };
  await writeFile(path.join(manifestFolder, "part-00000001.index.json"), JSON.stringify(index), "utf8");

  const repository = EventIndexRepository.create({ folder: root });
  const bounds = await repository.findBoundsForSymbolMarketType({ symbol: "btc", marketType: "5m" });

  assert.equal(bounds.minIngestedAt, 900);
  assert.equal(bounds.maxIngestedAt, 1200);
});

test("event index repository refreshIndices loads newly created index file incrementally", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "tick-collector-index-refresh-"));
  const manifestFolder = path.join(root, "manifests/2026/03/02/17");
  const journalFolder = path.join(root, "journal/2026/03/02/17");
  await mkdir(manifestFolder, { recursive: true });
  await mkdir(journalFolder, { recursive: true });

  const partPathA = path.join(journalFolder, "part-00000001.ndjson.gz");
  const eventA = buildStoredEvent({ eventId: "a", ingestedAt: 1000, sequence: 1, price: 10 });
  await writeFile(partPathA, gzipSync(`${JSON.stringify(eventA)}\n`));
  const indexA: EventIndexFile = {
    candidates: [
      { partPath: partPathA, ingestedAt: 1000, sequence: 1, lineIndex: 0, source: "crypto", eventType: "crypto.price", provider: "binance", symbol: "btc" }
    ]
  };
  await writeFile(path.join(manifestFolder, "part-00000001.index.json"), JSON.stringify(indexA), "utf8");

  const repository = EventIndexRepository.create({ folder: root });
  const first = await repository.findEventsInRange({ startTimestamp: 1000, endTimestampExclusive: 1001, symbol: "btc", marketType: "5m" });

  const partPathB = path.join(journalFolder, "part-00000002.ndjson.gz");
  const eventB = buildStoredEvent({ eventId: "b", ingestedAt: 1300, sequence: 2, price: 11 });
  await writeFile(partPathB, gzipSync(`${JSON.stringify(eventB)}\n`));
  const indexB: EventIndexFile = {
    candidates: [
      { partPath: partPathB, ingestedAt: 1300, sequence: 2, lineIndex: 0, source: "crypto", eventType: "crypto.price", provider: "binance", symbol: "btc" }
    ]
  };
  await writeFile(path.join(manifestFolder, "part-00000002.index.json"), JSON.stringify(indexB), "utf8");

  const second = await repository.findEventsInRange({ startTimestamp: 1000, endTimestampExclusive: 2000, symbol: "btc", marketType: "5m" });

  assert.equal(first.length, 1);
  assert.equal(first[0]?.eventId, "a");
  assert.equal(second.length, 2);
  assert.equal(second[0]?.eventId, "a");
  assert.equal(second[1]?.eventId, "b");
});

test("event index repository range keeps crypto by symbol and polymarket only for requested marketType", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "tick-collector-index-markettype-"));
  const manifestFolder = path.join(root, "manifests/2026/03/02/17");
  const journalFolder = path.join(root, "journal/2026/03/02/17");
  await mkdir(manifestFolder, { recursive: true });
  await mkdir(journalFolder, { recursive: true });

  const partPath = path.join(journalFolder, "part-00000001.ndjson.gz");
  const events: StoredEvent[] = [
    buildStoredEvent({ eventId: "crypto-btc", ingestedAt: 1000, sequence: 1, price: 10 }),
    {
      eventId: "poly-5m",
      source: "polymarket",
      eventType: "polymarket.price",
      ingestedAt: 1001,
      sequence: 2,
      symbol: "btc",
      marketType: "5m",
      payload: { price: 0.55 }
    },
    {
      eventId: "poly-15m",
      source: "polymarket",
      eventType: "polymarket.price",
      ingestedAt: 1002,
      sequence: 3,
      symbol: "btc",
      marketType: "15m",
      payload: { price: 0.45 }
    }
  ];
  await writeFile(partPath, gzipSync(`${events.map((event) => JSON.stringify(event)).join("\n")}\n`));

  const index: EventIndexFile = {
    candidates: [
      { partPath, ingestedAt: 1000, sequence: 1, lineIndex: 0, source: "crypto", eventType: "crypto.price", provider: "binance", symbol: "btc" },
      { partPath, ingestedAt: 1001, sequence: 2, lineIndex: 1, source: "polymarket", eventType: "polymarket.price", symbol: "btc", marketType: "5m" },
      { partPath, ingestedAt: 1002, sequence: 3, lineIndex: 2, source: "polymarket", eventType: "polymarket.price", symbol: "btc", marketType: "15m" }
    ]
  };
  await writeFile(path.join(manifestFolder, "part-00000001.index.json"), JSON.stringify(index), "utf8");

  const repository = EventIndexRepository.create({ folder: root });
  const eventsIn5m = await repository.findEventsInRange({
    startTimestamp: 1000,
    endTimestampExclusive: 2000,
    symbol: "btc",
    marketType: "5m"
  });

  assert.equal(eventsIn5m.length, 2);
  assert.equal(eventsIn5m[0]?.eventId, "crypto-btc");
  assert.equal(eventsIn5m[1]?.eventId, "poly-5m");
});
