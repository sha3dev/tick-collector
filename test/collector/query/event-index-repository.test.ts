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
