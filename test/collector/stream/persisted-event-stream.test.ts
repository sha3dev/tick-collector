import * as assert from "node:assert/strict";
import { mkdir, mkdtemp, writeFile } from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { test } from "node:test";
import { gzipSync } from "node:zlib";

import { PersistedEventStream } from "../../../src/collector/stream/persisted-event-stream.ts";
import type { StoredEvent } from "../../../src/collector/types/stored-event.ts";

async function writePartFile(root: string, relativePath: string, events: StoredEvent[]): Promise<void> {
  const fullPath = path.join(root, relativePath);
  const folder = path.dirname(fullPath);
  const payload = `${events.map((event) => JSON.stringify(event)).join("\n")}\n`;
  await mkdir(folder, { recursive: true });
  await writeFile(fullPath, gzipSync(payload));
}

function buildEvent(options: { eventId: string; ingestedAt: number; sequence: number }): StoredEvent {
  const event: StoredEvent = {
    eventId: options.eventId,
    source: "crypto",
    eventType: "crypto.price",
    ingestedAt: options.ingestedAt,
    exchangeTs: options.ingestedAt,
    sequence: options.sequence,
    symbol: "btc",
    provider: "binance",
    marketSlug: null,
    assetId: null,
    payload: { id: options.eventId }
  };
  return event;
}

test("persisted event stream reads events one by one in chronological ascending order", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "tick-collector-stream-"));
  await writePartFile(root, "journal/2026/03/02/17/part-00000001.ndjson.gz", [buildEvent({ eventId: "b", ingestedAt: 200, sequence: 2 })]);
  await writePartFile(root, "journal/2026/03/02/17/part-00000002.ndjson.gz", [buildEvent({ eventId: "a", ingestedAt: 100, sequence: 1 })]);
  await writePartFile(root, "journal/2026/03/02/17/part-00000003.ndjson.gz", [buildEvent({ eventId: "c", ingestedAt: 300, sequence: 3 })]);

  const stream = PersistedEventStream.create({ folder: root });
  const first = await stream.readNext();
  const second = await stream.readNext();
  const third = await stream.readNext();
  const fourth = await stream.readNext();

  assert.equal(first?.eventId, "a");
  assert.equal(second?.eventId, "b");
  assert.equal(third?.eventId, "c");
  assert.equal(fourth, null);
});

test("persisted event stream applies minIngestedAtExclusive filter", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "tick-collector-stream-filter-"));
  await writePartFile(root, "journal/2026/03/02/17/part-00000001.ndjson.gz", [
    buildEvent({ eventId: "a", ingestedAt: 100, sequence: 1 }),
    buildEvent({ eventId: "b", ingestedAt: 200, sequence: 2 }),
    buildEvent({ eventId: "c", ingestedAt: 300, sequence: 3 })
  ]);

  const stream = PersistedEventStream.create({ folder: root, minIngestedAtExclusive: 200 });
  const first = await stream.readNext();
  const second = await stream.readNext();

  assert.equal(first?.eventId, "c");
  assert.equal(second, null);
});
