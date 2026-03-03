import * as assert from "node:assert/strict";
import { gunzipSync } from "node:zlib";
import { mkdtemp, readdir, readFile } from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { test } from "node:test";

import { GzipRotatingWriter } from "../../../src/collector/storage/gzip-rotating-writer.ts";

async function listFilesRecursively(root: string): Promise<string[]> {
  const entries = await readdir(root, { withFileTypes: true });
  const files: string[] = [];

  for (const entry of entries) {
    const fullPath = path.join(root, entry.name);
    if (entry.isDirectory()) {
      const nested = await listFilesRecursively(fullPath);
      files.push(...nested);
    } else {
      files.push(fullPath);
    }
  }

  return files;
}

test("gzip rotating writer writes ndjson and rotates by size", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "tick-collector-storage-"));
  const writer = GzipRotatingWriter.create({ outputDir: root, maxPartBytes: 200, flushIntervalMs: 60_000 });

  await writer.start();
  writer.append({
    eventId: "1",
    source: "crypto",
    eventType: "crypto.price",
    ingestedAt: Date.now(),
    exchangeTs: Date.now(),
    sequence: 1,
    symbol: "btc",
    provider: "binance",
    payload: { v: "a".repeat(400) }
  });
  writer.append({
    eventId: "2",
    source: "crypto",
    eventType: "crypto.trade",
    ingestedAt: Date.now(),
    exchangeTs: Date.now(),
    sequence: 2,
    symbol: "btc",
    provider: "binance",
    payload: { v: "b".repeat(400) }
  });
  await writer.stop();

  const files = await listFilesRecursively(root);
  const gzipFiles = files.filter((file) => file.endsWith(".ndjson.gz"));
  const manifestFiles = files.filter((file) => file.endsWith(".manifest.json"));
  const indexFiles = files.filter((file) => file.endsWith(".index.json"));
  assert.equal(gzipFiles.length >= 1, true);
  assert.equal(manifestFiles.length >= 1, true);
  assert.equal(indexFiles.length >= 1, true);

  const firstGzipPath = gzipFiles[0] ?? "";
  const firstGzip = await readFile(firstGzipPath);
  const raw = gunzipSync(firstGzip).toString("utf8");
  const firstLine = raw.split("\n")[0] ?? "";
  assert.equal(firstLine.includes('"eventId"'), true);
});

test("gzip rotating writer writes manifest and index on flush while service is running", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "tick-collector-storage-flush-"));
  const writer = GzipRotatingWriter.create({ outputDir: root, maxPartBytes: 64 * 1024 * 1024, flushIntervalMs: 60_000 });

  await writer.start();
  writer.append({
    eventId: "flush-1",
    source: "crypto",
    eventType: "crypto.price",
    ingestedAt: Date.now(),
    exchangeTs: Date.now(),
    sequence: 1,
    symbol: "btc",
    provider: "binance",
    payload: { value: 1 }
  });
  await writer.flush();

  const filesAfterFlush = await listFilesRecursively(root);
  const manifestFilesAfterFlush = filesAfterFlush.filter((file) => file.endsWith(".manifest.json"));
  const indexFilesAfterFlush = filesAfterFlush.filter((file) => file.endsWith(".index.json"));
  const manifestRawAfterFlush = await readFile(manifestFilesAfterFlush[0] ?? "", "utf8");
  const manifestAfterFlush = JSON.parse(manifestRawAfterFlush) as { isClosed: boolean };
  await writer.stop();
  const filesAfterStop = await listFilesRecursively(root);
  const manifestFilesAfterStop = filesAfterStop.filter((file) => file.endsWith(".manifest.json"));
  const manifestRawAfterStop = await readFile(manifestFilesAfterStop[0] ?? "", "utf8");
  const manifestAfterStop = JSON.parse(manifestRawAfterStop) as { isClosed: boolean };

  assert.equal(manifestFilesAfterFlush.length >= 1, true);
  assert.equal(indexFilesAfterFlush.length >= 1, true);
  assert.equal(manifestAfterFlush.isClosed, false);
  assert.equal(manifestAfterStop.isClosed, true);
});

test("gzip rotating writer partitions files by UTC hour", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "tick-collector-storage-hour-"));
  const writer = GzipRotatingWriter.create({ outputDir: root, maxPartBytes: 64 * 1024 * 1024, flushIntervalMs: 60_000 });
  const hourA = Date.UTC(2026, 2, 3, 16, 59, 59, 0);
  const hourB = Date.UTC(2026, 2, 3, 17, 0, 0, 10);

  await writer.start();
  writer.append({
    eventId: "h-a",
    source: "crypto",
    eventType: "crypto.price",
    ingestedAt: hourA,
    exchangeTs: hourA,
    sequence: 1,
    symbol: "btc",
    provider: "binance",
    payload: { value: 1 }
  });
  writer.append({
    eventId: "h-b",
    source: "crypto",
    eventType: "crypto.price",
    ingestedAt: hourB,
    exchangeTs: hourB,
    sequence: 2,
    symbol: "btc",
    provider: "binance",
    payload: { value: 2 }
  });
  await writer.stop();

  const files = await listFilesRecursively(root);
  const gzipFiles = files.filter((file) => file.endsWith(".ndjson.gz"));
  const has16Hour = gzipFiles.some((file) => file.includes("/2026/03/03/16/"));
  const has17Hour = gzipFiles.some((file) => file.includes("/2026/03/03/17/"));
  assert.equal(has16Hour, true);
  assert.equal(has17Hour, true);
});

test("gzip rotating writer recovers next partSequence after restart in same hour", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "tick-collector-storage-restart-"));
  const timestamp = Date.UTC(2026, 2, 3, 18, 0, 0, 5);
  const writerA = GzipRotatingWriter.create({ outputDir: root, maxPartBytes: 64 * 1024 * 1024, flushIntervalMs: 60_000 });

  await writerA.start();
  writerA.append({
    eventId: "r-a",
    source: "crypto",
    eventType: "crypto.price",
    ingestedAt: timestamp,
    exchangeTs: timestamp,
    sequence: 1,
    symbol: "btc",
    provider: "binance",
    payload: { value: 1 }
  });
  await writerA.stop();

  const writerB = GzipRotatingWriter.create({ outputDir: root, maxPartBytes: 64 * 1024 * 1024, flushIntervalMs: 60_000 });
  await writerB.start();
  writerB.append({
    eventId: "r-b",
    source: "crypto",
    eventType: "crypto.price",
    ingestedAt: timestamp + 500,
    exchangeTs: timestamp + 500,
    sequence: 2,
    symbol: "btc",
    provider: "binance",
    payload: { value: 2 }
  });
  await writerB.stop();

  const files = await listFilesRecursively(root);
  const manifestFiles = files.filter((file) => file.endsWith(".manifest.json"));
  const manifests = await Promise.all(
    manifestFiles.map(async (file) => {
      const raw = await readFile(file, "utf8");
      const parsed = JSON.parse(raw) as { partSequence: number; isClosed: boolean; hourBucketStartAt: number };
      return parsed;
    })
  );
  const sequences = manifests.map((manifest) => manifest.partSequence).sort((left, right) => left - right);
  assert.equal(sequences.includes(1), true);
  assert.equal(sequences.includes(2), true);
  assert.equal(
    manifests.every((manifest) => manifest.isClosed),
    true
  );
  assert.equal(
    manifests.every((manifest) => manifest.hourBucketStartAt === Date.UTC(2026, 2, 3, 18, 0, 0, 0)),
    true
  );
});
