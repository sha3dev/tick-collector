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
    marketSlug: null,
    assetId: null,
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
    marketSlug: null,
    assetId: null,
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
