import * as assert from "node:assert/strict";
import { mkdtemp, readdir, readFile } from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { test } from "node:test";
import { gunzipSync } from "node:zlib";

import CONFIG from "../../../src/config.ts";
import { buildCollectorApp } from "../../../src/index.ts";

type StoredEventLike = { eventType: string; source: "crypto" | "polymarket"; provider: string | null };

async function waitFor(ms: number): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(() => {
      resolve();
    }, ms);
  });
}

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

function parseStoredEvents(contents: string): StoredEventLike[] {
  const lines = contents
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  const events = lines.map((line) => JSON.parse(line) as StoredEventLike);
  return events;
}

test("live collector ingests events from enabled sources in 30 seconds", { timeout: 120_000 }, async () => {
  const outputDir = await mkdtemp(path.join(os.tmpdir(), "tick-collector-live-"));
  const app = buildCollectorApp(outputDir);
  const enabledSources = new Set<string>(CONFIG.COLLECTOR.enabledSources);
  const requiredTypes = new Set<string>();

  if (enabledSources.has("polymarket")) {
    requiredTypes.add("polymarket.price");
    requiredTypes.add("polymarket.book");
  }

  if (enabledSources.has("binance") || enabledSources.has("chainlink")) {
    requiredTypes.add("crypto.price");
    requiredTypes.add("crypto.status");
  }

  if (enabledSources.has("binance")) {
    requiredTypes.add("crypto.trade");
    requiredTypes.add("crypto.orderbook");
  }

  await app.start();
  await waitFor(30_000);
  await app.stop();

  const allFiles = await listFilesRecursively(outputDir);
  const gzipFiles = allFiles.filter((filePath) => filePath.endsWith(".ndjson.gz"));
  assert.equal(gzipFiles.length > 0, true);

  const observedTypes = new Set<string>();
  const observedSources = new Set<string>();

  for (const gzipFile of gzipFiles) {
    const compressed = await readFile(gzipFile);
    const decompressed = gunzipSync(compressed).toString("utf8");
    const events = parseStoredEvents(decompressed);

    for (const event of events) {
      observedTypes.add(event.eventType);
      if (event.source === "polymarket") {
        observedSources.add("polymarket");
      }
      if (event.source === "crypto" && event.provider) {
        observedSources.add(event.provider);
      }
    }
  }

  for (const requiredType of requiredTypes) {
    assert.equal(observedTypes.has(requiredType), true, `missing required event type: ${requiredType}`);
  }

  for (const observedSource of observedSources) {
    assert.equal(enabledSources.has(observedSource), true, `observed source is not enabled: ${observedSource}`);
  }
});
