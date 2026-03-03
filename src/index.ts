import { fileURLToPath } from "node:url";

import CONFIG from "./config.ts";
import { CollectorApp } from "./collector/runtime/collector-app.ts";
import { CollectorCli } from "./collector/runtime/collector-cli.ts";
export { PersistedEventStream } from "./collector/stream/persisted-event-stream.ts";
export type { PersistedEventStreamOptions } from "./collector/stream/persisted-event-stream.ts";
export type { MarketDataPoint, DataPointCoverage, SelectedEventMeta } from "./collector/query/types/market-data-point.ts";
export type { ReadDataPointOptions } from "./collector/query/types/read-data-point-options.ts";
export type { ReadDataPointRangeOptions } from "./collector/query/types/read-data-point-range-options.ts";
export type { ReadSourcesFilter } from "./collector/query/types/read-sources-filter.ts";

export function buildCollectorApp(outputDir?: string): CollectorApp {
  const app = CollectorApp.create({
    outputDir: outputDir ?? CONFIG.COLLECTOR.outputDir,
    flushIntervalMs: CONFIG.COLLECTOR.flushIntervalMs,
    maxGzipPartBytes: CONFIG.COLLECTOR.maxGzipPartBytes,
    symbols: [...CONFIG.COLLECTOR.symbols],
    windows: [...CONFIG.COLLECTOR.windows],
    enabledSources: [...CONFIG.COLLECTOR.enabledSources],
    coalesceIntervalMs: CONFIG.COLLECTOR.coalesceIntervalMs
  });
  return app;
}

function isMainModule(): boolean {
  const executedPath = process.argv[1] ?? "";
  const currentPath = fileURLToPath(import.meta.url);
  const isMain = executedPath === currentPath;
  return isMain;
}

if (isMainModule()) {
  const app = buildCollectorApp();
  const cli = CollectorCli.create({ app });
  void cli.run();
}
