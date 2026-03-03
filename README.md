# @sha3/tick-collector

Real-time market data collector for TensorFlow dataset generation.

It connects to `@sha3/crypto` and `@sha3/polymarket`, subscribes continuously to crypto and Polymarket 5m/15m markets, and writes incremental compressed event logs (`.ndjson.gz`) to disk.

## Why It Exists

Training pipelines need reproducible, chronologically ordered market snapshots. This service provides a long-running ingestion process that normalizes multiple live feeds into one stable append-only event format.

## TL;DR (60s)

```bash
npm install
npm run check
npm run start
```

Collector output is written to `data/` by default.

## Quick Start

```bash
npm run start
```

Stop with `Ctrl+C` (`SIGINT`) to trigger graceful flush and shutdown.

## What It Collects

- Crypto feed events from `@sha3/crypto`:
  - `crypto.price`
  - `crypto.trade`
  - `crypto.orderbook`
  - `crypto.status`
- Polymarket stream events from `@sha3/polymarket`:
  - `polymarket.price`
  - `polymarket.book`

## Event Contract

Each persisted line is a `StoredEvent` JSON object:

```ts
type StoredEvent = {
  eventId: string;
  source: "crypto" | "polymarket";
  eventType: string;
  ingestedAt: number; // canonical timestamp (always present)
  exchangeTs?: number; // upstream timestamp, present when available
  sequence: number;
  symbol?: string;
  provider?: string;
  marketType?: "5m" | "15m";
  marketSlug?: string;
  marketStartAt?: number; // UTC epoch ms for market window start
  marketEventIndex?: number;
  assetId?: string;
  payload: unknown; // raw event payload
};
```

Non-applicable fields are omitted (not stored as `null`).

## JSON Line Types

Each line in `.ndjson.gz` is one `StoredEvent`. These are the concrete line categories:

- `crypto.price`
  - `source="crypto"`, `provider=<exchange|chainlink>`, `symbol=<btc|eth|sol|xrp>`, `payload.price`.
- `crypto.orderbook`
  - `source="crypto"`, `provider=<exchange>`, `symbol=<btc|eth|sol|xrp>`, `payload.bids/asks`.
- `crypto.trade`
  - `source="crypto"`, `provider=<exchange>`, `symbol=<btc|eth|sol|xrp>`, `payload.price/size`.
- `crypto.status`
  - `source="crypto"`, `provider=<provider>`, `payload.status/message`.
- `polymarket.price`
  - `source="polymarket"`, `symbol=<btc|eth|sol|xrp>`, `marketType=<5m|15m>`, `marketStartAt=<epoch-ms>`, `assetId=<token-id>`, `payload.price`.
- `polymarket.book`
  - `source="polymarket"`, `symbol=<btc|eth|sol|xrp>`, `marketType=<5m|15m>`, `marketStartAt=<epoch-ms>`, `assetId=<token-id>`, `payload.bids/asks`.

## Storage Layout

Partitioned by UTC hour:

```text
data/
  journal/YYYY/MM/DD/HH/part-<runId>-<seq>.ndjson.gz
  manifests/YYYY/MM/DD/HH/part-<runId>-<seq>.manifest.json
  manifests/YYYY/MM/DD/HH/part-<runId>-<seq>.index.json
```

Manifest fields:

- file
- indexFile
- runId
- partSequence
- hourBucketStartAt
- isClosed
- minIngestedAt
- maxIngestedAt
- eventCount
- sources
- eventTypes
- createdAt

## Integration Guide (External Projects)

1. Run this collector service in the environment with websocket egress.
2. Consume `.ndjson.gz` files from `data/journal` in chronological order.
3. Use `ingestedAt` as canonical ordering key in your training export step.
4. Use manifest files to speed up incremental dataset scans.

### Install + Import

```bash
npm install @sha3/tick-collector
```

```ts
import { buildCollectorApp, PersistedEventStream } from "@sha3/tick-collector";
```

### Embedding In Another Service

```ts
import { buildCollectorApp } from "@sha3/tick-collector";

const app = buildCollectorApp("./data");
await app.start();

process.on("SIGINT", () => {
  void app.stop();
});
```

## Public API Reference

Exports from `src/index.ts`:

- `buildCollectorApp(outputDir?: string): CollectorApp`
- `PersistedEventStream`
- `ReadDataPointOptions`
- `ReadDataPointRangeOptions`
- `MarketDataPoint`
- `CreateWindowIteratorOptions`
- `WindowEventBatch`
- `WindowEventIterator`
- `WindowIteratorAvailability`
- `WindowIteratorNextResult`

`CollectorApp` public methods:

- `start(): Promise<void>`
- `stop(): Promise<void>`

`PersistedEventStream`:

- `new PersistedEventStream({ folder })`
- `read({ timestamp, symbol, marketType, sources?, maxDistanceMs?, orderbookLevels? }): Promise<MarketDataPoint | null>`
- `readRange({ startTimestamp, endTimestamp, stepMs, symbol, marketType, sources?, maxDistanceMs?, orderbookLevels? }): Promise<MarketDataPoint[]>`
- `createWindowIterator({ symbol, marketType, startTimestamp?, pollIntervalMs?, signal? }): WindowEventIterator`

### Method Parameters

`read(options)`:

- `timestamp`:
  - Target UTC epoch milliseconds for the datapoint snapshot.
- `symbol`:
  - Market asset symbol (`btc`, `eth`, `sol`, `xrp`).
- `marketType`:
  - Window type (`5m` or `15m`).
- `sources` (optional):
  - Overrides enabled read sources for this call.
- `maxDistanceMs` (optional):
  - Maximum allowed time gap between `timestamp` and any selected source event.
  - Example: `30_000` means each selected event must be within 30 seconds of `timestamp`.
  - If no suitable event is found within this distance, that field is returned as missing (`null` in datapoint output + `coverage.missingFields` entry).
- `orderbookLevels` (optional):
  - Max bid/ask levels kept per orderbook snapshot (top-N depth).

`readRange(options)`:

- `startTimestamp`:
  - Inclusive UTC epoch milliseconds range start.
- `endTimestamp`:
  - Inclusive UTC epoch milliseconds range end.
- `stepMs`:
  - Sampling interval in milliseconds between datapoints (`> 0`).
- `symbol`, `marketType`, `sources?`, `maxDistanceMs?`, `orderbookLevels?`:
  - Same meaning as in `read(options)`.

`createWindowIterator(options)`:

- `symbol`:
  - Asset symbol (`btc`, `eth`, `sol`, `xrp`).
- `marketType`:
  - Window size (`5m` or `15m`).
- `startTimestamp` (optional):
  - Starting cursor timestamp. If omitted, iteration starts from the oldest available aligned window.
- `pollIntervalMs` (optional):
  - Poll frequency while waiting for the next window to close in continuous mode.
- `signal` (optional):
  - `AbortSignal` to stop iteration.

Example:

```ts
import { PersistedEventStream } from "@sha3/tick-collector";

const stream = new PersistedEventStream({ folder: "./data" });
const datapoint = await stream.read({
  timestamp: Date.now(),
  symbol: "btc",
  marketType: "5m",
  sources: ["binance", "coinbase", "kraken", "okx", "chainlink", "polymarket"],
  maxDistanceMs: 30_000,
  orderbookLevels: 20
});

if (datapoint) {
  console.log(datapoint.polymarket.upPrice, datapoint.coverage.missingFields);
}

const range = await stream.readRange({
  startTimestamp: Date.now() - 60_000,
  endTimestamp: Date.now(),
  stepMs: 5_000,
  symbol: "btc",
  marketType: "5m",
  maxDistanceMs: 30_000
});

console.log("points", range.length);

const iterator = stream.createWindowIterator({ symbol: "btc", marketType: "5m" });

const availability = await iterator.getAvailability();
console.log("availableWindows", availability.availableWindows);

const nextWindow = await iterator.next();
if (!nextWindow.done && nextWindow.value) {
  console.log(nextWindow.value.windowStartAt, nextWindow.value.events.length);
}
```

## Configuration Reference (`src/config.ts`)

`CONFIG` is exported as a single default object.

- `CONFIG.COLLECTOR.symbols`
  - Polymarket discovery symbols and crypto symbols (`btc/eth/sol/xrp`).
- `CONFIG.COLLECTOR.windows`
  - Polymarket windows (`5m`, `15m`).
- `CONFIG.COLLECTOR.enabledSources`
  - Active sources/providers (`binance`, `chainlink`, `polymarket`, etc.).
- `CONFIG.COLLECTOR.coalesceIntervalMs`
  - Temporal coalescing window; only the last event per source/type/instrument is persisted per bucket.
- `CONFIG.COLLECTOR.outputDir`
  - Base directory for journal/manifests.
- `CONFIG.COLLECTOR.flushIntervalMs`
  - Flush interval for incremental gzip writes.
- `CONFIG.COLLECTOR.maxGzipPartBytes`
  - Size threshold for gzip part rotation.
- `CONFIG.READER.defaultSources`
  - Default sources for datapoint reads (same flat source format as `enabledSources`).
- `CONFIG.READER.maxDistanceMs`
  - Maximum temporal distance allowed for nearest-event selection.
- `CONFIG.READER.orderbookLevels`
  - Top-N bids/asks returned in datapoint orderbooks.
- `CONFIG.READER.tieBreakerPolicy`
  - Tie break rule for nearest events (`prefer-past`).

Example:

```ts
enabledSources: ["binance", "chainlink", "polymarket"];
coalesceIntervalMs: 500;
```

## Compatibility

- Node.js 20+
- ESM runtime (`"type": "module"`)
- TypeScript strict mode
- Outbound websocket connectivity required

## Testing

```bash
npm run test
```

`npm run check` includes:

- lint
- format check
- typecheck
- test suite
- a live integration test that runs for ~30 seconds and requires all event types

## Scripts

- `npm run start`: run collector CLI
- `npm run check`: full validation pipeline
- `npm run fix`: lint/prettier autofix
- `npm run typecheck`: TypeScript validation
- `npm run test`: run tests (includes live integration)

## AI Usage

Assistants must follow `AGENTS.md` and `ai/*.md` as blocking rules.

Mandatory highlights:

- Class-first architecture with constructor injection.
- One public class per file.
- Single-return policy.
- Braces in all control-flow blocks.
- Typed errors at boundaries.
- Update tests for every behavior change.
- Run `npm run check` before finalizing.

## Troubleshooting

- `Websocket/network errors`: verify outbound websocket access to crypto providers and Polymarket APIs.
- `No output files`: ensure the process stays up long enough to hit flush intervals and that `CONFIG.COLLECTOR.enabledSources` is not empty.
- `Integration test failures`: `test/collector/integration/live-collector.test.ts` depends on real-time upstream feeds and can fail during upstream outages.
