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
  ingestedAt: number; // canonical timestamp
  exchangeTs: number | null; // informational only
  sequence: number;
  symbol: string | null;
  provider: string | null;
  marketSlug: string | null;
  assetId: string | null;
  payload: unknown; // raw event payload
};
```

## Storage Layout

Chronological partitioning by ingest hour:

```text
data/
  journal/YYYY/MM/DD/HH/part-XXXXXXXX.ndjson.gz
  manifests/YYYY/MM/DD/HH/part-XXXXXXXX.manifest.json
```

Manifest fields:

- file
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

`CollectorApp` public methods:

- `start(): Promise<void>`
- `stop(): Promise<void>`

`PersistedEventStream`:

- `new PersistedEventStream({ folder, minIngestedAtExclusive? })`
- `readNext(): Promise<StoredEvent | null>`

Example:

```ts
import { PersistedEventStream } from "@sha3/tick-collector";

const stream = new PersistedEventStream({ folder: "./data", minIngestedAtExclusive: Date.now() - 60_000 });

for (;;) {
  const event = await stream.readNext();
  if (!event) {
    break;
  }

  console.log(event.ingestedAt, event.eventType);
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
