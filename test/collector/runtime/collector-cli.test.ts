import * as assert from "node:assert/strict";
import { test } from "node:test";

import { CollectorBootstrapError } from "../../../src/collector/errors/collector-bootstrap-error.ts";
import { CollectorCli } from "../../../src/collector/runtime/collector-cli.ts";
import type { CollectorLogger } from "../../../src/collector/types/collector-logger.ts";

type FakeApp = { start: () => Promise<void>; stop: () => Promise<void> };

type FakeLogger = { logger: CollectorLogger; entries: string[] };

function createFakeLogger(): FakeLogger {
  const entries: string[] = [];
  const logger: CollectorLogger = {
    debug: (value: string): void => {
      entries.push(`debug:${value}`);
    },
    info: (value: string): void => {
      entries.push(`info:${value}`);
    },
    warn: (value: string): void => {
      entries.push(`warn:${value}`);
    },
    error: (value: string): void => {
      entries.push(`error:${value}`);
    }
  };
  const fakeLogger = { logger, entries };
  return fakeLogger;
}

test("collector cli run starts/stops app and emits lifecycle logs", async () => {
  const appEvents: string[] = [];
  const app: FakeApp = {
    start: async (): Promise<void> => {
      appEvents.push("start");
    },
    stop: async (): Promise<void> => {
      appEvents.push("stop");
    }
  };
  const fakeLogger = createFakeLogger();
  const cli = CollectorCli.create({ app: app as never, logger: fakeLogger.logger });
  const mutableCli = cli as unknown as { waitForSignals: () => Promise<void> };
  mutableCli.waitForSignals = async (): Promise<void> => {
    appEvents.push("wait-signals");
  };

  await cli.run();

  assert.deepEqual(appEvents, ["start", "wait-signals", "stop"]);
  assert.deepEqual(fakeLogger.entries, [
    "info:starting collector app",
    "info:collector app started",
    "info:shutdown signal received",
    "info:collector app stopped"
  ]);
});

test("collector cli run logs error and throws bootstrap error on failure", async () => {
  const app: FakeApp = {
    start: async (): Promise<void> => {
      throw new Error("boom");
    },
    stop: async (): Promise<void> => {
      // empty
    }
  };
  const fakeLogger = createFakeLogger();
  const cli = CollectorCli.create({ app: app as never, logger: fakeLogger.logger });

  await assert.rejects(
    async (): Promise<void> => {
      await cli.run();
    },
    (error: unknown): boolean => {
      const isExpectedError = error instanceof CollectorBootstrapError;
      return isExpectedError;
    }
  );

  assert.deepEqual(fakeLogger.entries, ["info:starting collector app", "error:collector cli failed: boom"]);
});
