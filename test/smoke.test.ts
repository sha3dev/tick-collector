import * as assert from "node:assert/strict";
import { test } from "node:test";

import { buildCollectorApp } from "../src/index.ts";

test("buildCollectorApp creates collector instance", () => {
  const app = buildCollectorApp();
  assert.equal(typeof app.start, "function");
  assert.equal(typeof app.stop, "function");
});
