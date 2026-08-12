// Contract tests for stable benchmark-environment identity.

import assert from "node:assert/strict";
import test from "node:test";

import { benchmarkEnvironmentIdentity } from "./environment.mjs";

test("environment identity ignores only capture time", () => {
  const first = environment("2026-08-12T00:00:00Z");
  const second = environment("2026-08-12T00:01:00Z");
  assert.equal(
    benchmarkEnvironmentIdentity(first),
    benchmarkEnvironmentIdentity(second),
  );
  second.host.cpu = "different";
  assert.notEqual(
    benchmarkEnvironmentIdentity(first),
    benchmarkEnvironmentIdentity(second),
  );
});

function environment(capturedAt) {
  return {
    schema: "kafkars.benchmark-environment.v1",
    captured_at: capturedAt,
    source: { kafka_client: { commit: "abc", dirty: false } },
    toolchain: { rustc: "rustc" },
    host: { cpu: "cpu" },
    broker: { version: "4.3.1" },
  };
}
