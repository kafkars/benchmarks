// Evidence-contract tests for scheduled fixed-load comparisons.

import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  intendedOffset,
  validateFixedLatencyCsv,
  validateFixedProducerResult,
} from "./fixed-validation.mjs";

const settings = {
  acks: "all",
  idempotence: true,
  compression: "none",
};
const contract = {
  adapter: "kafkars",
  topic: "fixed-topic",
  runId: "0123456789abcdef",
  records: 2,
  payloadBytes: 64,
  offeredRate: 1_000,
  callers: 4,
  settings,
};
const latency = { p50: 1, p95: 2, p99: 2, p999: 2, max: 2 };
const result = {
  schema: "kafkars.producer-fixed-load.v1",
  adapter: "kafkars",
  run_id: contract.runId,
  topic: contract.topic,
  offered_records: 2,
  accepted_records: 2,
  acknowledged_records: 2,
  failed_records: 0,
  payload_bytes: 64,
  acknowledged_payload_bytes: 128,
  load: {
    mode: "scheduled-open-loop-fixed-rate",
    callers_per_producer: 4,
    offered_records_per_second: 1_000,
    schedule_span_ns: 1_000_000,
    drain_duration_ns: 2_000_000,
    admission_pressure_records: 0,
  },
  acknowledged_records_per_second_including_drain: 1_000,
  latency_ns: {
    uncorrected: latency,
    corrected: latency,
    schedule_delay: latency,
  },
  settings,
  valid: true,
};

test("fixed-load result validation rejects schedule or pressure drift", () => {
  assert.doesNotThrow(() => validateFixedProducerResult(result, contract));
  assert.throws(() =>
    validateFixedProducerResult(
      { ...result, load: { ...result.load, admission_pressure_records: 1 } },
      contract,
    ),
  );
});

test("fixed-load latency rows prove corrected and uncorrected clocks", () => {
  const directory = mkdtempSync(join(tmpdir(), "kafkars-fixed-test-"));
  const path = join(directory, "latency.csv");
  try {
    writeFileSync(
      path,
      "sequence,intended_ns,admitted_ns,completed_ns,uncorrected_latency_ns,corrected_latency_ns\n0,0,100,1100,1000,1100\n1,1000000,1000100,1002100,2000,2100\n",
    );
    assert.doesNotThrow(() => validateFixedLatencyCsv(path, 2, 1_000));
    writeFileSync(
      path,
      "sequence,intended_ns,admitted_ns,completed_ns,uncorrected_latency_ns,corrected_latency_ns\n0,0,100,1100,999,1100\n1,1000000,1000100,1002100,2000,2100\n",
    );
    assert.throws(() => validateFixedLatencyCsv(path, 2, 1_000));
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("fixed-load schedule offsets use exact integer nanoseconds", () => {
  assert.equal(intendedOffset(99_999, 100_000), 999_990_000);
  assert.throws(() => intendedOffset(1, 0));
});
