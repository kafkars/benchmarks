// Negative and positive evidence-contract tests for the benchmark sealer.

import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  positiveInteger,
  validateExecutionOrder,
  validateKafkarsNativeMetrics,
  validateLatencyCsv,
  validateProducerResult,
  validateVerification,
} from "./validation.mjs";
import { renderResolvedWorkload } from "./workload.mjs";

const settings = {
  acks: "all",
  idempotence: true,
  compression: "none",
  linger_ms: 5,
  batch_records: 256,
  batch_bytes: 65_536,
  request_bytes: 1_048_576,
  max_in_flight_requests_per_broker: 5,
  queue_bytes: 67_108_864,
  max_outstanding_records: 8,
  retry_max_replacements: 600,
  retry_backoff_ms: 100,
  explicit_balanced_partitioning: true,
  admission_shape: "public-batch",
  completion_shape: "aggregate-batch-terminal",
};

const result = {
  schema: "kafkars.producer-benchmark.v1",
  adapter: "kafkars",
  adapter_version: "0.1.0",
  run_id: "0123456789abcdef",
  topic: "kafkars-bench-0123456789abcdef-kafkars",
  offered_records: 2,
  accepted_records: 2,
  acknowledged_records: 2,
  failed_records: 0,
  payload_bytes: 64,
  acknowledged_payload_bytes: 128,
  duration_ns: 1_000,
  acknowledged_records_per_second: 2_000,
  acknowledged_mib_per_second: 0.125,
  latency_ns: { p50: 10, p95: 20, p99: 20, p999: 20, max: 20 },
  settings,
  native_metrics: {
    producer_requests: {
      requests: 1,
      partition_batches: 2,
      records: 2,
      encoded_record_bytes: 128,
      peak_in_flight_requests: 5,
      peak_in_flight_requests_per_broker: 5,
    },
  },
  valid: true,
};

const verification = {
  schema: "kafkars.producer-verification.v1",
  topic: result.topic,
  expected_records: 2,
  verified_records: 2,
  duplicates: 0,
  missing_records: 0,
  corrupt: 0,
  unexpected: 0,
  eof_partitions: 2,
  valid: true,
};

test("scalar and execution-order inputs fail closed", () => {
  assert.equal(positiveInteger("records", "2"), 2);
  assert.equal(positiveInteger("warmup", "0", true), 0);
  assert.deepEqual(validateExecutionOrder("kafkars,librdkafka-c"), [
    "kafkars",
    "librdkafka-c",
  ]);
  assert.throws(() => positiveInteger("records", "2x"));
  assert.throws(() => positiveInteger("records", "0"));
  assert.throws(() => validateExecutionOrder("kafkars,kafkars"));
});

test("native producer metrics enforce the configured request gate", () => {
  assert.doesNotThrow(() => validateKafkarsNativeMetrics(result, 2, 5, 3));
  assert.throws(() =>
    validateKafkarsNativeMetrics(
      {
        ...result,
        native_metrics: {
          producer_requests: {
            ...result.native_metrics.producer_requests,
            peak_in_flight_requests_per_broker: 6,
          },
        },
      },
      2,
      5,
      3,
    ),
  );
});

test("producer and verifier contracts reject semantic tampering", () => {
  assert.doesNotThrow(() =>
    validateProducerResult(
      result,
      "kafkars",
      result.topic,
      result.run_id,
      2,
      64,
      settings,
    ),
  );
  assert.doesNotThrow(() =>
    validateVerification(verification, result.topic, 2, 2),
  );
  assert.throws(() =>
    validateProducerResult(
      { ...result, acknowledged_records: 1 },
      "kafkars",
      result.topic,
      result.run_id,
      2,
      64,
      settings,
    ),
  );
  assert.throws(() =>
    validateProducerResult(
      { ...result, settings: { ...settings, linger_ms: 6 } },
      "kafkars",
      result.topic,
      result.run_id,
      2,
      64,
      settings,
    ),
  );
  assert.throws(() =>
    validateVerification({ ...verification, eof_partitions: 1 }, result.topic, 2, 2),
  );
});

test("latency evidence requires exact unique and internally consistent rows", () => {
  const directory = mkdtempSync(join(tmpdir(), "kafkars-latency-test-"));
  const path = join(directory, "latency.csv");
  try {
    writeFileSync(
      path,
      "sequence,admitted_ns,completed_ns,latency_ns\n0,1,11,10\n1,2,22,20\n",
    );
    assert.doesNotThrow(() => validateLatencyCsv(path, 2));
    writeFileSync(
      path,
      "sequence,admitted_ns,completed_ns,latency_ns\n0,1,11,10\n0,2,22,20\n",
    );
    assert.throws(() => validateLatencyCsv(path, 2));
    writeFileSync(
      path,
      "sequence,admitted_ns,completed_ns,latency_ns\n0,1,11,9\n1,2,22,20\n",
    );
    assert.throws(() => validateLatencyCsv(path, 2));
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("resolved workload rendering preserves runtime overrides and exclusions", () => {
  const rendered = renderResolvedWorkload({
    records: 2_000,
    warmup_records: 200,
    payload_bytes: 1_024,
    partitions: 12,
    replication_factor: 3,
    min_in_sync_replicas: 2,
    max_outstanding_records: 1_024,
    linger_ms: 5,
    batch_records: 256,
    batch_bytes: 65_536,
    request_bytes: 1_048_576,
    max_in_flight_requests_per_broker: 5,
    retry_max_replacements: 600,
    retry_backoff_ms: 100,
    warmup_serialized_partition_primer_records: 12,
    native_request_concurrency: {
      status: "matched-configured",
      kafkars: "five",
      librdkafka_c: "five",
    },
  });
  assert.match(rendered, /^records = 2000$/m);
  assert.match(rendered, /^max_outstanding_records = 1024$/m);
  assert.match(rendered, /^status = "matched-configured"$/m);
});
