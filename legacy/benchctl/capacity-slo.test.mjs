// Contract tests for sustainable-capacity service-level objectives.

import assert from "node:assert/strict";
import test from "node:test";

import {
  CANONICAL_CAPACITY_SLO,
  CANONICAL_CAPACITY_SEARCH,
  CANONICAL_CAPACITY_WINDOW,
  assessCapacityEvidenceContract,
  evaluateCapacitySlo,
  isCanonicalCapacitySlo,
} from "./capacity-slo.mjs";

test("capacity SLO accepts a correct drained stable window", () => {
  const assessment = evaluateCapacitySlo(
    result(),
    native(),
    8_192,
    CANONICAL_CAPACITY_SLO,
  );
  assert.equal(assessment.sustainable, true);
  assert.equal(assessment.observed.drain_tail_ns, 100_000_000);
  assert.equal(assessment.budgets.queue_slope_records_per_second, 1_000);
  assert.equal(isCanonicalCapacitySlo(CANONICAL_CAPACITY_SLO), true);
});

test("capacity evidence requires the complete predeclared search", () => {
  const valid = assessCapacityEvidenceContract({
    canonicalSlo: true,
    search: CANONICAL_CAPACITY_SEARCH,
    window: CANONICAL_CAPACITY_WINDOW,
    bracketWidthFraction: 0.04,
    statisticalCredibility: true,
  });
  assert.equal(valid.valid, true);
  const coarse = assessCapacityEvidenceContract({
    canonicalSlo: true,
    search: { ...CANONICAL_CAPACITY_SEARCH, resolutionFraction: 0.25 },
    window: CANONICAL_CAPACITY_WINDOW,
    bracketWidthFraction: 0.2,
    statisticalCredibility: true,
  });
  assert.equal(coarse.valid, false);
  assert.equal(coarse.canonical_search, false);
  assert.equal(coarse.bracket_inside_resolution, false);
});

test("capacity SLO rejects growing queues and retries", () => {
  const evidence = native();
  evidence.requests.retries = 1;
  evidence.sampled_producer_queue.linear_slope_records_per_second = 1_001;
  const assessment = evaluateCapacitySlo(
    result(),
    evidence,
    8_192,
    CANONICAL_CAPACITY_SLO,
  );
  assert.equal(assessment.sustainable, false);
  assert.equal(assessment.checks.no_native_retries, false);
  assert.equal(assessment.checks.native_queue_slope_within_budget, false);
});

function result() {
  return {
    schema: "kafkars.producer-fixed-load.v1",
    offered_records: 100_000,
    acknowledged_records: 100_000,
    failed_records: 0,
    load: {
      offered_records_per_second: 100_000,
      schedule_span_ns: 999_990_000,
      drain_duration_ns: 1_099_990_000,
      admission_pressure_records: 0,
    },
    latency_ns: {
      corrected: { p99: 10_000_000 },
      schedule_delay: { p99: 1_000_000 },
    },
  };
}

function native() {
  return {
    schema: "kafkars.librdkafka-native-metrics.v1",
    requests: { retries: 0, timeouts: 0 },
    sampled_producer_queue: {
      samples: 10,
      peak_records: 1_024,
      linear_slope_records_per_second: 10,
    },
  };
}
