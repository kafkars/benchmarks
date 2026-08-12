// Contract tests for four-point fixed-load matrix aggregation.

import assert from "node:assert/strict";
import test from "node:test";

import { assessFixedMatrix } from "./fixed-matrix.mjs";

test("fixed matrix requires credible leadership at every derived point", () => {
  const result = assessFixedMatrix(
    [25, 50, 75, 90].map((percentage) => point(percentage, 0.8, 0.9, 3)),
    true,
  );
  assert.equal(result.evidence_credible, true);
  assert.equal(result.gates.corrected_p99_parity_within_ten_percent, true);
  assert.equal(result.gates.corrected_p99_leadership_at_every_point, true);
  assert.equal(result.gates.cpu_leadership_at_every_point, true);
  assert.ok(
    result.across_point_geometric_means
      .produce_requests_librdkafka_over_kafkars.geometric_mean > 2.9,
  );
});

test("fixed matrix fails closed on noisy or non-leading points", () => {
  const points = [25, 50, 75, 90].map((percentage) =>
    point(percentage, percentage === 75 ? 1.05 : 0.8, 0.9, 3),
  );
  points[0].statistical_credibility = false;
  const result = assessFixedMatrix(points, true);
  assert.equal(result.evidence_credible, false);
  assert.equal(result.gates.corrected_p99_parity_within_ten_percent, false);
  assert.equal(result.gates.corrected_p99_leadership_at_every_point, false);
});

function point(percentage, corrected, uncorrected, requests) {
  return {
    percentage,
    statistical_credibility: true,
    corrected_p99: summary(corrected),
    uncorrected_p99: summary(uncorrected),
    request_efficiency: summary(requests),
    resource_credibility: true,
    cpu_core_seconds: summary(0.7),
    peak_memory: summary(0.75),
    peak_rss: summary(0.8),
  };
}

function summary(value) {
  return {
    geometric_mean: value,
    confidence_interval_95: { lower: value * 0.99, upper: value * 1.01 },
  };
}
