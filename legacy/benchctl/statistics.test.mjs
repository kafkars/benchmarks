// Exact aggregate behavior for paired benchmark statistics.

import assert from "node:assert/strict";
import test from "node:test";

import {
  assessFixedLoadCredibility,
  assessResourceCredibility,
  assessStatisticalCredibility,
  median,
  summarizePositiveValues,
  summarizeRatios,
} from "./statistics.mjs";

test("median is deterministic for odd and even sample counts", () => {
  assert.equal(median([3, 1, 2]), 2);
  assert.equal(median([4, 1, 3, 2]), 2.5);
  assert.throws(() => median([]));
});

test("paired ratio summary retains range, median, and geometric mean", () => {
  const summary = summarizeRatios([0.5, 1, 2]);
  assert.equal(summary.repetitions, 3);
  assert.equal(summary.minimum, 0.5);
  assert.equal(summary.median, 1);
  assert.equal(summary.maximum, 2);
  assert.ok(Math.abs(summary.geometric_mean - 1) < Number.EPSILON);
  assert.ok(summary.coefficient_of_variation > 0);
  assert.equal(
    summary.confidence_interval_95.method,
    "paired-block-percentile-bootstrap",
  );
  assert.equal(summary.confidence_interval_95.resamples, 50_000);
  assert.ok(summary.confidence_interval_95.lower <= summary.geometric_mean);
  assert.ok(summary.confidence_interval_95.upper >= summary.geometric_mean);
  assert.throws(() => summarizeRatios([1, 0]));
});

test("absolute repetitions use a non-paired bootstrap label", () => {
  const summary = summarizePositiveValues([10, 11, 12]);
  assert.equal(
    summary.confidence_interval_95.method,
    "repetition-percentile-bootstrap",
  );
});

test("single paired ratio exposes its insufficient variation evidence", () => {
  const summary = summarizeRatios([1.2]);
  assert.equal(summary.coefficient_of_variation, null);
  assert.equal(summary.confidence_interval_95.lower, 1.2);
  assert.equal(summary.confidence_interval_95.upper, 1.2);
});

test("statistical credibility requires five quiet paired blocks", () => {
  const stable = summarizeRatios([1.1, 1.11, 1.09, 1.1, 1.1]);
  const sparse = summarizeRatios([1.1]);
  assert.equal(assessStatisticalCredibility(stable, stable).valid, true);
  assert.equal(assessStatisticalCredibility(sparse, stable).valid, false);
  const noisy = summarizeRatios([0.8, 1.4, 0.9, 1.3, 1.0]);
  assert.equal(assessStatisticalCredibility(stable, noisy).valid, false);
  assert.equal(assessFixedLoadCredibility(stable, stable).valid, true);
  assert.equal(assessFixedLoadCredibility(stable, sparse).valid, false);
  assert.equal(assessFixedLoadCredibility(stable, noisy).valid, false);
  assert.equal(assessResourceCredibility(stable, stable).valid, true);
  assert.equal(assessResourceCredibility(stable, noisy).valid, false);
});
