// Contract tests for deterministic sustainable-capacity search.

import assert from "node:assert/strict";
import test from "node:test";

import {
  deriveFixedRates,
  searchSustainableCapacity,
} from "./capacity-search.mjs";

const configuration = {
  initialRate: 10_000,
  minRate: 1_000,
  maxRate: 100_000,
  growthFactor: 2,
  resolutionFraction: 0.05,
};

test("capacity search expands and refines a passing lower bracket", () => {
  const result = searchSustainableCapacity(
    configuration,
    (rate) => rate <= 31_000,
  );
  assert.equal(result.sustainable_records_per_second, 30_000);
  assert.equal(result.first_failing_records_per_second, 31_250);
  assert.ok(result.bracket_width_fraction <= 0.05);
  assert.deepEqual(result.derived_fixed_rates, {
    25: 7_500,
    50: 15_000,
    75: 22_500,
    90: 27_000,
  });
});

test("capacity search contracts when its initial rate fails", () => {
  const result = searchSustainableCapacity(
    configuration,
    (rate) => rate <= 6_000,
  );
  assert.equal(result.sustainable_records_per_second, 5_937);
  assert.equal(result.first_failing_records_per_second, 6_093);
});

test("capacity search fails without both sides of a bracket", () => {
  assert.throws(() =>
    searchSustainableCapacity(configuration, () => true),
  );
  assert.throws(() =>
    searchSustainableCapacity(configuration, () => false),
  );
  assert.throws(() => deriveFixedRates(0));
});
