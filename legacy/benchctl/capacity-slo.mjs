// Predeclared sustainable-capacity service-level objectives and evaluation.

export const CANONICAL_CAPACITY_SLO = Object.freeze({
  corrected_p99_ns: 250_000_000,
  schedule_delay_p99_ns: 50_000_000,
  drain_tail_ns: 1_000_000_000,
  queue_slope_floor_records_per_second: 1_000,
  queue_slope_fraction_of_offered_rate: 0.01,
  minimum_queue_samples: 3,
});
export const CANONICAL_CAPACITY_SEARCH = Object.freeze({
  initialRate: 25_000,
  minRate: 1_000,
  maxRate: 1_000_000,
  growthFactor: 2,
  resolutionFraction: 0.05,
});
export const CANONICAL_CAPACITY_WINDOW = Object.freeze({
  windowSeconds: 10,
  warmupSeconds: 2,
});

export function evaluateCapacitySlo(result, native, maxOutstanding, slo) {
  validateInputs(result, native, maxOutstanding, slo);
  const scheduleSpan = result.load.schedule_span_ns;
  const duration = result.load.drain_duration_ns;
  const drainTail = Math.max(0, duration - scheduleSpan);
  const queue = native.sampled_producer_queue;
  const slopeBudget = Math.max(
    slo.queue_slope_floor_records_per_second,
    result.load.offered_records_per_second *
      slo.queue_slope_fraction_of_offered_rate,
  );
  const checks = {
    all_records_acknowledged:
      result.acknowledged_records === result.offered_records &&
      result.failed_records === 0,
    no_admission_pressure: result.load.admission_pressure_records === 0,
    no_native_retries: native.requests.retries === 0,
    no_native_timeouts: native.requests.timeouts === 0,
    corrected_p99_within_budget:
      result.latency_ns.corrected.p99 <= slo.corrected_p99_ns,
    schedule_delay_p99_within_budget:
      result.latency_ns.schedule_delay.p99 <= slo.schedule_delay_p99_ns,
    drain_tail_within_budget: drainTail <= slo.drain_tail_ns,
    native_queue_within_budget: queue.peak_records <= maxOutstanding,
    enough_queue_samples: queue.samples >= slo.minimum_queue_samples,
    native_queue_slope_within_budget:
      queue.linear_slope_records_per_second !== null &&
      queue.linear_slope_records_per_second <= slopeBudget,
  };
  return {
    sustainable: Object.values(checks).every(Boolean),
    checks,
    observed: {
      corrected_p99_ns: result.latency_ns.corrected.p99,
      schedule_delay_p99_ns: result.latency_ns.schedule_delay.p99,
      drain_tail_ns: drainTail,
      native_retries: native.requests.retries,
      native_timeouts: native.requests.timeouts,
      native_queue_samples: queue.samples,
      native_queue_peak_records: queue.peak_records,
      native_queue_slope_records_per_second:
        queue.linear_slope_records_per_second,
    },
    budgets: {
      ...slo,
      max_outstanding_records: maxOutstanding,
      queue_slope_records_per_second: slopeBudget,
    },
  };
}

export function isCanonicalCapacitySlo(slo) {
  return JSON.stringify(slo) === JSON.stringify(CANONICAL_CAPACITY_SLO);
}

export function assessCapacityEvidenceContract({
  canonicalSlo,
  search,
  window,
  bracketWidthFraction,
  statisticalCredibility,
}) {
  const canonicalSearch =
    JSON.stringify(search) === JSON.stringify(CANONICAL_CAPACITY_SEARCH);
  const canonicalWindow =
    JSON.stringify(window) === JSON.stringify(CANONICAL_CAPACITY_WINDOW);
  const bracketInsideResolution =
    typeof bracketWidthFraction === "number" &&
    Number.isFinite(bracketWidthFraction) &&
    bracketWidthFraction >= 0 &&
    bracketWidthFraction <= CANONICAL_CAPACITY_SEARCH.resolutionFraction;
  return {
    canonical_slo: canonicalSlo,
    canonical_search: canonicalSearch,
    canonical_window: canonicalWindow,
    bracket_inside_resolution: bracketInsideResolution,
    statistical_credibility: statisticalCredibility,
    valid:
      canonicalSlo &&
      canonicalSearch &&
      canonicalWindow &&
      bracketInsideResolution &&
      statisticalCredibility,
  };
}

function validateInputs(result, native, maxOutstanding, slo) {
  if (
    result?.schema !== "kafkars.producer-fixed-load.v1" ||
    native?.schema !== "kafkars.librdkafka-native-metrics.v1" ||
    !Number.isSafeInteger(maxOutstanding) ||
    maxOutstanding <= 0 ||
    !Object.values(slo).every(
      (value) => typeof value === "number" && Number.isFinite(value) && value > 0,
    )
  ) {
    throw new Error("capacity SLO input violates the sealed contract");
  }
}
