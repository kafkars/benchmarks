// Aggregate credibility and latency gates for the four derived load points.

import { FIXED_LOAD_PERCENTAGES } from "./capacity-search.mjs";

export function assessFixedMatrix(points, referenceCredible) {
  if (
    !Array.isArray(points) ||
    points.length !== FIXED_LOAD_PERCENTAGES.length ||
    points.some(
      (point, index) =>
        point.percentage !== FIXED_LOAD_PERCENTAGES[index] ||
        !positiveSummary(point.corrected_p99) ||
        !positiveSummary(point.uncorrected_p99) ||
        !positiveSummary(point.request_efficiency) ||
        !positiveSummary(point.cpu_core_seconds) ||
        !positiveSummary(point.peak_memory) ||
        !positiveSummary(point.peak_rss),
    )
  ) {
    throw new Error("fixed-load matrix points violate the sealed contract");
  }
  const everyPointCredible = points.every(
    (point) => point.statistical_credibility === true,
  );
  const everyResourcePointCredible = points.every(
    (point) => point.resource_credibility === true,
  );
  const evidenceCredible =
    referenceCredible && everyPointCredible && everyResourcePointCredible;
  const corrected = summarizeAcrossPoints(points, "corrected_p99");
  const uncorrected = summarizeAcrossPoints(points, "uncorrected_p99");
  const requestEfficiency = summarizeAcrossPoints(points, "request_efficiency");
  const cpu = summarizeAcrossPoints(points, "cpu_core_seconds");
  const peakMemory = summarizeAcrossPoints(points, "peak_memory");
  const peakRss = summarizeAcrossPoints(points, "peak_rss");
  return {
    evidence_credible: evidenceCredible,
    reference_capacity_credible: referenceCredible,
    every_fixed_point_credible: everyPointCredible,
    every_resource_point_credible: everyResourcePointCredible,
    across_point_geometric_means: {
      corrected_p99_kafkars_over_librdkafka: corrected,
      uncorrected_p99_kafkars_over_librdkafka: uncorrected,
      produce_requests_librdkafka_over_kafkars: requestEfficiency,
      cpu_core_seconds_kafkars_over_librdkafka: cpu,
      peak_memory_kafkars_over_librdkafka: peakMemory,
      peak_rss_kafkars_over_librdkafka: peakRss,
    },
    gates: {
      corrected_p99_parity_within_ten_percent:
        evidenceCredible &&
        points.every(
          (point) =>
            point.corrected_p99.geometric_mean <= 1.1 &&
            point.corrected_p99.confidence_interval_95.upper <= 1.1,
        ),
      corrected_p99_leadership_at_every_point:
        evidenceCredible &&
        points.every(
          (point) =>
            point.corrected_p99.geometric_mean < 1 &&
            point.corrected_p99.confidence_interval_95.upper < 1,
        ),
      cpu_parity_within_ten_percent:
        evidenceCredible &&
        points.every(
          (point) =>
            point.cpu_core_seconds.geometric_mean <= 1.1 &&
            point.cpu_core_seconds.confidence_interval_95.upper <= 1.1,
        ),
      cpu_leadership_at_every_point:
        evidenceCredible &&
        points.every(
          (point) =>
            point.cpu_core_seconds.geometric_mean < 1 &&
            point.cpu_core_seconds.confidence_interval_95.upper < 1,
        ),
    },
  };
}

function summarizeAcrossPoints(points, key) {
  const values = points.map((point) => point[key].geometric_mean);
  return {
    points: values.length,
    geometric_mean: Math.exp(
      values.reduce((total, value) => total + Math.log(value), 0) /
        values.length,
    ),
    minimum: Math.min(...values),
    maximum: Math.max(...values),
  };
}

function positiveSummary(value) {
  return (
    typeof value?.geometric_mean === "number" &&
    Number.isFinite(value.geometric_mean) &&
    value.geometric_mean > 0 &&
    typeof value.confidence_interval_95?.upper === "number" &&
    Number.isFinite(value.confidence_interval_95.upper) &&
    value.confidence_interval_95.upper > 0
  );
}
