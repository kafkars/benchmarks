//! The statistics this crate derives from evidence that
//! [`summarize_positive_values`](crate::summarize_positive_values) does not
//! already provide: two central tendencies, and the percentile read.
//!
//! The central tendencies are absent rather than zero for an empty sample, and
//! both refuse a value that is not finite and strictly positive, for the same
//! reason `summary` does: these samples are throughputs, latencies, and ratios,
//! and a zero in that position means the measurement failed rather than
//! measured zero.
//!
//! The geometric mean is the one that matters for comparisons. Averaging
//! ratios arithmetically makes a two-times win and a two-times loss average to
//! 1.25 instead of 1.0, which flatters whichever subject happens to be in the
//! numerator; the geometric mean is symmetric under inverting the comparison,
//! so `summarize(a/b)` and `summarize(b/a)` agree about which subject won.
//!
//! Percentiles are *read*, never stored. A `kafkars.producer-benchmark.v2`
//! document carries distributions, not percentiles, so every p50, p99, and
//! p99.9 in this crate is derived here from the same bytes a sceptic would
//! derive it from, and inherits the histogram's conservative rule of reporting
//! a bucket's inclusive upper bound.

use crate::error::ReportResult;

/// Reads one percentile out of an encoded histogram, in nanoseconds.
///
/// Returns `None` when the histogram recorded nothing, which is a real outcome
/// — an attempt where no offer reached a terminal has no latency to report, and
/// zero would be a lie about a very fast run.
///
/// # Errors
///
/// Returns a [`ReportErrorKind::Document`](crate::ReportErrorKind::Document)
/// error when the histogram does not satisfy its own layout invariants.
pub fn histogram_percentile(
    encoded: &bench_schema::EncodedHistogram,
    quantile: f64,
) -> ReportResult<Option<u64>> {
    let histogram = bench_schema::Histogram::decode(encoded)?;
    Ok(histogram.value_at_quantile(quantile))
}

/// Returns the median of a sample, or `None` when it is empty.
///
/// An even-sized sample averages the two middle values, matching `median` in
/// the legacy control plane's `statistics.mjs`.
#[must_use]
#[expect(
    clippy::indexing_slicing,
    reason = "both indexes are derived from a length that was checked to be non-zero"
)]
pub fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted: Vec<f64> = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        Some(sorted[middle])
    } else {
        Some(f64::midpoint(sorted[middle - 1], sorted[middle]))
    }
}

/// Returns the geometric mean of a sample of finite, strictly positive values,
/// or `None` when the sample is empty or contains a value that is not.
#[must_use]
#[expect(
    clippy::cast_precision_loss,
    reason = "sample sizes are small integers that f64 represents exactly"
)]
pub fn geometric_mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() || !values.iter().all(|value| value.is_finite() && *value > 0.0) {
        return None;
    }
    let total: f64 = values.iter().map(|value| value.ln()).sum();
    Some((total / values.len() as f64).exp())
}
