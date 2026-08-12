//! The formatting vocabulary both renderings share.
//!
//! Every number is formatted with a fixed number of decimals, so the same
//! summary always renders the same bytes and two reports can be diffed. There
//! is no locale, no thousands separator, and no "about". Absent measurements
//! print as [`NOT_REPORTED`](super::NOT_REPORTED), never as `0`.
//!
//! Markdown and HTML disagree about markup and agree about digits, so the digit
//! rules live here and are called from both. A rounding change made in one
//! renderer and not the other would show up as a diff between the two goldens
//! that nobody asked for.

use bench_schema::{PairedRatio, SubjectDispersion, SuiteSummary};

use crate::suite::{SuiteMetric, metric_of_field, pair_passes, pair_regresses};

use super::NOT_REPORTED;

/// The scorecard's header row, shared by the suite and bundle views.
pub(super) const SCORECARD_HEADER: &str = "| Subject | Role | Goodput (records/s) | p50 (ms) | p99 (ms) | \
     p99.9 (ms) | Admission p99 (ms) | CPU (core-s) | Peak RSS (MiB) |";

/// The scorecard's alignment row: every numeric column is right-aligned.
pub(super) const SCORECARD_ALIGNMENT: &str =
    "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |";

/// Whether a dispersion row is judged or merely reported.
///
/// Read off the name rather than carried in the document: a series named
/// `numerator/denominator` is a ratio, and a subject name cannot contain a
/// slash. That keeps `kafkars.suite-summary.v1` unchanged while still letting a
/// reader see which rows the gate acted on.
///
/// The distinction decides what a reader should conclude. A row named
/// `head/base` is the per-attempt ratio series, which is what the budget gate
/// judges; a row named after a single subject is that subject's own values,
/// which nothing gates on. Both are shown because together they separate "the
/// machine drifted" from "the subjects disagreed", and a table that showed only
/// the gated rows would leave a reader unable to tell those apart.
pub(super) fn dispersion_role(entry: &SubjectDispersion) -> &'static str {
    if entry.name.contains('/') && is_gated_metric(&entry.metric) {
        "yes"
    } else {
        "no"
    }
}

/// Whether the budget gate covers this metric.
fn is_gated_metric(field: &str) -> bool {
    matches!(
        metric_of_field(field),
        Some(SuiteMetric::Goodput | SuiteMetric::P99Latency)
    )
}

/// How many attempts a summary counts as valid.
pub(super) fn valid_count(summary: &SuiteSummary) -> usize {
    summary
        .attempts
        .iter()
        .filter(|attempt| attempt.run_valid)
        .count()
}

/// The word for a boolean gate or validity outcome.
pub(super) fn pass_word(passed: bool) -> &'static str {
    if passed { "pass" } else { "fail" }
}

/// The word describing which direction is better for a metric.
pub(super) fn direction_word(metric: SuiteMetric) -> &'static str {
    if metric.higher_is_better() {
        "larger is better"
    } else {
        "smaller is better"
    }
}

/// What a pair's interval says about the threshold, in one word.
pub(super) fn verdict_word(
    pair: &PairedRatio,
    metric: SuiteMetric,
    threshold: f64,
) -> &'static str {
    if pair_passes(pair, metric, threshold) {
        "yes"
    } else if pair_regresses(pair, metric, threshold) {
        "no, worse"
    } else {
        "unresolved"
    }
}

/// Escapes the five characters that can end an HTML context.
pub(super) fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            other => out.push(other),
        }
    }
    out
}

/// Formats a ratio or a dimensionless fraction.
pub(super) fn format_ratio(value: f64) -> String {
    format!("{value:.4}")
}

/// Formats a rate.
pub(super) fn format_rate(value: f64) -> String {
    format!("{value:.1}")
}

/// Formats a duration in nanoseconds as milliseconds.
pub(super) fn format_ms(nanoseconds: u64) -> String {
    #[expect(
        clippy::cast_precision_loss,
        reason = "presentation of a duration, not identity arithmetic"
    )]
    let milliseconds = nanoseconds as f64 / 1_000_000.0;
    format!("{milliseconds:.3}")
}

/// Formats a duration in seconds.
pub(super) fn format_seconds(value: f64) -> String {
    format!("{value:.3}")
}

/// Formats a byte count as mebibytes.
pub(super) fn format_mib(bytes: u64) -> String {
    #[expect(
        clippy::cast_precision_loss,
        reason = "presentation of a byte count, not identity arithmetic"
    )]
    let mebibytes = bytes as f64 / (1024.0 * 1024.0);
    format!("{mebibytes:.1}")
}

/// Converts nanoseconds to seconds for presentation.
#[expect(
    clippy::cast_precision_loss,
    reason = "presentation of a duration, not identity arithmetic"
)]
pub(super) fn nanos_to_seconds(nanoseconds: u64) -> f64 {
    nanoseconds as f64 / 1_000_000_000.0
}

/// Formats an optional duration, absent as [`NOT_REPORTED`].
pub(super) fn optional_ms(nanoseconds: Option<u64>) -> String {
    nanoseconds.map_or_else(|| NOT_REPORTED.to_owned(), format_ms)
}

/// Formats an optional count, absent as [`NOT_REPORTED`].
pub(super) fn optional_count(count: Option<u64>) -> String {
    count.map_or_else(|| NOT_REPORTED.to_owned(), |value| value.to_string())
}

/// Formats an optional ratio, absent as [`NOT_REPORTED`].
pub(super) fn optional_ratio(value: Option<f64>) -> String {
    value.map_or_else(|| NOT_REPORTED.to_owned(), format_ratio)
}

/// Formats an optional rate, absent as [`NOT_REPORTED`].
pub(super) fn optional_rate(value: Option<f64>) -> String {
    value.map_or_else(|| NOT_REPORTED.to_owned(), format_rate)
}
