//! Reporting over sealed benchmark evidence: turning repeated measurements into
//! a summary that states its own credibility.
//!
//! Reporting is a separate concern from measuring on purpose. Adapters produce
//! numbers, the control plane seals them, and only afterwards does anything in
//! this crate read them. Nothing here may reach a broker, spawn a process, or
//! open a socket, so a reporting bug can never move a measurement.
//!
//! The house rule this crate exists to enforce is that a single number is not a
//! result. A summary carries its repetition count and its dispersion beside its
//! central tendency, and a caller that wants to make a claim has to consult
//! both against [`MINIMUM_PAIRED_REPETITIONS`] and
//! [`COEFFICIENT_OF_VARIATION_BUDGET`]. Statistical credibility is an axis of
//! its own: a run can complete perfectly and still be too noisy to mean
//! anything.
//!
//! The behavioral reference for this loop is the legacy Node control plane's
//! `statistics.mjs`, which owns the geometric mean, median, extrema, and the
//! paired bootstrap confidence interval. Everything here that overlaps it is a
//! port rather than a reinvention, and the two implementations must not
//! disagree about the parts that overlap.
//!
//! # Layout
//!
//! - `error` — the single failure type, a kind plus context.
//! - `rng` — the seeded generator every resampling result is reproducible from.
//! - `stats` — median and geometric mean.
//! - `summary` — repetition count, mean, and dispersion, plus the two budgets.
//! - `bootstrap` — paired-block percentile confidence intervals.
//! - `economics` — request economics from a client's native statistics stream.
//! - `slo` — one measurement against one declared objective.
//! - `suite` — medians, pairs, intervals, and gates across repetitions.
//! - `render` — Markdown and self-contained HTML over a decided summary.
//! - `packet` — the deterministic analysis packet, and the one check a
//!   model-written summary has to pass.
#![forbid(unsafe_code)]

mod bootstrap;
mod economics;
mod error;
mod packet;
mod render;
mod rng;
mod slo;
mod stats;
mod suite;
mod summary;

pub use bootstrap::{
    BOOTSTRAP_METHOD, BootstrapOptions, DEFAULT_BOOTSTRAP_RESAMPLES, LOWER_QUANTILE,
    PairedObservation, RatioInterval, UPPER_QUANTILE, paired_ratio_interval, paired_ratios,
};
pub use economics::{
    BatchWindow, RequestEconomics, STATISTICS_FILE_NAME, read_request_economics,
    request_economics_from_snapshots,
};
pub use error::{ReportError, ReportErrorKind, ReportResult};
pub use packet::{build_packet, validate_llm_summary};
pub use render::{
    DIAGNOSTIC_BANNER, NOT_REPORTED, render_html_suite, render_markdown_bundle,
    render_markdown_suite,
};
pub use rng::{Rng, split_mix64};
pub use slo::{MAX_FAILED_RECORDS, SloVerdict, evaluate_slo};
pub use stats::{geometric_mean, histogram_percentile, median};
pub use suite::{
    DEFAULT_PRACTICAL_THRESHOLD, SubjectEconomics, SuiteMetric, SuiteOptions, SuiteReport,
    UNSEALED_DIGEST, metric_of_field, pair_passes, pair_regresses, summarize_suite,
    summarize_suite_report,
};
pub use summary::{
    COEFFICIENT_OF_VARIATION_BUDGET, MINIMUM_PAIRED_REPETITIONS, PositiveValueSummary,
    SummaryError, summarize_positive_values,
};

#[cfg(test)]
mod bootstrap_test;
#[cfg(test)]
mod economics_test;
#[cfg(test)]
mod fixture;
#[cfg(test)]
mod packet_test;
#[cfg(test)]
mod render_test;
#[cfg(test)]
mod rng_test;
#[cfg(test)]
mod slo_test;
#[cfg(test)]
mod stats_test;
#[cfg(test)]
mod summary_test;
