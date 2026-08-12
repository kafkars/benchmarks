//! Aggregation across repetitions: the step where a set of anecdotes becomes a
//! result, or fails to.
//!
//! One sealed bundle per attempt goes in and one `kafkars.suite-summary.v1`
//! comes out. Every attempt appears in that document, including the ones no
//! statistic was allowed to be drawn from, because the set a median was taken
//! over is part of the median.
//!
//! The behavioral reference for this loop is the legacy Node control plane's
//! `statistics.mjs`. Everything here that overlaps it is a port rather than a
//! reinvention, and the two implementations must not disagree about the parts
//! that overlap.
//!
//! # Layout
//!
//! - `report` — how a suite is to be summarized, and what comes back.
//! - `metric` — the metrics compared, in their one fixed order.
//! - `attempt` — one sealed bundle, read into what this crate needs.
//! - `comparison` — who is compared with whom, and what the pair is called.
//! - `medians` — per-subject medians, and the two dispersion tables. Read this
//!   one for *which* dispersion
//!   [`COEFFICIENT_OF_VARIATION_BUDGET`](crate::COEFFICIENT_OF_VARIATION_BUDGET)
//!   is calibrated for, which is not the obvious answer.
//! - `pairs` — paired ratios and their bootstrap intervals.
//! - `gates` — what has to be true before a difference may be called one.
//! - `summarize` — the pass that assembles the document.

mod attempt;
mod comparison;
mod gates;
mod medians;
mod metric;
mod pairs;
mod report;
mod summarize;

pub use attempt::UNSEALED_DIGEST;
pub use metric::{SuiteMetric, metric_of_field};
pub use pairs::{pair_passes, pair_regresses};
pub use report::{DEFAULT_PRACTICAL_THRESHOLD, SubjectEconomics, SuiteOptions, SuiteReport};
pub use summarize::{summarize_suite, summarize_suite_report};

pub(crate) use attempt::LoadedAttempt;

#[cfg(test)]
mod attempt_test;
#[cfg(test)]
mod comparison_test;
#[cfg(test)]
mod fixture;
#[cfg(test)]
mod gates_test;
#[cfg(test)]
mod medians_test;
#[cfg(test)]
mod metric_test;
#[cfg(test)]
mod summarize_test;
