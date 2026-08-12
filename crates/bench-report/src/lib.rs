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
//! `statistics.mjs`, which still owns the geometric mean, median, extrema, and
//! the paired bootstrap confidence interval. This crate currently mirrors only
//! the repetition count, arithmetic mean, and coefficient of variation; the
//! bootstrap is deliberately deferred rather than reimplemented twice, and the
//! two implementations must not disagree about the parts that overlap.
#![forbid(unsafe_code)]

mod summary;

pub use summary::{
    COEFFICIENT_OF_VARIATION_BUDGET, MINIMUM_PAIRED_REPETITIONS, PositiveValueSummary,
    SummaryError, summarize_positive_values,
};

#[cfg(test)]
mod summary_test;
