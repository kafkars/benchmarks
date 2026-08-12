//! Per-subject medians, and the two dispersion tables beside them.
//!
//! # Which dispersion the budget is about
//!
//! [`COEFFICIENT_OF_VARIATION_BUDGET`](crate::COEFFICIENT_OF_VARIATION_BUDGET)
//! is calibrated for the dispersion of the
//! **paired ratio series** — one ratio per attempt, the same numbers the
//! bootstrap resamples — because that is what `statistics.mjs` measured it on:
//! the legacy control plane fed `summarizeRatios` the per-pair
//! `head/base` ratios and asked `assessStatisticalCredibility` about *those*.
//!
//! The distinction is not academic, and it cuts both ways.
//!
//! - A machine that drifts — a thermal ramp, a noisy neighbour — moves both
//!   subjects together. Every subject's raw values are then far outside the
//!   budget while every ratio is rock steady, and pairing is precisely the
//!   technique that makes such a run usable. Gating on raw values throws it away.
//! - Two subjects that move in *opposite* directions produce raw values that
//!   each look quiet and a ratio that swings twice as far as either. Gating on
//!   raw values passes the run whose comparison is least trustworthy.
//!
//! The per-subject raw coefficients are still reported, and are still worth
//! reading — they are how a reader tells "the machine was noisy" from "the
//! subjects disagreed". They are informational rows in the dispersion table,
//! named after the subject; the gated rows are named `numerator/denominator`.

use bench_schema::{SubjectDispersion, SubjectMedians, SuiteAttempt, SuiteSubjectObservation};

use crate::stats::median;
use crate::summary::summarize_positive_values;

use super::comparison::{Comparison, SubjectIdentity, ratio_label};
use super::metric::SuiteMetric;

/// Per-subject medians over the valid attempts.
pub(super) fn subject_medians(
    subjects: &[SubjectIdentity],
    valid: &[&SuiteAttempt],
) -> Vec<SubjectMedians> {
    subjects
        .iter()
        .filter_map(|subject| {
            let observations: Vec<&SuiteSubjectObservation> = valid
                .iter()
                .filter_map(|attempt| {
                    attempt
                        .subjects
                        .iter()
                        .find(|entry| entry.name == subject.name)
                })
                .collect();
            if observations.is_empty() {
                return None;
            }
            Some(SubjectMedians {
                name: subject.name.clone(),
                role: subject.role.clone(),
                declared: agreed_declaration(&observations),
                acknowledged_records_per_second: median_of(&observations, SuiteMetric::Goodput)
                    .unwrap_or(0.0),
                p50_intended_to_terminal_ns: median_ns(&observations, SuiteMetric::P50Latency),
                p99_intended_to_terminal_ns: median_ns(&observations, SuiteMetric::P99Latency),
                p999_intended_to_terminal_ns: median_ns(&observations, SuiteMetric::P999Latency),
                p99_admission_wait_ns: median_ns(&observations, SuiteMetric::P99AdmissionWait),
                p99_intended_to_call_start_ns: complete_median(
                    &observations,
                    SuiteMetric::SchedulerLateness,
                )
                .map(round_to_u64),
                p99_accepted_to_terminal_ns: median_ns(
                    &observations,
                    SuiteMetric::AcceptedToTerminal,
                ),
                max_rss_bytes: complete_median(&observations, SuiteMetric::MaxRssBytes)
                    .map(round_to_u64),
                cpu_core_seconds: complete_median(&observations, SuiteMetric::CpuCoreSeconds),
                cpu_core_seconds_per_million_acknowledged: complete_cpu_per_million(&observations),
            })
        })
        .collect()
}

/// The declaration every observation agreed on, or `None` when they did not.
///
/// Disagreement is not averaged into a plausible-looking answer: a subject
/// whose declared execution surface changed between repetitions has no single
/// declaration, and the `matched-execution-surface` gate reads the per-attempt
/// values rather than this one for exactly that reason.
fn agreed_declaration(
    observations: &[&SuiteSubjectObservation],
) -> Option<bench_schema::DeclaredExecution> {
    let first = observations.first()?.declared.clone()?;
    observations
        .iter()
        .all(|observation| observation.declared.as_ref() == Some(&first))
        .then_some(first)
}

/// The median CPU cost per million acknowledged records, when every observation
/// reported one.
fn complete_cpu_per_million(observations: &[&SuiteSubjectObservation]) -> Option<f64> {
    let values: Vec<f64> = observations
        .iter()
        .filter_map(|observation| observation.cpu_core_seconds_per_million_acknowledged)
        .collect();
    if values.len() != observations.len() {
        return None;
    }
    median(&values)
}

/// The median of one metric over one subject's observations.
fn median_of(observations: &[&SuiteSubjectObservation], metric: SuiteMetric) -> Option<f64> {
    let values: Vec<f64> = observations
        .iter()
        .filter_map(|observation| metric.observed(observation))
        .collect();
    median(&values)
}

/// The median of one metric, present only when every observation reported it.
///
/// Absence is contagious on purpose: a median of the three attempts that
/// happened to report memory is not the run's memory.
fn complete_median(observations: &[&SuiteSubjectObservation], metric: SuiteMetric) -> Option<f64> {
    let values: Vec<f64> = observations
        .iter()
        .filter_map(|observation| metric.observed(observation))
        .collect();
    if values.len() != observations.len() {
        return None;
    }
    median(&values)
}

/// The median of one nanosecond metric, rounded back to whole nanoseconds.
fn median_ns(observations: &[&SuiteSubjectObservation], metric: SuiteMetric) -> u64 {
    median_of(observations, metric).map_or(0, round_to_u64)
}

/// Rounds a non-negative reporting statistic back to an integer.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "the value is a rounded non-negative median of counts this harness can record"
)]
fn round_to_u64(value: f64) -> u64 {
    if value.is_finite() && value > 0.0 {
        value.round() as u64
    } else {
        0
    }
}

/// One comparison's per-attempt ratios for one metric, in attempt order.
///
/// The same series [`paired_ratios`](super::pairs::paired_ratios) hands the
/// bootstrap: an attempt where either subject is missing contributes nothing,
/// because a ratio needs both halves measured under the same conditions. A
/// denominator of zero produces a non-finite ratio, which is deliberately
/// *kept* rather than filtered — it makes the series unsummarizable, and an
/// unsummarizable series fails the gate instead of quietly shortening it.
fn ratio_series(comparison: &Comparison, metric: SuiteMetric, valid: &[&SuiteAttempt]) -> Vec<f64> {
    valid
        .iter()
        .filter_map(|attempt| {
            let observed = |name: &str| {
                attempt
                    .subjects
                    .iter()
                    .find(|entry| entry.name == name)
                    .and_then(|entry| metric.observed(entry))
            };
            match (
                observed(&comparison.numerator),
                observed(&comparison.denominator),
            ) {
                (Some(numerator), Some(denominator)) => Some(numerator / denominator),
                _ => None,
            }
        })
        .collect()
}

/// Per-pair, per-metric dispersion of the ratio series across valid attempts.
///
/// These are the rows the budget gate reads. See the module contract for why
/// the budget is about ratios rather than about either subject's raw values.
pub(super) fn pair_dispersion(
    comparisons: &[Comparison],
    valid: &[&SuiteAttempt],
) -> Vec<SubjectDispersion> {
    let mut dispersion = Vec::new();
    for comparison in comparisons {
        for metric in SuiteMetric::ALL {
            let ratios = ratio_series(comparison, metric, valid);
            if ratios.is_empty() {
                continue;
            }
            dispersion.push(SubjectDispersion {
                name: ratio_label(comparison),
                metric: metric.field().to_owned(),
                coefficient_of_variation: summarize_positive_values(&ratios)
                    .ok()
                    .and_then(|summary| summary.coefficient_of_variation),
            });
        }
    }
    dispersion
}

/// Per-subject, per-metric dispersion across the valid attempts.
///
/// Informational: these rows say whether the machine was steady, which is a
/// different question from whether the comparison was. Nothing gates on them.
pub(super) fn subject_dispersion(
    subjects: &[SubjectIdentity],
    valid: &[&SuiteAttempt],
) -> Vec<SubjectDispersion> {
    let mut dispersion = Vec::new();
    for subject in subjects {
        for metric in SuiteMetric::ALL {
            let values: Vec<f64> = valid
                .iter()
                .filter_map(|attempt| {
                    attempt
                        .subjects
                        .iter()
                        .find(|entry| entry.name == subject.name)
                })
                .filter_map(|observation| metric.observed(observation))
                .collect();
            if values.is_empty() {
                continue;
            }
            dispersion.push(SubjectDispersion {
                name: subject.name.clone(),
                metric: metric.field().to_owned(),
                coefficient_of_variation: summarize_positive_values(&values)
                    .ok()
                    .and_then(|summary| summary.coefficient_of_variation),
            });
        }
    }
    dispersion
}
