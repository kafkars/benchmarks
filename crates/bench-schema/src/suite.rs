//! `kafkars.suite-summary.v1`: what a set of repetitions of one experiment says
//! once every attempt is in.
//!
//! A single attempt is an anecdote. This document is the first place in the
//! evidence chain where a number is allowed to be called a result, and the shape
//! of it is deliberate: the per-attempt observations stay in the document
//! alongside the medians derived from them, so a reader can recompute every
//! summary statistic from the same bytes that state it. A summary a reader
//! cannot audit is a summary a reader has to trust.
//!
//! # What is summarized, and how
//!
//! - **Medians, not means.** One stalled attempt should not move the reported
//!   number, and a median of an odd repetition count is a value that actually
//!   occurred.
//! - **Ratios of medians, with an interval.** A paired ratio names its numerator
//!   and its denominator, because a ratio whose denominator is implied is
//!   decoration. `resamples` records how many bootstrap resamples produced
//!   `ci_low` and `ci_high`, and `seed` records what made that resampling
//!   reproducible.
//! - **Dispersion beside every central value.** A coefficient of variation is
//!   absent rather than zero when the underlying values could not produce one,
//!   because "we did not measure spread" and "there was no spread" are opposite
//!   claims.
//! - **Gates that name themselves.** Each [`GateOutcome`] carries the sentence it
//!   is enforcing, so a failing gate is readable without the code that ran it.
//!
//! `practical_threshold` is the difference the suite was set up to care about,
//! recorded next to the intervals so that "the interval excludes zero" is never
//! confused with "the difference matters".
//!
//! # Invariants ([`SuiteSummary::validate`])
//!
//! `claim_eligible` is false, as it is for every document this milestone can
//! produce; every declared subject role is one the vocabulary knows;
//! `ci_low <= ci_high` for every pair; and `practical_threshold` is a finite,
//! non-negative fraction. Everything else is measurement, and this crate does
//! not second-guess measurement.
//!
//! Floating-point numbers are welcome here. A suite summary is evidence, never
//! identity: it is not hashed into an experiment id, and
//! [`canonical_bytes`](crate::canonical_bytes) refuses it if anyone tries.

use serde::{Deserialize, Serialize};

use crate::error::{SchemaError, SchemaResult};
use crate::experiment::{SUBJECT_ROLES, is_subject_role};
use crate::identity::ExperimentId;
use crate::schema_id::{SUITE_SUMMARY_V1, require_schema};
use crate::status::ExecutionStatus;

/// One subject's numbers as one attempt reported them.
///
/// Every latency is the value a reader derives from the attempt's histograms,
/// in nanoseconds, and every one of them includes admission wait — these are
/// `intended_to_terminal` percentiles, so a subject cannot look faster by
/// spending longer refusing to accept work.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuiteSubjectObservation {
    /// Subject name, matching the resolved experiment.
    pub name: String,
    /// The subject's role in this comparison, when it declared one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// Acknowledged records per second over the measured interval.
    pub acknowledged_records_per_second: f64,
    /// Median offer-to-terminal latency.
    pub p50_intended_to_terminal_ns: u64,
    /// 99th percentile offer-to-terminal latency.
    pub p99_intended_to_terminal_ns: u64,
    /// 99.9th percentile offer-to-terminal latency.
    pub p999_intended_to_terminal_ns: u64,
    /// 99th percentile of the admission wait alone.
    pub p99_admission_wait_ns: u64,
    /// Peak resident set size, when the platform reported one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rss_bytes: Option<u64>,
    /// CPU core-seconds consumed, when the platform reported them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_core_seconds: Option<f64>,
}

/// One attempt of the repeated experiment, named by the bundle it came from.
///
/// Invalid attempts stay in the list. A suite that silently dropped them would
/// report a median over a set nobody can reconstruct, and the count of what was
/// discarded is exactly the fact a sceptical reader wants first.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuiteAttempt {
    /// Attempt id, which also names the bundle directory.
    pub attempt_id: String,
    /// The sealed bundle's digest, so the attempt can be fetched and checked.
    pub bundle_digest: String,
    /// How far the attempt got.
    pub execution_status: ExecutionStatus,
    /// Whether the attempt's evidence may be believed; the separate axis.
    pub run_valid: bool,
    /// One entry per subject the attempt measured.
    pub subjects: Vec<SuiteSubjectObservation>,
}

/// One subject's median across the valid attempts, field for field.
///
/// The fields mirror [`SuiteSubjectObservation`] exactly, so that reading a
/// median and reading the observations it came from is the same act.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectMedians {
    /// Subject name, matching the resolved experiment.
    pub name: String,
    /// The subject's role in this comparison, when it declared one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// Median acknowledged records per second.
    pub acknowledged_records_per_second: f64,
    /// Median of the per-attempt median offer-to-terminal latencies.
    pub p50_intended_to_terminal_ns: u64,
    /// Median of the per-attempt 99th percentile offer-to-terminal latencies.
    pub p99_intended_to_terminal_ns: u64,
    /// Median of the per-attempt 99.9th percentile offer-to-terminal latencies.
    pub p999_intended_to_terminal_ns: u64,
    /// Median of the per-attempt 99th percentile admission waits.
    pub p99_admission_wait_ns: u64,
    /// Median peak resident set size, when every attempt reported one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rss_bytes: Option<u64>,
    /// Median CPU core-seconds, when every attempt reported them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_core_seconds: Option<f64>,
}

/// One directed ratio between two subjects, with its confidence interval.
///
/// `metric` names the [`SubjectMedians`] field the ratio is over, so a reader
/// never has to guess which number was divided. The interval is the bootstrap
/// interval over the paired per-attempt values, at the resample count the
/// summary declares.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PairedRatio {
    /// Subject on top of the ratio.
    pub numerator_subject: String,
    /// Subject the ratio divides by, named rather than implied.
    pub denominator_subject: String,
    /// Which [`SubjectMedians`] field this ratio is over.
    pub metric: String,
    /// The numerator's median divided by the denominator's median.
    pub ratio_of_medians: f64,
    /// Lower end of the bootstrap interval.
    pub ci_low: f64,
    /// Upper end of the bootstrap interval.
    pub ci_high: f64,
}

/// How much one subject's metric moved across the repetitions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectDispersion {
    /// Subject name, matching the resolved experiment.
    pub name: String,
    /// Which [`SuiteSubjectObservation`] field the spread is over.
    pub metric: String,
    /// Standard deviation over the mean; absent when it could not be computed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coefficient_of_variation: Option<f64>,
}

/// One named condition the suite checked, and what it found.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateOutcome {
    /// Stable gate name, for example `dispersion-within-budget`.
    pub name: String,
    /// The sentence the gate enforces, in the gate's own words.
    pub description: String,
    /// Whether the condition held.
    pub passed: bool,
    /// What was actually observed, pass or fail.
    pub detail: String,
}

/// `kafkars.suite-summary.v1`: the repeated experiment's answer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuiteSummary {
    /// Schema id, always [`SuiteSummary::SCHEMA`].
    pub schema: String,
    /// The experiment every attempt below is an attempt of.
    pub experiment_id: ExperimentId,
    /// Scenario name, carried for readers who do not resolve ids.
    pub scenario_name: String,
    /// Repetitions the suite was asked for.
    pub repetitions: u32,
    /// Seed behind every deterministic choice, including the resampling.
    pub seed: u64,
    /// Bootstrap resamples behind every interval below.
    pub resamples: u32,
    /// The relative difference the suite was set up to care about.
    pub practical_threshold: f64,
    /// Every attempt, valid or not, in execution order.
    pub attempts: Vec<SuiteAttempt>,
    /// Per-subject medians over the valid attempts.
    pub medians: Vec<SubjectMedians>,
    /// Directed ratios between subjects, with intervals.
    pub pairs: Vec<PairedRatio>,
    /// Per-subject, per-metric spread across the repetitions.
    pub dispersion: Vec<SubjectDispersion>,
    /// Conditions the suite checked, in the order it checked them.
    pub gates: Vec<GateOutcome>,
    /// Whether the suite may support a published claim. False in this
    /// milestone, always.
    pub claim_eligible: bool,
    /// Anything a reader must know that the numbers do not say.
    pub notes: Vec<String>,
}

impl SuiteSummary {
    /// Schema id this document declares.
    pub const SCHEMA: &'static str = SUITE_SUMMARY_V1;

    /// Reports whether the document declares the expected schema id.
    pub fn has_expected_schema(&self) -> bool {
        self.schema == Self::SCHEMA
    }

    /// Parses and validates a suite summary from its bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the bytes are not JSON, the schema id is wrong, or
    /// an invariant documented on this type fails.
    pub fn from_slice(bytes: &[u8]) -> SchemaResult<Self> {
        let document: Self = serde_json::from_slice(bytes)
            .map_err(|error| SchemaError::parse(format!("suite-summary.v1: {error}")))?;
        document.validate()?;
        Ok(document)
    }

    /// Checks the invariants documented on this type.
    ///
    /// # Errors
    ///
    /// Returns an error naming the first violated invariant.
    pub fn validate(&self) -> SchemaResult<()> {
        require_schema(&self.schema, Self::SCHEMA)?;
        if self.claim_eligible {
            return Err(SchemaError::invalid_field(
                "claim_eligible",
                "no suite in this milestone may support a published claim",
            ));
        }
        if !self.practical_threshold.is_finite() || self.practical_threshold < 0.0 {
            return Err(SchemaError::invalid_field(
                "practical_threshold",
                "must be a finite, non-negative relative difference",
            ));
        }
        for (index, attempt) in self.attempts.iter().enumerate() {
            for (subject, observation) in attempt.subjects.iter().enumerate() {
                check_role(
                    &format!("attempts[{index}].subjects[{subject}].role"),
                    observation.role.as_deref(),
                )?;
            }
        }
        for (index, median) in self.medians.iter().enumerate() {
            check_role(&format!("medians[{index}].role"), median.role.as_deref())?;
        }
        for (index, pair) in self.pairs.iter().enumerate() {
            if !pair.ci_low.is_finite() || !pair.ci_high.is_finite() || pair.ci_low > pair.ci_high {
                return Err(SchemaError::invalid_field(
                    &format!("pairs[{index}]"),
                    &format!(
                        "interval [{}, {}] is empty or not a number",
                        pair.ci_low, pair.ci_high
                    ),
                ));
            }
        }
        Ok(())
    }
}

/// Fails unless an optional role is one the vocabulary knows.
fn check_role(field: &str, role: Option<&str>) -> SchemaResult<()> {
    match role {
        Some(role) if !is_subject_role(role) => Err(SchemaError::invalid_field(
            field,
            &format!("{role:?} is not one of {SUBJECT_ROLES:?}"),
        )),
        _ => Ok(()),
    }
}
