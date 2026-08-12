//! The document itself: the ratios, the dispersion, the gates, and the
//! invariants a suite summary is required to satisfy.
//!
//! Everything here is derived from the observations in the sibling module and
//! carried beside them, never instead of them. That is what makes the document
//! auditable: a reader who distrusts a ratio can recompute it from the same
//! bytes that state it.

use serde::{Deserialize, Serialize};

use crate::error::{SchemaError, SchemaResult};
use crate::experiment::{SUBJECT_ROLES, is_subject_role};
use crate::identity::ExperimentId;
use crate::schema_id::{SUITE_SUMMARY_V1, require_schema};

use super::observation::{SubjectMedians, SuiteAttempt};

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
    /// Which [`SuiteSubjectObservation`](super::SuiteSubjectObservation) field
    /// the spread is over.
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
