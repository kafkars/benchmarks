//! `kafkars.classification.v1` and `kafkars.comparison.v1`: whether the run may
//! be believed, and what it says.
//!
//! Validity is a separate axis from execution status, and this is the document
//! that holds it. A run can complete every phase and still be invalid; a
//! partial run can still contain one subject's perfectly good evidence. Keeping
//! the two in different documents means neither can be quietly upgraded into
//! the other.
//!
//! `claim_eligible` is false for every run this milestone can produce, and
//! `deferred_checks` is why: bootstrap confidence intervals, latency-row
//! validation, native metric gates, statistics summaries, and process resource
//! capture are all still in the legacy Node harness. Listing the skipped checks
//! by name in the evidence is the honest alternative to a footnote in a
//! document nobody opens.
//!
//! [`Comparison`] is the one place in this crate where floating-point numbers
//! are welcome. Ratios are measurement, and measurement is not identity: a
//! comparison document is never hashed into an experiment id, and
//! [`canonical_bytes`](crate::canonical_bytes) will refuse it if anyone tries.

use serde::{Deserialize, Serialize};

use crate::schema_id::{CLASSIFICATION_V1, COMPARISON_V1};

/// Whether one subject's evidence may be believed, and why not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectValidity {
    /// Subject name, matching the resolved experiment.
    pub name: String,
    /// Whether this subject's evidence is usable.
    pub valid: bool,
    /// One reason per failed check. Empty when valid.
    pub reasons: Vec<String>,
}

/// `kafkars.classification.v1`: the verdict on one attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Classification {
    /// Schema id, always [`Classification::SCHEMA`].
    pub schema: String,
    /// Whether the attempt as a whole is usable evidence.
    pub run_valid: bool,
    /// Whether the attempt may support a published claim. False in this
    /// milestone, always.
    pub claim_eligible: bool,
    /// Per-subject verdicts.
    pub subjects: Vec<SubjectValidity>,
    /// Checks this milestone does not perform, named so that a reader knows
    /// what the verdict does *not* cover.
    pub deferred_checks: Vec<String>,
    /// Why the run is invalid, or why it cannot support a claim.
    pub reasons: Vec<String>,
}

impl Classification {
    /// Schema id this document declares.
    pub const SCHEMA: &'static str = CLASSIFICATION_V1;

    /// Reports whether the document declares the expected schema id.
    pub fn has_expected_schema(&self) -> bool {
        self.schema == Self::SCHEMA
    }
}

/// One directed comparison between two subjects of the same attempt.
///
/// Ratios are candidate over baseline, so a goodput ratio above one and a
/// latency ratio below one both favour the candidate. Each ratio is absent
/// rather than zero when the underlying number was missing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComparisonPair {
    /// Subject the ratios divide by.
    pub baseline: String,
    /// Subject the ratios are for.
    pub candidate: String,
    /// Acknowledged records per second, candidate over baseline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acknowledged_goodput_ratio: Option<f64>,
    /// 99th percentile latency, candidate over baseline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p99_latency_ratio: Option<f64>,
}

/// `kafkars.comparison.v1`: what the attempt says about its subjects relative
/// to each other.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Comparison {
    /// Schema id, always [`Comparison::SCHEMA`].
    pub schema: String,
    /// Whether the subjects were comparable at all. A single attempt of a
    /// diagnostic scenario is not, and says so.
    pub comparable: bool,
    /// One entry per compared pair.
    pub pairs: Vec<ComparisonPair>,
    /// Why the subjects are not comparable, when they are not.
    pub reasons: Vec<String>,
}

impl Comparison {
    /// Schema id this document declares.
    pub const SCHEMA: &'static str = COMPARISON_V1;

    /// Reports whether the document declares the expected schema id.
    pub fn has_expected_schema(&self) -> bool {
        self.schema == Self::SCHEMA
    }
}
