//! `kafkars.analysis-packet.v1`: everything a reader — human or model — is
//! allowed to reason from, and nothing else.
//!
//! The packet is the boundary between measurement and interpretation. Upstream
//! of it, numbers are derived by code that can be re-run; downstream of it,
//! prose is written about numbers that can no longer change. Making that
//! boundary a document rather than a convention is what lets
//! [`LlmSummary::validate_against`](crate::LlmSummary::validate_against) check a
//! summary mechanically instead of trusting it.
//!
//! # Numbered metrics, referenced findings
//!
//! Every quantity the packet exposes lives in `metrics` under a short stable key
//! — `M001`, `M002` — carrying its own human name and unit. Findings do not
//! restate values; they cite keys. A finding that cites `M004` cannot drift from
//! `M004`, and a finding that cites a key the packet does not define is caught
//! by [`AnalysisPacket::validate`] rather than by a reader noticing.
//!
//! `evidence_refs` does the same job for provenance: a short key maps to a
//! bundle-relative path or a bundle digest, so a claim can point at the file it
//! came from without embedding a path in prose.
//!
//! # The verdict is deterministic
//!
//! `verdict` is computed here, from the suite's intervals and gates, and no
//! downstream document may disagree with it. That is the whole point of the
//! split: the prose layer can say things the packet does not, but it cannot say
//! *improved* about a packet that says *inconclusive*.
//!
//! # Invariants ([`AnalysisPacket::validate`])
//!
//! `validity.runs_valid <= validity.runs_total`; `claim_eligible` is false;
//! every metric key is non-empty; every subject role is one the vocabulary
//! knows; and every `metric_refs` entry in `deterministic_findings` names a
//! metric the packet actually defines.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::{SchemaError, SchemaResult};
use crate::experiment::{SUBJECT_ROLES, is_subject_role};
use crate::identity::ExperimentId;
use crate::schema_id::{ANALYSIS_PACKET_V1, require_schema};

/// What the evidence says happened, decided by code and never by prose.
///
/// The wire strings are pinned by tests, because
/// [`LlmSummary`](crate::LlmSummary) is checked for equality against this value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// The head subject is better on the metrics that were gated.
    Improved,
    /// The head subject is worse on the metrics that were gated.
    Regressed,
    /// Some gated metrics improved and others regressed.
    Mixed,
    /// The evidence does not separate the subjects, or does not suffice.
    Inconclusive,
    /// The evidence may not be reasoned from at all.
    Invalid,
}

impl Verdict {
    /// Returns the wire string for this verdict.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Improved => "improved",
            Self::Regressed => "regressed",
            Self::Mixed => "mixed",
            Self::Inconclusive => "inconclusive",
            Self::Invalid => "invalid",
        }
    }
}

/// Where the packet's numbers came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PacketSource {
    /// The experiment the suite summarized, when a suite is the source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suite: Option<ExperimentId>,
    /// Digest of every sealed bundle behind the numbers, in attempt order.
    pub bundle_digests: Vec<String>,
}

/// One subject the packet is about.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PacketSubject {
    /// Subject name, matching the resolved experiment.
    pub name: String,
    /// The subject's role in this comparison, when it declared one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
}

/// How much of the underlying evidence may be believed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PacketValidity {
    /// Attempts whose evidence passed every performed check.
    pub runs_valid: u32,
    /// Attempts the suite ran, valid or not.
    pub runs_total: u32,
    /// Whether the packet may support a published claim. False in this
    /// milestone, always.
    pub claim_eligible: bool,
}

/// One numbered quantity, with the name and unit a reader needs to quote it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PacketMetric {
    /// Human name, for example `p99 offer-to-terminal, head`.
    pub name: String,
    /// The value itself.
    pub value: f64,
    /// Unit the value is in, for example `ns`, `records/s`, `ratio`.
    pub unit: String,
}

/// One statement the deterministic layer is willing to make.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PacketFinding {
    /// The statement, in one sentence.
    pub text: String,
    /// Keys into [`AnalysisPacket::metrics`] the statement rests on.
    pub metric_refs: Vec<String>,
}

/// `kafkars.analysis-packet.v1`: the deterministic input to interpretation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnalysisPacket {
    /// Schema id, always [`AnalysisPacket::SCHEMA`].
    pub schema: String,
    /// Where the numbers came from.
    pub source: PacketSource,
    /// What the evidence says, decided here and nowhere else.
    pub verdict: Verdict,
    /// Scenario name, carried for readers who do not resolve ids.
    pub scenario_name: String,
    /// The subjects the packet is about, in comparison order.
    pub subjects: Vec<PacketSubject>,
    /// How much of the evidence may be believed.
    pub validity: PacketValidity,
    /// Every quantity a reader may cite, by stable key.
    pub metrics: BTreeMap<String, PacketMetric>,
    /// Statements the deterministic layer makes, each citing its metrics.
    pub deterministic_findings: Vec<PacketFinding>,
    /// Anything unexplained that a reader must weigh before concluding.
    pub anomalies: Vec<String>,
    /// Short key to bundle-relative path or bundle digest.
    pub evidence_refs: BTreeMap<String, String>,
}

impl AnalysisPacket {
    /// Schema id this document declares.
    pub const SCHEMA: &'static str = ANALYSIS_PACKET_V1;

    /// Reports whether the document declares the expected schema id.
    pub fn has_expected_schema(&self) -> bool {
        self.schema == Self::SCHEMA
    }

    /// Parses and validates a packet from its bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the bytes are not JSON, the schema id is wrong, or
    /// an invariant documented on this type fails.
    pub fn from_slice(bytes: &[u8]) -> SchemaResult<Self> {
        let document: Self = serde_json::from_slice(bytes)
            .map_err(|error| SchemaError::parse(format!("analysis-packet.v1: {error}")))?;
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
        if self.validity.claim_eligible {
            return Err(SchemaError::invalid_field(
                "validity.claim_eligible",
                "no packet in this milestone may support a published claim",
            ));
        }
        if self.validity.runs_valid > self.validity.runs_total {
            return Err(SchemaError::invalid_field(
                "validity.runs_valid",
                &format!(
                    "{} valid runs out of {} total",
                    self.validity.runs_valid, self.validity.runs_total
                ),
            ));
        }
        for (index, subject) in self.subjects.iter().enumerate() {
            if let Some(role) = &subject.role
                && !is_subject_role(role)
            {
                return Err(SchemaError::invalid_field(
                    &format!("subjects[{index}].role"),
                    &format!("{role:?} is not one of {SUBJECT_ROLES:?}"),
                ));
            }
        }
        if self.metrics.keys().any(|key| key.trim().is_empty()) {
            return Err(SchemaError::invalid_field(
                "metrics",
                "a metric nobody can cite by key is a metric nobody can cite",
            ));
        }
        for (index, finding) in self.deterministic_findings.iter().enumerate() {
            for reference in &finding.metric_refs {
                if !self.metrics.contains_key(reference) {
                    return Err(SchemaError::invalid_field(
                        &format!("deterministic_findings[{index}].metric_refs"),
                        &format!("{reference:?} is not a metric this packet defines"),
                    ));
                }
            }
        }
        Ok(())
    }
}
