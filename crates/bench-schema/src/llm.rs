//! `kafkars.llm-summary.v1`: prose written over an analysis packet, and bound
//! to it.
//!
//! A language model is useful here for exactly one thing: saying, in readable
//! English, what a packet of numbers means. It is not useful for deciding what
//! the numbers are, and this document is shaped so that it cannot. Every
//! quantitative claim is a reference into the packet's `metrics`, every citation
//! is a reference into the packet's `evidence_refs`, and the verdict is copied
//! rather than concluded.
//!
//! [`LlmSummary::validate_against`] is the enforcement, and it is mechanical:
//! a dangling metric reference, a dangling evidence reference, an uncited
//! finding, or a verdict that differs from the packet's is a rejected document,
//! not a warning. The model may hedge, elaborate, and speculate — that is what
//! `hypotheses` and `caveats` are for, and hypotheses are labeled with a
//! confidence so speculation cannot be mistaken for a finding — but it may not
//! overrule the deterministic layer.
//!
//! # A finding must cite
//!
//! `findings` is the one list whose entries are asserted as fact, so an entry
//! with no `metric_refs` is refused outright. The alternative is worse than it
//! looks: an uncited sentence in the findings list reads exactly like a cited
//! one, carries the same authority, and rests on nothing a reader can check. A
//! statement the model cannot attach to a number in the packet is a hypothesis,
//! and `hypotheses` is where it belongs — labeled with its confidence, and
//! deliberately not required to cite.
//!
//! Rejecting a summary costs nothing: the packet is still there, and the numbers
//! in it never depended on the prose.

use serde::{Deserialize, Serialize};

use crate::error::{SchemaError, SchemaResult};
use crate::packet::{AnalysisPacket, Verdict};
use crate::schema_id::{LLM_SUMMARY_V1, require_schema};

/// How strongly a hypothesis is held.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    /// A guess worth writing down and nothing more.
    Low,
    /// Consistent with the evidence, not established by it.
    Medium,
    /// Strongly indicated, but still not something the packet asserts.
    High,
}

impl Confidence {
    /// Returns the wire string for this confidence.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

/// One statement about the evidence, cited from the packet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LlmFinding {
    /// The statement, in one sentence.
    pub text: String,
    /// Keys into the packet's `metrics` this statement rests on.
    pub metric_refs: Vec<String>,
    /// Keys into the packet's `evidence_refs` this statement points at.
    pub evidence_refs: Vec<String>,
}

/// One proposed explanation, labeled with how much weight it carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LlmHypothesis {
    /// The proposed explanation, in one sentence.
    pub text: String,
    /// How strongly it is held.
    pub confidence: Confidence,
    /// Keys into the packet's `evidence_refs` that motivated it.
    pub evidence_refs: Vec<String>,
}

/// How a summary was produced, carried with the summary itself.
///
/// The same facts also land beside the document as `llm-provenance.json`, and
/// that sidecar is not going away — the surrounding tooling reads it, and a
/// summary that was rejected still leaves one. Embedding a copy answers the
/// different question: a summary that has been *moved* — attached to an issue,
/// pasted into a report — arrives without its sidecar, and a reader then has no
/// way to ask which model wrote it, under which prompt, over which packet.
///
/// `input_sha256` is the digest of the exact packet bytes that were sent, so
/// the pairing is checkable rather than asserted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LlmProvenance {
    /// Model identifier the request named.
    pub model: String,
    /// Version of the committed prompt artifact the request was built from.
    pub prompt_version: String,
    /// Reasoning effort the request asked for.
    pub reasoning_effort: String,
    /// The provider's identifier for the response, when it gave one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_id: Option<String>,
    /// sha-256 of the analysis packet bytes that were sent.
    pub input_sha256: String,
    /// sha-256 of the summary bytes that came back.
    pub output_sha256: String,
    /// UTC instant the summary was written.
    pub created_at: String,
}

/// `kafkars.llm-summary.v1`: the readable layer over an analysis packet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LlmSummary {
    /// Schema id, always [`LlmSummary::SCHEMA`].
    pub schema: String,
    /// The packet's verdict, copied verbatim; never concluded here.
    pub verdict: Verdict,
    /// What happened, in a paragraph a reader can act on.
    pub executive_summary: String,
    /// Statements about the evidence, each citing the packet.
    pub findings: Vec<LlmFinding>,
    /// Proposed explanations, each labeled with its confidence.
    pub hypotheses: Vec<LlmHypothesis>,
    /// Experiments that would settle what this one did not.
    pub next_experiments: Vec<String>,
    /// What a reader must not conclude from this summary.
    pub caveats: Vec<String>,
    /// How this summary was produced, when the writer recorded it.
    ///
    /// Optional because the guardrail is about what the prose may claim, not
    /// about who wrote it: a hand-authored summary that cites correctly is
    /// still valid evidence, and refusing it for lacking a model name would
    /// make provenance a gate it was never meant to be.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<LlmProvenance>,
}

impl LlmSummary {
    /// Schema id this document declares.
    pub const SCHEMA: &'static str = LLM_SUMMARY_V1;

    /// Reports whether the document declares the expected schema id.
    pub fn has_expected_schema(&self) -> bool {
        self.schema == Self::SCHEMA
    }

    /// Parses and validates a summary from its bytes.
    ///
    /// This checks the document against itself only. A summary is not evidence
    /// until it has also passed [`LlmSummary::validate_against`] with the packet
    /// it was written over.
    ///
    /// # Errors
    ///
    /// Returns an error when the bytes are not JSON, the schema id is wrong, or
    /// an invariant documented on this type fails.
    pub fn from_slice(bytes: &[u8]) -> SchemaResult<Self> {
        let document: Self = serde_json::from_slice(bytes)
            .map_err(|error| SchemaError::parse(format!("llm-summary.v1: {error}")))?;
        document.validate()?;
        Ok(document)
    }

    /// Checks the invariants this document can check on its own.
    ///
    /// # Errors
    ///
    /// Returns an error naming the first violated invariant.
    pub fn validate(&self) -> SchemaResult<()> {
        require_schema(&self.schema, Self::SCHEMA)?;
        if self.executive_summary.trim().is_empty() {
            return Err(SchemaError::invalid_field(
                "executive_summary",
                "a summary that says nothing is not a summary",
            ));
        }
        Ok(())
    }

    /// Checks this summary against the packet it claims to be about.
    ///
    /// Four things are enforced, and all four are the same rule seen from
    /// different sides: prose may not invent quantities, may not invent
    /// provenance, may not assert a finding it cannot cite, and may not
    /// overrule the deterministic verdict.
    ///
    /// # Errors
    ///
    /// Returns an error naming the first reference that does not resolve, the
    /// first uncited finding, or the verdict disagreement.
    pub fn validate_against(&self, packet: &AnalysisPacket) -> SchemaResult<()> {
        self.validate()?;
        if self.verdict != packet.verdict {
            return Err(SchemaError::invalid_field(
                "verdict",
                &format!(
                    "{:?} does not match the packet's deterministic verdict {:?}",
                    self.verdict.as_str(),
                    packet.verdict.as_str()
                ),
            ));
        }
        for (index, finding) in self.findings.iter().enumerate() {
            if finding.metric_refs.is_empty() {
                return Err(SchemaError::invalid_field(
                    &format!("findings[{index}].metric_refs"),
                    "a finding states a fact about the evidence and must cite at least one \
                     metric the packet defines; an uncited statement is a hypothesis",
                ));
            }
            for reference in &finding.metric_refs {
                if !packet.metrics.contains_key(reference) {
                    return Err(SchemaError::invalid_field(
                        &format!("findings[{index}].metric_refs"),
                        &format!("{reference:?} is not a metric the packet defines"),
                    ));
                }
            }
            check_evidence(
                packet,
                &format!("findings[{index}].evidence_refs"),
                &finding.evidence_refs,
            )?;
        }
        for (index, hypothesis) in self.hypotheses.iter().enumerate() {
            check_evidence(
                packet,
                &format!("hypotheses[{index}].evidence_refs"),
                &hypothesis.evidence_refs,
            )?;
        }
        Ok(())
    }
}

/// Fails unless every reference names an evidence pointer the packet carries.
fn check_evidence(packet: &AnalysisPacket, field: &str, references: &[String]) -> SchemaResult<()> {
    for reference in references {
        if !packet.evidence_refs.contains_key(reference) {
            return Err(SchemaError::invalid_field(
                field,
                &format!("{reference:?} is not an evidence reference the packet defines"),
            ));
        }
    }
    Ok(())
}
