//! Round-trips, and the three ways prose can fail to be about its packet.
#![expect(clippy::unwrap_used, reason = "test assertions may unwrap")]

use crate::llm::{Confidence, LlmFinding, LlmHypothesis, LlmSummary};
use crate::packet::Verdict;
use crate::packet_test::fixture as packet;
use crate::{AnalysisPacket, SchemaErrorKind};

fn fixture() -> LlmSummary {
    LlmSummary {
        schema: LlmSummary::SCHEMA.to_owned(),
        verdict: Verdict::Improved,
        executive_summary: "The head subject acknowledged about 20% more records per second \
             than the base subject across five repetitions, with the interval clear of the \
             practical threshold."
            .to_owned(),
        findings: vec![LlmFinding {
            text: "Goodput improved by a margin the repetitions agree on.".to_owned(),
            metric_refs: vec!["M003".to_owned()],
            evidence_refs: vec!["E001".to_owned()],
        }],
        hypotheses: vec![LlmHypothesis {
            text: "The gap is admission-path work rather than broker-side batching.".to_owned(),
            confidence: Confidence::Low,
            evidence_refs: vec!["E002".to_owned()],
        }],
        next_experiments: vec![
            "repeat at 16 KiB payloads to test the batching hypothesis".to_owned(),
        ],
        caveats: vec!["one host, one broker set, one payload size".to_owned()],
    }
}

#[test]
fn a_summary_round_trips_through_bytes() {
    let document = fixture();
    document.validate().unwrap();

    let bytes = serde_json::to_vec(&document).unwrap();
    let reparsed = LlmSummary::from_slice(&bytes).unwrap();

    assert_eq!(reparsed, document);
    assert!(reparsed.has_expected_schema());
}

#[test]
fn the_confidence_wire_strings_are_pinned() {
    for (confidence, wire) in [
        (Confidence::Low, "low"),
        (Confidence::Medium, "medium"),
        (Confidence::High, "high"),
    ] {
        assert_eq!(confidence.as_str(), wire);
        assert_eq!(
            serde_json::to_string(&confidence).unwrap(),
            format!("\"{wire}\"")
        );
    }
}

#[test]
fn a_summary_that_cites_only_the_packet_is_accepted() {
    fixture().validate_against(&packet()).unwrap();
}

#[test]
fn the_schema_id_is_checked() {
    let mut wrong = fixture();
    wrong.schema = "kafkars.analysis-packet.v1".to_owned();

    assert_eq!(
        wrong.validate().unwrap_err().kind(),
        SchemaErrorKind::WrongSchema
    );
}

#[test]
fn a_summary_that_says_nothing_is_refused() {
    let mut empty = fixture();
    empty.executive_summary = "   ".to_owned();

    let error = empty.validate().unwrap_err();

    assert_eq!(error.kind(), SchemaErrorKind::InvalidField);
    assert!(error.context().starts_with("executive_summary"), "{error}");
}

#[test]
fn the_summary_may_not_overrule_the_deterministic_verdict() {
    let packet: AnalysisPacket = packet();
    for verdict in [
        Verdict::Regressed,
        Verdict::Mixed,
        Verdict::Inconclusive,
        Verdict::Invalid,
    ] {
        let mut disagreeing = fixture();
        disagreeing.verdict = verdict;

        let error = disagreeing.validate_against(&packet).unwrap_err();

        assert_eq!(error.kind(), SchemaErrorKind::InvalidField);
        assert!(error.context().starts_with("verdict"), "{error}");
        assert!(error.context().contains("improved"), "{error}");
    }
}

#[test]
fn a_summary_may_not_cite_a_metric_the_packet_does_not_define() {
    let mut dangling = fixture();
    dangling.findings[0].metric_refs.push("M404".to_owned());

    let error = dangling.validate_against(&packet()).unwrap_err();

    assert_eq!(error.kind(), SchemaErrorKind::InvalidField);
    assert!(
        error.context().starts_with("findings[0].metric_refs"),
        "{error}"
    );
    assert!(error.context().contains("M404"), "{error}");
}

#[test]
fn a_summary_may_not_cite_evidence_the_packet_does_not_carry() {
    let mut in_finding = fixture();
    in_finding.findings[0].evidence_refs.push("E404".to_owned());
    let error = in_finding.validate_against(&packet()).unwrap_err();
    assert!(
        error.context().starts_with("findings[0].evidence_refs"),
        "{error}"
    );

    let mut in_hypothesis = fixture();
    in_hypothesis.hypotheses[0]
        .evidence_refs
        .push("E404".to_owned());
    let error = in_hypothesis.validate_against(&packet()).unwrap_err();
    assert!(
        error.context().starts_with("hypotheses[0].evidence_refs"),
        "{error}"
    );
}

#[test]
fn a_summary_that_cites_nothing_at_all_is_still_bound_to_the_verdict() {
    let mut silent = fixture();
    silent.findings.clear();
    silent.hypotheses.clear();

    silent.validate_against(&packet()).unwrap();

    silent.verdict = Verdict::Inconclusive;
    assert!(silent.validate_against(&packet()).is_err());
}
