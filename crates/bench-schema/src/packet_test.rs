//! Round-trips, the verdict vocabulary, and the reference-integrity rules of
//! the deterministic analysis packet.
#![expect(clippy::unwrap_used, reason = "test assertions may unwrap")]

use std::collections::BTreeMap;

use crate::packet::{
    AnalysisPacket, PacketFinding, PacketMetric, PacketSource, PacketSubject, PacketValidity,
    Verdict,
};
use crate::{ExperimentId, SUBJECT_ROLE_BASE, SUBJECT_ROLE_HEAD, SchemaErrorKind};

const EXPERIMENT: &str = "d2932f88ad348028796b43b929f2fca826b683d19f13c0d1ad30d676b25dac15";

fn metric(name: &str, value: f64, unit: &str) -> PacketMetric {
    PacketMetric {
        name: name.to_owned(),
        value,
        unit: unit.to_owned(),
    }
}

pub(crate) fn fixture() -> AnalysisPacket {
    let mut metrics = BTreeMap::new();
    metrics.insert(
        "M001".to_owned(),
        metric(
            "acknowledged records per second, head",
            120_000.0,
            "records/s",
        ),
    );
    metrics.insert(
        "M002".to_owned(),
        metric(
            "acknowledged records per second, base",
            100_000.0,
            "records/s",
        ),
    );
    metrics.insert(
        "M003".to_owned(),
        metric("goodput ratio, head over base", 1.2, "ratio"),
    );

    let mut evidence_refs = BTreeMap::new();
    evidence_refs.insert(
        "E001".to_owned(),
        "20260812T101500Z-0123abcd/suite-summary.json".to_owned(),
    );
    evidence_refs.insert("E002".to_owned(), "c".repeat(64));

    AnalysisPacket {
        schema: AnalysisPacket::SCHEMA.to_owned(),
        source: PacketSource {
            suite: Some(ExperimentId::parse(EXPERIMENT).unwrap()),
            bundle_digests: vec!["c".repeat(64)],
        },
        verdict: Verdict::Improved,
        scenario_name: "producer-balanced-1k-diagnostic".to_owned(),
        subjects: vec![
            PacketSubject {
                name: "kafkars".to_owned(),
                role: Some(SUBJECT_ROLE_HEAD.to_owned()),
            },
            PacketSubject {
                name: "librdkafka-c".to_owned(),
                role: Some(SUBJECT_ROLE_BASE.to_owned()),
            },
        ],
        validity: PacketValidity {
            runs_valid: 5,
            runs_total: 5,
            claim_eligible: false,
        },
        metrics,
        deterministic_findings: vec![PacketFinding {
            text: "head acknowledged 20% more records per second than base".to_owned(),
            metric_refs: vec!["M001".to_owned(), "M002".to_owned(), "M003".to_owned()],
        }],
        anomalies: vec!["attempt 3 ran while the host was compiling".to_owned()],
        evidence_refs,
    }
}

#[test]
fn a_packet_round_trips_through_bytes() {
    let document = fixture();
    document.validate().unwrap();

    let bytes = serde_json::to_vec(&document).unwrap();
    let reparsed = AnalysisPacket::from_slice(&bytes).unwrap();

    assert_eq!(reparsed, document);
    assert!(reparsed.has_expected_schema());
}

#[test]
fn the_verdict_wire_strings_are_pinned() {
    for (verdict, wire) in [
        (Verdict::Improved, "improved"),
        (Verdict::Regressed, "regressed"),
        (Verdict::Mixed, "mixed"),
        (Verdict::Inconclusive, "inconclusive"),
        (Verdict::Invalid, "invalid"),
    ] {
        assert_eq!(verdict.as_str(), wire);
        assert_eq!(
            serde_json::to_string(&verdict).unwrap(),
            format!("\"{wire}\"")
        );
    }
}

#[test]
fn an_absent_suite_source_is_omitted_from_the_bytes() {
    let mut document = fixture();
    document.source.suite = None;
    for subject in &mut document.subjects {
        subject.role = None;
    }

    let bytes = serde_json::to_string(&document).unwrap();

    assert!(!bytes.contains("\"suite\""));
    assert!(!bytes.contains("\"role\""));
    assert_eq!(
        AnalysisPacket::from_slice(bytes.as_bytes()).unwrap(),
        document
    );
}

#[test]
fn the_schema_id_is_checked() {
    let mut wrong = fixture();
    wrong.schema = "kafkars.comparison.v1".to_owned();

    assert_eq!(
        wrong.validate().unwrap_err().kind(),
        SchemaErrorKind::WrongSchema
    );
}

#[test]
fn a_claim_eligible_packet_is_refused() {
    let mut claiming = fixture();
    claiming.validity.claim_eligible = true;

    let error = claiming.validate().unwrap_err();

    assert_eq!(error.kind(), SchemaErrorKind::InvalidField);
    assert!(
        error.context().starts_with("validity.claim_eligible"),
        "{error}"
    );
}

#[test]
fn more_valid_runs_than_runs_is_refused() {
    let mut impossible = fixture();
    impossible.validity.runs_valid = 6;

    let error = impossible.validate().unwrap_err();

    assert!(
        error.context().starts_with("validity.runs_valid"),
        "{error}"
    );
}

#[test]
fn an_unknown_subject_role_is_refused() {
    let mut mislabeled = fixture();
    mislabeled.subjects[0].role = Some("candidate".to_owned());

    let error = mislabeled.validate().unwrap_err();

    assert!(error.context().starts_with("subjects[0].role"), "{error}");
}

#[test]
fn a_finding_may_not_cite_a_metric_the_packet_does_not_define() {
    let mut dangling = fixture();
    dangling.deterministic_findings[0]
        .metric_refs
        .push("M404".to_owned());

    let error = dangling.validate().unwrap_err();

    assert_eq!(error.kind(), SchemaErrorKind::InvalidField);
    assert!(
        error
            .context()
            .starts_with("deterministic_findings[0].metric_refs"),
        "{error}"
    );
    assert!(error.context().contains("M404"), "{error}");
}

#[test]
fn a_metric_nobody_can_cite_is_refused() {
    let mut unkeyed = fixture();
    unkeyed
        .metrics
        .insert(String::new(), metric("nameless", 1.0, "ratio"));

    assert!(
        unkeyed
            .validate()
            .unwrap_err()
            .context()
            .starts_with("metrics")
    );
}
