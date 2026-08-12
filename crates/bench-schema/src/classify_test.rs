//! Tests pinning the validity and comparison documents, including the one
//! place floating-point numbers are welcome.
#![expect(
    clippy::unwrap_used,
    reason = "classification fixtures are exact; a bad one must fail the test immediately"
)]

use crate::{
    Classification, Comparison, ComparisonPair, SchemaErrorKind, SubjectValidity, canonical_bytes,
    parse_json_slice, pretty_bytes,
};

fn classification() -> Classification {
    Classification {
        schema: Classification::SCHEMA.to_owned(),
        run_valid: true,
        claim_eligible: false,
        subjects: vec![SubjectValidity {
            name: "kafkars".to_owned(),
            valid: true,
            reasons: Vec::new(),
        }],
        deferred_checks: vec![
            "latency-csv row validation".to_owned(),
            "librdkafka statistics summaries".to_owned(),
        ],
        reasons: vec!["one diagnostic repetition rather than five predeclared ones".to_owned()],
    }
}

fn comparison() -> Comparison {
    Comparison {
        schema: Comparison::SCHEMA.to_owned(),
        comparable: false,
        pairs: vec![ComparisonPair {
            baseline: "librdkafka-c".to_owned(),
            candidate: "kafkars".to_owned(),
            acknowledged_goodput_ratio: Some(1.03),
            p99_latency_ratio: Some(0.97),
        }],
        reasons: vec!["a single paired repetition cannot support a comparison".to_owned()],
    }
}

#[test]
fn the_schema_ids_are_the_registered_ones() {
    assert_eq!(Classification::SCHEMA, "kafkars.classification.v1");
    assert_eq!(Comparison::SCHEMA, "kafkars.comparison.v1");
    assert!(classification().has_expected_schema());
    assert!(comparison().has_expected_schema());
}

#[test]
fn a_classification_round_trips_and_carries_no_floats() {
    let classification = classification();

    let bytes = canonical_bytes(&classification).unwrap();

    assert_eq!(
        parse_json_slice::<Classification>(&bytes).unwrap(),
        classification
    );
}

#[test]
fn validity_is_recorded_separately_from_claim_eligibility() {
    let text = String::from_utf8(canonical_bytes(&classification()).unwrap()).unwrap();

    assert!(text.contains(r#""claim_eligible":false"#), "{text}");
    assert!(text.contains(r#""run_valid":true"#), "{text}");
    assert!(text.contains("deferred_checks"), "{text}");
}

#[test]
fn a_comparison_may_carry_ratios_and_therefore_cannot_be_hashed() {
    let error = canonical_bytes(&comparison()).unwrap_err();

    assert_eq!(error.kind(), SchemaErrorKind::NonCanonicalNumber);
}

#[test]
fn a_comparison_is_written_as_evidence() {
    let bytes = pretty_bytes(&comparison()).unwrap();
    let text = String::from_utf8(bytes.clone()).unwrap();

    assert!(
        text.contains(r#""acknowledged_goodput_ratio": 1.03"#),
        "{text}"
    );
    assert_eq!(
        parse_json_slice::<Comparison>(&bytes).unwrap(),
        comparison()
    );
}

#[test]
fn a_classification_that_claims_eligibility_is_refused() {
    // The same refusal `kafkars.experiment.v1`, `kafkars.suite-summary.v1`, and
    // `kafkars.analysis-packet.v1` already make. This document decides whether
    // a run may be believed at all, so it is the last one that should be able
    // to grant itself more than the milestone allows on a hand edit.
    let mut claiming = classification();
    claiming.claim_eligible = true;

    let error = claiming.validate().unwrap_err();

    assert_eq!(error.kind(), SchemaErrorKind::InvalidField);
    assert!(error.to_string().contains("claim_eligible"), "{error}");

    let bytes = pretty_bytes(&claiming).unwrap();
    assert!(
        Classification::from_slice(&bytes).is_err(),
        "the parse gate refuses it too, so no reader has to remember to check"
    );
}

#[test]
fn an_invalid_run_that_gives_no_reason_is_refused() {
    let mut silent = classification();
    silent.run_valid = false;
    silent.reasons = Vec::new();

    let error = silent.validate().unwrap_err();

    assert!(error.to_string().contains("must say why"), "{error}");
}

#[test]
fn a_valid_run_may_still_name_why_it_cannot_be_claimed() {
    // `reasons` carries both "why invalid" and "why not claim-eligible". The
    // fixture is valid *and* names a reason, which is the ordinary shape.
    let document = classification();

    assert!(document.run_valid && !document.reasons.is_empty());
    assert!(document.validate().is_ok());
}

#[test]
fn a_subject_verdict_that_disagrees_with_itself_is_refused() {
    let mut valid_with_reasons = classification();
    valid_with_reasons.subjects[0].reasons = vec!["the adapter exited with code 3".to_owned()];
    assert!(valid_with_reasons.validate().is_err());

    let mut invalid_without_reasons = classification();
    invalid_without_reasons.subjects[0].valid = false;
    assert!(invalid_without_reasons.validate().is_err());

    let mut nameless = classification();
    nameless.subjects[0].name = String::new();
    assert!(nameless.validate().is_err());
}

#[test]
fn a_classification_of_a_different_schema_is_refused() {
    let mut wrong = classification();
    "kafkars.comparison.v1".clone_into(&mut wrong.schema);

    let error = wrong.validate().unwrap_err();

    assert_eq!(error.kind(), SchemaErrorKind::WrongSchema);
}

#[test]
fn a_well_formed_classification_parses_back_through_the_validating_gate() {
    let bytes = pretty_bytes(&classification()).unwrap();

    assert_eq!(
        Classification::from_slice(&bytes).unwrap(),
        classification()
    );
}

#[test]
fn a_missing_ratio_is_absent_rather_than_zero() {
    let mut comparison = comparison();
    comparison.pairs[0].acknowledged_goodput_ratio = None;
    comparison.pairs[0].p99_latency_ratio = None;

    let text = String::from_utf8(pretty_bytes(&comparison).unwrap()).unwrap();

    assert!(!text.contains("ratio"), "{text}");
}
