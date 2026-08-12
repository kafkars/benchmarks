//! Round-trips and the invariant matrix for the capacity search document.
#![expect(clippy::unwrap_used, reason = "test assertions may unwrap")]

use crate::capacity::{CapacityProbe, CapacitySearch, CapacityStatus};
use crate::{SchemaErrorKind, SloSpec};

fn probe(rate: u64, satisfied: bool) -> CapacityProbe {
    CapacityProbe {
        offered_records_per_second: rate,
        attempt_id: format!("20260812T1015{rate:02}Z-0123abcd"),
        bundle_digest: "b".repeat(64),
        satisfied,
        reasons: if satisfied {
            Vec::new()
        } else {
            vec!["corrected p99 250ms exceeded at 412ms".to_owned()]
        },
    }
}

fn fixture() -> CapacitySearch {
    CapacitySearch {
        schema: CapacitySearch::SCHEMA.to_owned(),
        subject: "librdkafka-c".to_owned(),
        slo: SloSpec {
            corrected_p99_ms: Some(250),
            ..SloSpec::default()
        },
        probes: vec![
            probe(25_000, true),
            probe(50_000, true),
            probe(75_000, false),
        ],
        bracket_low: Some(50_000),
        bracket_high: Some(75_000),
        resolution: 5_000,
        confirmed_rate: Some(50_000),
        confirmation: vec![probe(50_000, true), probe(50_000, true)],
        status: CapacityStatus::Converged,
    }
}

#[test]
fn a_capacity_search_round_trips_through_bytes() {
    let document = fixture();
    document.validate().unwrap();

    let bytes = serde_json::to_vec(&document).unwrap();
    let reparsed = CapacitySearch::from_slice(&bytes).unwrap();

    assert_eq!(reparsed, document);
    assert!(reparsed.has_expected_schema());
}

#[test]
fn the_status_wire_strings_are_pinned() {
    assert_eq!(CapacityStatus::Converged.as_str(), "converged");
    assert_eq!(CapacityStatus::Unconverged.as_str(), "unconverged");
    assert_eq!(CapacityStatus::Invalid.as_str(), "invalid");
    assert_eq!(
        serde_json::to_string(&CapacityStatus::Unconverged).unwrap(),
        r#""unconverged""#
    );
}

#[test]
fn an_unconverged_search_omits_its_bracket_and_rate() {
    let unconverged = CapacitySearch {
        bracket_low: None,
        bracket_high: None,
        confirmed_rate: None,
        confirmation: Vec::new(),
        status: CapacityStatus::Unconverged,
        ..fixture()
    };

    unconverged.validate().unwrap();
    let bytes = serde_json::to_string(&unconverged).unwrap();

    for key in ["bracket_low", "bracket_high", "confirmed_rate"] {
        assert!(!bytes.contains(key), "{key} must be omitted when absent");
    }
}

#[test]
fn the_schema_id_is_checked() {
    let mut wrong = fixture();
    wrong.schema = "kafkars.librdkafka-capacity-curve.v2".to_owned();

    assert_eq!(
        wrong.validate().unwrap_err().kind(),
        SchemaErrorKind::WrongSchema
    );
}

#[test]
fn a_converged_search_must_state_its_rate() {
    let mut silent = fixture();
    silent.confirmed_rate = None;

    let error = silent.validate().unwrap_err();

    assert_eq!(error.kind(), SchemaErrorKind::InvalidField);
    assert!(error.context().starts_with("confirmed_rate"), "{error}");
}

#[test]
fn an_unfinished_search_must_not_claim_a_rate() {
    for status in [CapacityStatus::Unconverged, CapacityStatus::Invalid] {
        let mut claiming = fixture();
        claiming.status = status;

        let error = claiming.validate().unwrap_err();

        assert!(
            error.context().contains(status.as_str()),
            "the rejection should name the status: {error}"
        );
    }
}

#[test]
fn a_search_without_a_subject_or_a_resolution_is_refused() {
    let mut anonymous = fixture();
    anonymous.subject = "  ".to_owned();
    assert!(
        anonymous
            .validate()
            .unwrap_err()
            .context()
            .starts_with("subject")
    );

    let mut zero_resolution = fixture();
    zero_resolution.resolution = 0;
    assert!(
        zero_resolution
            .validate()
            .unwrap_err()
            .context()
            .starts_with("resolution")
    );
}

#[test]
fn an_inverted_bracket_is_refused() {
    let mut inverted = fixture();
    inverted.bracket_low = Some(90_000);

    let error = inverted.validate().unwrap_err();

    assert!(error.context().starts_with("bracket_low"), "{error}");
}

#[test]
fn a_failing_probe_carries_its_reasons() {
    let document = fixture();
    let failing = document
        .probes
        .iter()
        .find(|probe| !probe.satisfied)
        .unwrap();

    assert_eq!(failing.offered_records_per_second, 75_000);
    assert_eq!(failing.reasons.len(), 1);
}
