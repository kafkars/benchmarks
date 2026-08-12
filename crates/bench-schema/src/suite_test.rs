//! Round-trips and the invariant matrix for the repeated-experiment summary.
#![expect(clippy::unwrap_used, reason = "test assertions may unwrap")]

use crate::suite::{
    GateOutcome, PairedRatio, SubjectDispersion, SubjectMedians, SuiteAttempt,
    SuiteSubjectObservation, SuiteSummary,
};
use crate::{
    DeclaredExecution, ExecutionStatus, ExperimentId, SUBJECT_ROLE_BASE, SUBJECT_ROLE_HEAD,
    SchemaErrorKind,
};

const EXPERIMENT: &str = "d2932f88ad348028796b43b929f2fca826b683d19f13c0d1ad30d676b25dac15";

fn declared() -> DeclaredExecution {
    DeclaredExecution {
        payload_construction: "prebuilt-pool-per-offer-sequence".to_owned(),
        ownership: "copy-in-reused-buffer".to_owned(),
        completion_mode: "delivery-callback".to_owned(),
        serialization: "excluded".to_owned(),
    }
}

fn observation(name: &str, role: &str, rate: f64) -> SuiteSubjectObservation {
    SuiteSubjectObservation {
        name: name.to_owned(),
        role: Some(role.to_owned()),
        declared: Some(declared()),
        acknowledged_records_per_second: rate,
        p50_intended_to_terminal_ns: 1_000_000,
        p99_intended_to_terminal_ns: 9_000_000,
        p999_intended_to_terminal_ns: 21_000_000,
        p99_admission_wait_ns: 250_000,
        p99_intended_to_call_start_ns: Some(120_000),
        p99_accepted_to_terminal_ns: 8_500_000,
        max_rss_bytes: Some(134_217_728),
        cpu_core_seconds: Some(4.25),
        cpu_core_seconds_per_million_acknowledged: Some(35.4),
    }
}

fn medians(name: &str, role: &str, rate: f64) -> SubjectMedians {
    SubjectMedians {
        name: name.to_owned(),
        role: Some(role.to_owned()),
        declared: Some(declared()),
        acknowledged_records_per_second: rate,
        p50_intended_to_terminal_ns: 1_000_000,
        p99_intended_to_terminal_ns: 9_000_000,
        p999_intended_to_terminal_ns: 21_000_000,
        p99_admission_wait_ns: 250_000,
        p99_intended_to_call_start_ns: Some(120_000),
        p99_accepted_to_terminal_ns: 8_500_000,
        max_rss_bytes: Some(134_217_728),
        cpu_core_seconds: Some(4.25),
        cpu_core_seconds_per_million_acknowledged: Some(35.4),
    }
}

fn fixture() -> SuiteSummary {
    SuiteSummary {
        schema: SuiteSummary::SCHEMA.to_owned(),
        experiment_id: ExperimentId::parse(EXPERIMENT).unwrap(),
        scenario_name: "producer-balanced-1k-diagnostic".to_owned(),
        repetitions: 5,
        seed: 44,
        resamples: 10_000,
        practical_threshold: 0.02,
        attempts: vec![SuiteAttempt {
            attempt_id: "20260812T101500Z-0123abcd".to_owned(),
            bundle_digest: "a".repeat(64),
            execution_status: ExecutionStatus::Complete,
            run_valid: true,
            subjects: vec![
                observation("kafkars", SUBJECT_ROLE_HEAD, 120_000.0),
                observation("librdkafka-c", SUBJECT_ROLE_BASE, 100_000.0),
            ],
        }],
        medians: vec![
            medians("kafkars", SUBJECT_ROLE_HEAD, 120_000.0),
            medians("librdkafka-c", SUBJECT_ROLE_BASE, 100_000.0),
        ],
        pairs: vec![PairedRatio {
            numerator_subject: "kafkars".to_owned(),
            denominator_subject: "librdkafka-c".to_owned(),
            metric: "acknowledged_records_per_second".to_owned(),
            ratio_of_medians: 1.2,
            ci_low: 1.14,
            ci_high: 1.27,
        }],
        dispersion: vec![SubjectDispersion {
            name: "kafkars".to_owned(),
            metric: "acknowledged_records_per_second".to_owned(),
            coefficient_of_variation: Some(0.013),
        }],
        gates: vec![GateOutcome {
            name: "dispersion-within-budget".to_owned(),
            description: "no subject's goodput varies by more than 5% across repetitions"
                .to_owned(),
            passed: true,
            detail: "worst coefficient of variation was 1.3%".to_owned(),
        }],
        claim_eligible: false,
        notes: vec!["single-host cluster; not a claim-grade environment".to_owned()],
    }
}

#[test]
fn a_suite_summary_round_trips_through_bytes() {
    let document = fixture();
    document.validate().unwrap();

    let bytes = serde_json::to_vec(&document).unwrap();
    let reparsed = SuiteSummary::from_slice(&bytes).unwrap();

    assert_eq!(reparsed, document);
    assert!(reparsed.has_expected_schema());
}

#[test]
fn absent_options_are_omitted_from_the_bytes() {
    let mut document = fixture();
    for observation in &mut document.attempts[0].subjects {
        observation.role = None;
        observation.declared = None;
        observation.p99_intended_to_call_start_ns = None;
        observation.max_rss_bytes = None;
        observation.cpu_core_seconds = None;
        observation.cpu_core_seconds_per_million_acknowledged = None;
    }
    for median in &mut document.medians {
        median.role = None;
        median.declared = None;
        median.p99_intended_to_call_start_ns = None;
        median.max_rss_bytes = None;
        median.cpu_core_seconds = None;
        median.cpu_core_seconds_per_million_acknowledged = None;
    }
    document.dispersion[0].coefficient_of_variation = None;

    let bytes = serde_json::to_string(&document).unwrap();

    for key in [
        "role",
        "declared",
        "p99_intended_to_call_start_ns",
        "max_rss_bytes",
        "cpu_core_seconds",
        "coefficient_of_variation",
    ] {
        assert!(!bytes.contains(key), "{key} must be omitted when absent");
    }
    // The client-internal percentile is not optional: it is always measured,
    // so it is always written, and a reader never has to decide whether an
    // absent field meant zero.
    assert!(bytes.contains("p99_accepted_to_terminal_ns"), "{bytes}");
    assert_eq!(
        SuiteSummary::from_slice(bytes.as_bytes()).unwrap(),
        document
    );
}

#[test]
fn the_schema_id_is_checked() {
    let mut wrong = fixture();
    wrong.schema = "kafkars.producer-comparison-suite.v1".to_owned();

    assert_eq!(
        wrong.validate().unwrap_err().kind(),
        SchemaErrorKind::WrongSchema
    );
    assert!(!wrong.has_expected_schema());
}

#[test]
fn a_claim_eligible_suite_is_refused() {
    let mut claiming = fixture();
    claiming.claim_eligible = true;

    let error = claiming.validate().unwrap_err();

    assert_eq!(error.kind(), SchemaErrorKind::InvalidField);
    assert!(error.context().starts_with("claim_eligible"), "{error}");
}

#[test]
fn an_unknown_role_anywhere_in_the_suite_is_refused() {
    let mut in_attempt = fixture();
    in_attempt.attempts[0].subjects[1].role = Some("baseline".to_owned());
    assert!(
        in_attempt
            .validate()
            .unwrap_err()
            .context()
            .starts_with("attempts[0].subjects[1].role")
    );

    let mut in_medians = fixture();
    in_medians.medians[0].role = Some("candidate".to_owned());
    assert!(
        in_medians
            .validate()
            .unwrap_err()
            .context()
            .starts_with("medians[0].role")
    );
}

#[test]
fn an_inverted_or_absent_interval_is_refused() {
    let mut inverted = fixture();
    inverted.pairs[0].ci_low = 1.30;
    assert!(
        inverted
            .validate()
            .unwrap_err()
            .context()
            .starts_with("pairs[0]")
    );

    let mut not_a_number = fixture();
    not_a_number.pairs[0].ci_high = f64::NAN;
    assert!(not_a_number.validate().is_err());
}

#[test]
fn a_negative_or_infinite_practical_threshold_is_refused() {
    for threshold in [-0.01, f64::INFINITY, f64::NAN] {
        let mut document = fixture();
        document.practical_threshold = threshold;

        assert!(
            document.validate().is_err(),
            "{threshold} should not be a practical threshold"
        );
    }
}

#[test]
fn an_invalid_attempt_stays_in_the_summary() {
    let mut document = fixture();
    document.attempts[0].run_valid = false;
    document.attempts[0].execution_status = ExecutionStatus::TimedOut;

    document.validate().unwrap();

    let bytes = serde_json::to_string(&document).unwrap();
    assert!(bytes.contains("timed_out"));
}
