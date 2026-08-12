//! Tests pinning the execution-status vocabulary, its precedence, and the
//! always-seal shape of the run status document.
#![expect(
    clippy::unwrap_used,
    reason = "status fixtures are exact; a bad one must fail the test immediately"
)]

use crate::{
    AdapterOutcome, ExecutionOrder, ExecutionStatus, ExperimentId, PhaseOutcome, PhaseRecord,
    ProcessExit, RunStatus, SubjectExecution, SubjectVerification, VerificationOutcome,
    canonical_bytes, parse_json_slice, parse_json_str, sha256_hex,
};

#[test]
fn the_execution_status_wire_strings_are_pinned() {
    assert_eq!(ExecutionStatus::Complete.as_str(), "complete");
    assert_eq!(ExecutionStatus::Partial.as_str(), "partial");
    assert_eq!(ExecutionStatus::Crashed.as_str(), "crashed");
    assert_eq!(ExecutionStatus::TimedOut.as_str(), "timed_out");

    for status in [
        ExecutionStatus::Complete,
        ExecutionStatus::Partial,
        ExecutionStatus::Crashed,
        ExecutionStatus::TimedOut,
    ] {
        let encoded = serde_json::to_string(&status).unwrap();
        assert_eq!(encoded, format!("\"{}\"", status.as_str()));
        assert_eq!(
            serde_json::from_str::<ExecutionStatus>(&encoded).unwrap(),
            status
        );
    }
}

#[test]
fn the_execution_status_precedence_is_worst_wins() {
    assert!(ExecutionStatus::TimedOut.severity() > ExecutionStatus::Crashed.severity());
    assert!(ExecutionStatus::Crashed.severity() > ExecutionStatus::Partial.severity());
    assert!(ExecutionStatus::Partial.severity() > ExecutionStatus::Complete.severity());

    assert_eq!(
        ExecutionStatus::Complete.worst(ExecutionStatus::Partial),
        ExecutionStatus::Partial
    );
    assert_eq!(
        ExecutionStatus::Crashed.worst(ExecutionStatus::Partial),
        ExecutionStatus::Crashed
    );
    assert_eq!(
        ExecutionStatus::Crashed.worst(ExecutionStatus::TimedOut),
        ExecutionStatus::TimedOut
    );
    assert_eq!(
        ExecutionStatus::Complete.worst(ExecutionStatus::Complete),
        ExecutionStatus::Complete
    );
}

#[test]
fn the_phase_outcome_wire_strings_are_pinned() {
    assert_eq!(
        serde_json::to_string(&PhaseOutcome::Succeeded).unwrap(),
        r#""succeeded""#
    );
    assert_eq!(
        serde_json::to_string(&PhaseOutcome::Failed).unwrap(),
        r#""failed""#
    );
    assert_eq!(
        serde_json::to_string(&PhaseOutcome::Skipped).unwrap(),
        r#""skipped""#
    );
}

fn sample_status() -> RunStatus {
    RunStatus {
        schema: RunStatus::SCHEMA.to_owned(),
        experiment_id: Some(ExperimentId::parse(&sha256_hex(b"experiment")).unwrap()),
        attempt_id: "20260812T101500Z-0a1b2c3d".to_owned(),
        execution_status: ExecutionStatus::Complete,
        failure_reason: None,
        interrupted: false,
        subjects: vec![SubjectExecution {
            name: "kafkars".to_owned(),
            execution: Some(ProcessExit {
                exit_code: Some(0),
                signal: None,
                timed_out: false,
                duration_ms: 12_345,
            }),
            adapter_outcome: Some(AdapterOutcome::Succeeded),
            result_present: true,
            verification: SubjectVerification {
                measured: Some(VerificationOutcome {
                    ran: true,
                    valid: Some(true),
                    exit_code: Some(0),
                }),
                warmup: None,
            },
        }],
        phases: vec![PhaseRecord {
            name: "Probe".to_owned(),
            outcome: PhaseOutcome::Succeeded,
            detail: None,
        }],
    }
}

#[test]
fn a_run_status_round_trips() {
    let status = sample_status();

    let bytes = canonical_bytes(&status).unwrap();

    assert_eq!(parse_json_slice::<RunStatus>(&bytes).unwrap(), status);
    assert!(status.has_expected_schema());
}

#[test]
fn a_status_without_an_experiment_id_is_representable() {
    // The attempt that dies during probing never learns its experiment id, and
    // still has to seal.
    let mut status = sample_status();
    status.experiment_id = None;
    status.execution_status = ExecutionStatus::Crashed;
    status.failure_reason = Some("adapter describe printed 3 MiB of nothing".to_owned());
    status.subjects.clear();

    let text = String::from_utf8(canonical_bytes(&status).unwrap()).unwrap();

    assert!(!text.contains("experiment_id"), "{text}");
    assert!(text.contains(r#""execution_status":"crashed""#), "{text}");
}

#[test]
fn a_killed_subject_records_its_signal() {
    let exit = ProcessExit {
        exit_code: None,
        signal: Some(9),
        timed_out: true,
        duration_ms: 2_000,
    };

    let text = String::from_utf8(canonical_bytes(&exit).unwrap()).unwrap();

    assert_eq!(text, r#"{"duration_ms":2000,"signal":9,"timed_out":true}"#);
}

#[test]
fn an_execution_order_names_what_decided_it() {
    let order = ExecutionOrder::new(
        vec!["librdkafka-c".to_owned(), "kafkars".to_owned()],
        "run-id-coin-flip",
    );

    let text = String::from_utf8(canonical_bytes(&order).unwrap()).unwrap();

    assert_eq!(
        text,
        r#"{"decided_by":"run-id-coin-flip","order":["librdkafka-c","kafkars"],"schema":"kafkars.execution-order.v1"}"#
    );
    assert!(order.has_expected_schema());
}

#[test]
fn a_skipped_verification_is_not_a_passed_verification() {
    let skipped = VerificationOutcome {
        ran: false,
        valid: None,
        exit_code: None,
    };

    let text = String::from_utf8(canonical_bytes(&skipped).unwrap()).unwrap();

    assert_eq!(text, r#"{"ran":false}"#);
}

#[test]
fn an_unknown_field_is_refused() {
    let error = parse_json_str::<ProcessExit>(r#"{"timed_out":false,"duration_ms":1,"why":2}"#)
        .unwrap_err();

    assert_eq!(error.kind(), crate::SchemaErrorKind::Parse);
}
