//! Pins the exit-code table: every documented code, both vocabularies, no
//! overlaps between the sealed-outcome range and the abnormal-ending range.
#![expect(clippy::expect_used, reason = "test assertions may use expect_err")]

use bench_schema::ExecutionStatus;

use crate::error::{
    CtlError, CtlErrorKind, EXIT_ATTEMPT_EXISTS, EXIT_INTERNAL, EXIT_INVALID, EXIT_SEAL_WRITE,
    EXIT_SEALED_COMPLETE, EXIT_SEALED_CRASHED, EXIT_SEALED_PARTIAL, EXIT_SEALED_TIMED_OUT,
    EXIT_USAGE, exit_code_for_status,
};

#[test]
fn sealed_status_codes_match_the_table() {
    assert_eq!(
        exit_code_for_status(ExecutionStatus::Complete),
        EXIT_SEALED_COMPLETE
    );
    assert_eq!(
        exit_code_for_status(ExecutionStatus::Partial),
        EXIT_SEALED_PARTIAL
    );
    assert_eq!(
        exit_code_for_status(ExecutionStatus::Crashed),
        EXIT_SEALED_CRASHED
    );
    assert_eq!(
        exit_code_for_status(ExecutionStatus::TimedOut),
        EXIT_SEALED_TIMED_OUT
    );
    assert_eq!(
        [
            EXIT_SEALED_COMPLETE,
            EXIT_SEALED_PARTIAL,
            EXIT_SEALED_CRASHED,
            EXIT_SEALED_TIMED_OUT
        ],
        [0, 20, 21, 22]
    );
}

#[test]
fn error_kind_codes_match_the_table() {
    let cases = [
        (CtlErrorKind::Usage, EXIT_USAGE, 64),
        (CtlErrorKind::InvalidExperiment, EXIT_INVALID, 65),
        (CtlErrorKind::AttemptExists, EXIT_ATTEMPT_EXISTS, 73),
        (CtlErrorKind::Seal, EXIT_SEAL_WRITE, 74),
        (CtlErrorKind::Internal, EXIT_INTERNAL, 70),
    ];
    for (kind, constant, literal) in cases {
        assert_eq!(CtlError::new(kind, "x").exit_code(), constant);
        assert_eq!(constant, literal);
    }
}

#[test]
fn constructors_set_kind_and_message() {
    let error = CtlError::usage("missing --experiment");
    assert_eq!(error.kind(), CtlErrorKind::Usage);
    assert_eq!(error.message(), "missing --experiment");
    assert_eq!(error.to_string(), "usage: missing --experiment");
    assert_eq!(
        CtlError::invalid("x").kind(),
        CtlErrorKind::InvalidExperiment
    );
    assert_eq!(
        CtlError::attempt_exists("x").kind(),
        CtlErrorKind::AttemptExists
    );
    assert_eq!(CtlError::seal("x").kind(), CtlErrorKind::Seal);
    assert_eq!(CtlError::internal("x").kind(), CtlErrorKind::Internal);
}

#[test]
fn schema_errors_become_pre_attempt_invalid() {
    let schema_error = bench_schema::require_schema("wrong", bench_schema::EXPERIMENT_V1)
        .expect_err("mismatched schema must fail");
    let error: CtlError = schema_error.into();
    assert_eq!(error.kind(), CtlErrorKind::InvalidExperiment);
    assert_eq!(error.exit_code(), 65);
}
