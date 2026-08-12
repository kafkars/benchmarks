//! Tests pinning the wire shape of `kafkars.producer-verification.v1`.
#![expect(
    clippy::unwrap_used,
    reason = "test fixtures assert exact document shape and must fail immediately"
)]

use crate::{PRODUCER_VERIFICATION_SCHEMA_V1, VerificationReport};

/// Byte-for-byte the object `adapters/librdkafka-c/verifier.c` prints, minus
/// its trailing newline.
const CLEAN_RUN: &str = concat!(
    r#"{"schema":"kafkars.producer-verification.v1","#,
    r#""topic":"kfb-0123456789abcdef-kafkars","expected_records":2000000,"#,
    r#""verified_records":2000000,"duplicates":0,"#,
    r#""missing_records":0,"corrupt":0,"#,
    r#""unexpected":0,"eof_partitions":6,"valid":true}"#
);

#[test]
fn clean_run_deserializes_every_field() {
    let report: VerificationReport = serde_json::from_str(CLEAN_RUN).unwrap();

    assert_eq!(report.schema, PRODUCER_VERIFICATION_SCHEMA_V1);
    assert_eq!(report.topic, "kfb-0123456789abcdef-kafkars");
    assert_eq!(report.expected_records, 2_000_000);
    assert_eq!(report.verified_records, 2_000_000);
    assert_eq!(report.duplicates, 0);
    assert_eq!(report.missing_records, 0);
    assert_eq!(report.corrupt, 0);
    assert_eq!(report.unexpected, 0);
    assert_eq!(report.eof_partitions, 6);
    assert!(report.valid);
}

#[test]
fn serialization_reproduces_the_verifier_key_order() {
    let report: VerificationReport = serde_json::from_str(CLEAN_RUN).unwrap();

    assert_eq!(serde_json::to_string(&report).unwrap(), CLEAN_RUN);
}

#[test]
fn clean_run_satisfies_the_control_plane_contract() {
    let report: VerificationReport = serde_json::from_str(CLEAN_RUN).unwrap();

    assert!(report.satisfies_contract("kfb-0123456789abcdef-kafkars", 2_000_000, 6));
}

#[test]
fn contract_rejects_a_topic_the_control_plane_did_not_ask_for() {
    let report: VerificationReport = serde_json::from_str(CLEAN_RUN).unwrap();

    assert!(!report.satisfies_contract("kfb-0123456789abcdef-librdkafka-c", 2_000_000, 6));
}

#[test]
fn contract_rejects_a_partition_that_never_reached_end_of_partition() {
    let report: VerificationReport = serde_json::from_str(CLEAN_RUN).unwrap();

    assert!(!report.satisfies_contract("kfb-0123456789abcdef-kafkars", 2_000_000, 7));
}

#[test]
fn losing_run_reports_its_own_verdict() {
    let losing = r#"{"schema":"kafkars.producer-verification.v1",
        "topic":"kfb-0123456789abcdef-kafkars","expected_records":100,
        "verified_records":98,"duplicates":1,"missing_records":2,
        "corrupt":0,"unexpected":1,"eof_partitions":4,"valid":false}"#;

    let report: VerificationReport = serde_json::from_str(losing).unwrap();

    assert!(!report.valid);
    assert_eq!(report.missing_records, 2);
    assert!(!report.satisfies_contract("kfb-0123456789abcdef-kafkars", 100, 4));
}

#[test]
fn a_document_from_a_later_writer_still_parses() {
    let with_extra_key = r#"{"schema":"kafkars.producer-verification.v1",
        "topic":"t","expected_records":1,"verified_records":1,"duplicates":0,
        "missing_records":0,"corrupt":0,"unexpected":0,"eof_partitions":1,
        "valid":true,"verifier_version":"2.15.0"}"#;

    let report: VerificationReport = serde_json::from_str(with_extra_key).unwrap();

    assert!(report.satisfies_contract("t", 1, 1));
}

#[test]
fn a_foreign_schema_id_is_visible_to_callers() {
    let foreign = r#"{"schema":"kafkars.producer-benchmark.v1",
        "topic":"t","expected_records":1,"verified_records":1,"duplicates":0,
        "missing_records":0,"corrupt":0,"unexpected":0,"eof_partitions":1,
        "valid":true}"#;

    let report: VerificationReport = serde_json::from_str(foreign).unwrap();

    assert!(!report.has_expected_schema());
    assert!(!report.satisfies_contract("t", 1, 1));
}
