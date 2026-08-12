//! Tests pinning the lenient views over the documents today's adapters write.
#![expect(
    clippy::unwrap_used,
    reason = "legacy fixtures are exact; a bad one must fail the test immediately"
)]

use crate::{KnownProducerResult, VerifierReport};

const CLOSED_LOOP_RESULT: &str = r#"{
  "schema": "kafkars.producer-benchmark.v1",
  "adapter": "kafkars",
  "adapter_version": "0.1.0",
  "run_id": "0123456789abcdef",
  "topic": "kafkars-bench-0123456789abcdef-kafkars",
  "offered_records": 10000,
  "accepted_records": 10000,
  "acknowledged_records": 10000,
  "failed_records": 0,
  "payload_bytes": 1024,
  "acknowledged_payload_bytes": 10240000,
  "duration_ns": 1234567890,
  "acknowledged_records_per_second": 81000.5,
  "acknowledged_mib_per_second": 79.1,
  "latency_ns": {"p50": 1000, "p95": 2000, "p99": 3000, "p999": 4000, "max": 5000},
  "settings": {"acks": "all", "idempotence": true},
  "native_metrics": {"availability": "captured", "producer_requests": {"requests": 40}},
  "valid": true
}"#;

const FIXED_LOAD_RESULT: &str = r#"{
  "schema": "kafkars.producer-fixed-load.v1",
  "adapter": "librdkafka-c",
  "run_id": "0123456789abcdef",
  "acknowledged_records": 100000,
  "acknowledged_records_per_second": 99999.25,
  "load": {"mode": "scheduled-open-loop-fixed-rate", "offered_records_per_second": 100000},
  "latency_ns": {
    "uncorrected": {"p50": 1, "p95": 2, "p99": 300, "p999": 4, "max": 5},
    "corrected": {"p50": 6, "p95": 7, "p99": 800, "p999": 9, "max": 10},
    "schedule_delay": {"p50": 11, "p95": 12, "p99": 13, "p999": 14, "max": 15}
  },
  "valid": true
}"#;

#[test]
fn a_closed_loop_result_yields_its_known_fields() {
    let result = KnownProducerResult::from_slice(CLOSED_LOOP_RESULT.as_bytes()).unwrap();

    assert!(result.has_known_schema());
    assert!(result.adapter_declared_valid());
    assert_eq!(result.adapter.as_deref(), Some("kafkars"));
    assert_eq!(result.run_id.as_deref(), Some("0123456789abcdef"));
    assert_eq!(result.acknowledged_records, Some(10_000));
    assert_eq!(result.failed_records, Some(0));
    assert_eq!(result.acknowledged_records_per_second, Some(81_000.5));
    assert_eq!(result.headline_p99_ns(), Some(3_000));
}

#[test]
fn a_fixed_load_result_prefers_the_corrected_tail() {
    let result = KnownProducerResult::from_slice(FIXED_LOAD_RESULT.as_bytes()).unwrap();

    assert!(result.has_known_schema());
    assert_eq!(result.headline_p99_ns(), Some(800));
    assert_eq!(
        result.latency_ns.unwrap().schedule_delay.unwrap().p99,
        Some(13)
    );
}

#[test]
fn an_unknown_key_is_ignored_rather_than_refused() {
    let grown = r#"{"schema":"kafkars.producer-benchmark.v1","valid":true,"a_field_from_2027":42}"#;

    let result = KnownProducerResult::from_slice(grown.as_bytes()).unwrap();

    assert!(result.has_known_schema());
    assert!(result.adapter_declared_valid());
}

#[test]
fn a_missing_field_is_absent_rather_than_a_parse_failure() {
    let result = KnownProducerResult::from_slice(b"{}").unwrap();

    assert!(!result.has_known_schema());
    assert!(!result.adapter_declared_valid());
    assert_eq!(result.headline_p99_ns(), None);
    assert_eq!(result, KnownProducerResult::default());
}

#[test]
fn an_unrecognized_schema_is_reported_rather_than_assumed() {
    let result =
        KnownProducerResult::from_slice(br#"{"schema":"kafkars.consumer-benchmark.v1"}"#).unwrap();

    assert!(!result.has_known_schema());
}

#[test]
fn malformed_bytes_are_a_parse_error() {
    assert!(KnownProducerResult::from_slice(b"not json").is_err());
}

#[test]
fn a_verification_document_is_judged_against_what_was_intended() {
    let intact = br#"{
      "schema": "kafkars.producer-verification.v1",
      "topic": "kafkars-bench-0123456789abcdef-kafkars",
      "expected_records": 10000,
      "verified_records": 10000,
      "duplicates": 0,
      "missing_records": 0,
      "corrupt": 0,
      "unexpected": 0,
      "eof_partitions": 12,
      "valid": true
    }"#;

    let report = VerifierReport::from_slice(intact).unwrap();

    assert!(report.has_known_schema());
    assert!(report.satisfies_contract("kafkars-bench-0123456789abcdef-kafkars", 10_000, 12));
    assert!(!report.satisfies_contract("another-topic", 10_000, 12));
    assert!(!report.satisfies_contract("kafkars-bench-0123456789abcdef-kafkars", 9_999, 12));
    assert!(!report.satisfies_contract("kafkars-bench-0123456789abcdef-kafkars", 10_000, 3));
}

#[test]
fn a_verifier_flag_alone_does_not_satisfy_the_contract() {
    let flagged_valid = br#"{
      "schema": "kafkars.producer-verification.v1",
      "topic": "t",
      "expected_records": 10,
      "verified_records": 9,
      "duplicates": 0,
      "missing_records": 1,
      "corrupt": 0,
      "unexpected": 0,
      "eof_partitions": 1,
      "valid": true
    }"#;

    let report = VerifierReport::from_slice(flagged_valid).unwrap();

    assert_eq!(report.valid, Some(true));
    assert!(!report.satisfies_contract("t", 10, 1));
}
