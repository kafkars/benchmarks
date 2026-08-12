//! The capability document says what the C sources actually do.
//!
//! The negative assertions matter more than the positive ones. Overclaiming a
//! capability produces a subject that accepts an experiment and then measures a
//! different one, which is the failure mode this whole document exists to
//! prevent.
#![expect(clippy::unwrap_used, reason = "test fixtures are exact")]

use bench_schema::{AdapterDescription, LoadMode, parse_json_slice, pretty_bytes};

use crate::describe::{
    ADAPTER_NAME, CLOSED_LOOP_RESULT_SCHEMA, FIXED_RATE_RESULT_SCHEMA, LIBRDKAFKA_VERSION,
    description,
};

#[test]
fn the_document_names_the_pinned_reference_client() {
    let document = description();

    assert!(document.has_expected_schema());
    assert_eq!(document.name, ADAPTER_NAME);
    assert_eq!(document.name, "librdkafka-c");
    assert_eq!(document.version, LIBRDKAFKA_VERSION);
    assert_eq!(
        document.version, "2.15.0",
        "the version is the pinned anchor, not whatever is installed"
    );
}

#[test]
fn it_claims_only_what_the_c_sources_configure() {
    let capabilities = description().capabilities;

    assert!(capabilities.producer);
    assert!(capabilities.idempotence, "enable.idempotence=true");
    assert!(!capabilities.consumer, "there is no consumer path in C");
    assert!(
        !capabilities.transactions,
        "no transactional API call exists in the C sources"
    );
    assert!(
        !capabilities.tls,
        "the pinned build disables SSL and the argv surface names no certificate"
    );
    assert_eq!(capabilities.compression, vec!["none".to_owned()]);
}

#[test]
fn it_names_a_result_schema_for_each_load_mode_it_runs() {
    let document = description();

    assert_eq!(
        document.result_schema(LoadMode::ClosedLoop),
        Some(CLOSED_LOOP_RESULT_SCHEMA)
    );
    assert_eq!(
        document.result_schema(LoadMode::ScheduledOpenLoopFixedRate),
        Some(FIXED_RATE_RESULT_SCHEMA)
    );
    assert_eq!(
        document.result_schema(LoadMode::ClosedLoop),
        Some("kafkars.producer-benchmark.v1"),
        "report.c prints this schema id"
    );
    assert_eq!(
        document.result_schema(LoadMode::ScheduledOpenLoopFixedRate),
        Some("kafkars.producer-fixed-load.v1"),
        "fixed_report.c prints this schema id"
    );
}

#[test]
fn the_document_round_trips_through_its_sealed_bytes() {
    let document = description();

    let bytes = pretty_bytes(&document).unwrap();
    let parsed: AdapterDescription = parse_json_slice(&bytes).unwrap();

    assert_eq!(parsed, document);
    assert!(bytes.ends_with(b"}\n"));
}
