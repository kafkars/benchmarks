//! Tests pinning the adapter protocol's three documents.
#![expect(
    clippy::unwrap_used,
    reason = "protocol fixtures are exact; a bad one must fail the test immediately"
)]

use std::collections::BTreeMap;

use crate::{
    AdapterCapabilities, AdapterDescription, AdapterOutcome, AdapterStatus, LoadMode,
    PRODUCER_BENCHMARK_V1, PRODUCER_FIXED_LOAD_V1, ValidateReport, canonical_bytes,
    parse_json_slice, parse_json_str,
};

fn description() -> AdapterDescription {
    let mut result_schemas = BTreeMap::new();
    result_schemas.insert(LoadMode::ClosedLoop, PRODUCER_BENCHMARK_V1.to_owned());
    result_schemas.insert(
        LoadMode::ScheduledOpenLoopFixedRate,
        PRODUCER_FIXED_LOAD_V1.to_owned(),
    );
    AdapterDescription {
        schema: AdapterDescription::SCHEMA.to_owned(),
        name: "kafkars".to_owned(),
        version: "0.1.0".to_owned(),
        capabilities: AdapterCapabilities {
            producer: true,
            consumer: false,
            idempotence: true,
            transactions: false,
            tls: false,
            compression: vec!["none".to_owned()],
            completion_modes: vec!["aggregate-batch-terminal".to_owned()],
            ownership_modes: vec!["public-batch".to_owned()],
            metric_families: vec!["producer_requests".to_owned()],
        },
        result_schemas: Some(result_schemas),
    }
}

#[test]
fn the_schema_ids_are_the_registered_ones() {
    assert_eq!(AdapterDescription::SCHEMA, "kafkars.adapter.v1");
    assert_eq!(ValidateReport::SCHEMA, "kafkars.adapter-validate.v1");
    assert_eq!(AdapterStatus::SCHEMA, "kafkars.adapter-status.v1");
    assert!(description().has_expected_schema());
    assert!(ValidateReport::supported().has_expected_schema());
    assert!(AdapterStatus::succeeded("a", "b").has_expected_schema());
}

#[test]
fn a_description_round_trips_with_load_mode_keyed_result_schemas() {
    let bytes = canonical_bytes(&description()).unwrap();
    let text = String::from_utf8(bytes.clone()).unwrap();

    assert!(
        text.contains(r#""closed-loop":"kafkars.producer-benchmark.v1""#),
        "{text}"
    );

    let parsed: AdapterDescription = parse_json_slice(&bytes).unwrap();

    assert_eq!(parsed, description());
    assert_eq!(
        parsed.result_schema(LoadMode::ScheduledOpenLoopFixedRate),
        Some(PRODUCER_FIXED_LOAD_V1)
    );
}

#[test]
fn a_description_without_result_schemas_omits_the_key() {
    let mut description = description();
    description.result_schemas = None;

    let text = String::from_utf8(canonical_bytes(&description).unwrap()).unwrap();

    assert!(!text.contains("result_schemas"), "{text}");
    assert_eq!(description.result_schema(LoadMode::ClosedLoop), None);
}

#[test]
fn the_adapter_outcome_wire_strings_are_pinned() {
    assert_eq!(AdapterOutcome::Succeeded.as_str(), "succeeded");
    assert_eq!(AdapterOutcome::Failed.as_str(), "failed");
    assert_eq!(
        serde_json::to_string(&AdapterOutcome::Succeeded).unwrap(),
        r#""succeeded""#
    );
    assert_eq!(
        serde_json::to_string(&AdapterOutcome::Failed).unwrap(),
        r#""failed""#
    );
    assert_eq!(
        serde_json::from_str::<AdapterOutcome>(r#""failed""#).unwrap(),
        AdapterOutcome::Failed
    );
    assert!(serde_json::from_str::<AdapterOutcome>(r#""Failed""#).is_err());
}

#[test]
fn a_successful_status_carries_no_failure() {
    let status = AdapterStatus::succeeded("2026-08-12T00:00:00Z", "2026-08-12T00:00:10Z");

    let text = String::from_utf8(canonical_bytes(&status).unwrap()).unwrap();

    assert_eq!(
        text,
        r#"{"finished_at":"2026-08-12T00:00:10Z","outcome":"succeeded","schema":"kafkars.adapter-status.v1","started_at":"2026-08-12T00:00:00Z"}"#
    );
}

#[test]
fn a_failed_status_names_its_stage_and_reason() {
    let status = AdapterStatus::failed(
        "warmup",
        "broker refused the connection",
        "2026-08-12T00:00:00Z",
        "2026-08-12T00:00:01Z",
    );

    assert_eq!(status.outcome, AdapterOutcome::Failed);
    let failure = status.failure.clone().unwrap();
    assert_eq!(failure.stage, "warmup");
    assert_eq!(failure.reason, "broker refused the connection");

    let bytes = canonical_bytes(&status).unwrap();
    assert_eq!(parse_json_slice::<AdapterStatus>(&bytes).unwrap(), status);
}

#[test]
fn a_validate_report_states_its_reasons() {
    let supported = ValidateReport::supported();
    assert!(supported.supported);
    assert!(supported.reasons.is_empty());

    let declined = ValidateReport::unsupported(vec!["compression lz4 is not supported".to_owned()]);
    assert!(!declined.supported);
    assert_eq!(declined.reasons.len(), 1);

    let text = String::from_utf8(canonical_bytes(&declined).unwrap()).unwrap();
    assert!(text.contains(r#""supported":false"#), "{text}");
}

#[test]
fn an_unknown_field_is_refused_in_an_authored_document() {
    let error = parse_json_str::<ValidateReport>(
        r#"{"schema":"kafkars.adapter-validate.v1","supported":true,"reasons":[],"extra":1}"#,
    )
    .unwrap_err();

    assert_eq!(error.kind(), crate::SchemaErrorKind::Parse);
}
