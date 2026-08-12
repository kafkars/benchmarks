//! Tests parsing the real scenario files from `scenarios/producer/`.
//!
//! These read from disk on purpose. A copy of a scenario pasted into a test is
//! a copy that stops being the scenario the harness runs; the point of the
//! check is that the committed files and these types cannot drift apart.
#![expect(
    clippy::unwrap_used,
    reason = "scenario fixtures are committed files; a parse failure must fail the test"
)]

use std::fs;
use std::path::PathBuf;

use crate::{
    ClusterProfile, LoadMode, SchemaErrorKind, SourceExperiment, SourceLoadMode, SubjectsFile,
};

fn scenario(name: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("scenarios")
        .join("producer")
        .join(name);
    fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()))
}

#[test]
fn the_legacy_closed_loop_scenario_parses() {
    let source = SourceExperiment::from_toml_str(&scenario("legacy-balanced-1k.toml")).unwrap();

    assert_eq!(source.name, "producer-balanced-1k-diagnostic");
    assert_eq!(source.status, "diagnostic");
    assert!(!source.claim_eligible);
    assert_eq!(source.load_mode, SourceLoadMode::ClosedLoopCapacityPoint);
    assert_eq!(source.records, Some(10_000));
    assert_eq!(source.warmup_records, Some(1_000));
    assert_eq!(source.offered_records_per_second, None);
    assert_eq!(source.payload.bytes, 1_024);
    assert_eq!(source.payload.seed, 44);
    assert_eq!(
        source.payload.identity.as_deref(),
        Some("KFB1 plus 16-byte run ID plus 64-bit sequence")
    );
    assert_eq!(source.application.callers_per_producer, 1);
    assert_eq!(source.application_api.batch_records, 256);
    assert_eq!(source.producer.acks, "all");
    assert_eq!(source.producer.max_in_flight_requests_per_broker, None);
    assert_eq!(
        source.producer.warmup_serialized_partition_primer_records,
        Some(12)
    );
    assert_eq!(source.cluster.partitions, 12);
    assert_eq!(source.cluster.unclean_leader_election, Some(false));
    assert!(source.validity.unwrap().require_complete_drain);
    assert_eq!(
        source.native_request_concurrency.unwrap().status,
        "matched-configured"
    );
    assert!(source.search.is_none());
    assert!(source.slo.is_none());
}

#[test]
fn the_legacy_fixed_rate_scenario_parses() {
    let source =
        SourceExperiment::from_toml_str(&scenario("legacy-balanced-1k-fixed.toml")).unwrap();

    assert_eq!(source.name, "producer-balanced-1k-fixed-diagnostic");
    assert_eq!(source.load_mode, SourceLoadMode::ScheduledOpenLoopFixedRate);
    assert_eq!(source.records, Some(100_000));
    assert_eq!(source.warmup_records, Some(10_000));
    assert_eq!(source.offered_records_per_second, Some(100_000));
    assert_eq!(source.application.callers_per_producer, 4);
    assert_eq!(source.producer.warmup_partitioning, None);
    assert!(source.validity.is_none());
    assert!(source.native_request_concurrency.is_none());
}

#[test]
fn the_legacy_reference_capacity_scenario_parses() {
    let source =
        SourceExperiment::from_toml_str(&scenario("legacy-balanced-1k-reference-capacity.toml"))
            .unwrap();

    assert_eq!(
        source.load_mode,
        SourceLoadMode::ScheduledOpenLoopCapacitySearch
    );
    assert_eq!(source.records, None);
    assert_eq!(source.window_seconds, Some(10));
    assert_eq!(source.warmup_seconds, Some(2));
    assert_eq!(source.repetitions_per_rate, Some(5));
    assert_eq!(source.producer.max_in_flight_requests_per_broker, Some(5));

    let search = source.search.unwrap();
    assert_eq!(search.initial_records_per_second, 25_000);
    assert_eq!(search.derived_fixed_load_percentages, vec![25, 50, 75, 90]);

    let slo = source.slo.unwrap();
    assert_eq!(slo.corrected_p99_ms, Some(250));
    assert_eq!(slo.native_timeouts, Some(0));
}

#[test]
fn the_scenario_load_modes_map_onto_the_resolved_vocabulary() {
    assert_eq!(
        SourceLoadMode::ClosedLoopCapacityPoint.resolved().unwrap(),
        LoadMode::ClosedLoop
    );
    assert_eq!(
        SourceLoadMode::ScheduledOpenLoopFixedRate
            .resolved()
            .unwrap(),
        LoadMode::ScheduledOpenLoopFixedRate
    );

    let error = SourceLoadMode::ScheduledOpenLoopCapacitySearch
        .resolved()
        .unwrap_err();

    assert_eq!(error.kind(), SchemaErrorKind::InvalidField);
    assert!(error.context().contains("capacity search"), "{error}");
}

#[test]
fn the_scenario_load_mode_wire_strings_are_pinned() {
    assert_eq!(
        SourceLoadMode::ClosedLoopCapacityPoint.as_str(),
        "closed-loop-capacity-point"
    );
    assert_eq!(
        serde_json::to_string(&SourceLoadMode::ScheduledOpenLoopCapacitySearch).unwrap(),
        r#""scheduled-open-loop-capacity-search""#
    );
}

#[test]
fn a_mistyped_scenario_key_is_refused() {
    let error = SourceExperiment::from_toml_str("name = \"x\"\nrecrods = 1\n").unwrap_err();

    assert_eq!(error.kind(), SchemaErrorKind::Parse);
    assert!(error.context().starts_with("scenario:"), "{error}");
}

#[test]
fn a_subjects_file_parses() {
    let subjects = SubjectsFile::from_toml_str(
        r#"
[[subjects]]
name = "kafkars"
command = ["target/release/kafkars-benchmark-adapter"]

[[subjects]]
name = "librdkafka-c"
command = ["target/release/bench-adapter-librdkafka", "--binary", "target/librdkafka"]
"#,
    )
    .unwrap();

    assert_eq!(subjects.subjects.len(), 2);
    assert_eq!(subjects.subjects[0].name, "kafkars");
    assert_eq!(subjects.subjects[1].command.len(), 3);
    assert!(subjects.subjects[0].role.is_none());
    assert!(subjects.validate().is_ok());
}

#[test]
fn a_subjects_file_carries_roles_when_it_declares_them() {
    let subjects = SubjectsFile::from_toml_str(
        r#"
[[subjects]]
name = "kafkars"
command = ["target/release/kafkars-benchmark-adapter"]
role = "head"

[[subjects]]
name = "librdkafka-c"
command = ["target/release/bench-adapter-librdkafka"]
role = "base"

[[subjects]]
name = "anchor-c"
command = ["target/release/bench-adapter-librdkafka"]
role = "anchor"
"#,
    )
    .unwrap();

    subjects.validate().unwrap();
    assert_eq!(subjects.subjects[0].role.as_deref(), Some("head"));
    assert_eq!(subjects.subjects[1].role.as_deref(), Some("base"));
    assert_eq!(subjects.subjects[2].role.as_deref(), Some("anchor"));
}

#[test]
fn a_subjects_file_with_an_unknown_role_is_refused() {
    let subjects = SubjectsFile::from_toml_str(
        r#"
[[subjects]]
name = "kafkars"
command = ["target/release/kafkars-benchmark-adapter"]
role = "candidate"
"#,
    )
    .unwrap();

    let error = subjects.validate().unwrap_err();

    assert_eq!(error.kind(), SchemaErrorKind::InvalidField);
    assert!(error.context().starts_with("subjects[0].role"), "{error}");
}

#[test]
fn an_empty_subjects_file_parses_to_no_subjects() {
    assert_eq!(
        SubjectsFile::from_toml_str("").unwrap(),
        SubjectsFile::default()
    );
}

#[test]
fn a_cluster_profile_parses_with_and_without_tools() {
    let bare = ClusterProfile::from_toml_str(
        r#"
name = "dev-compose"
bootstrap = "127.0.0.1:39092"
"#,
    )
    .unwrap();

    assert_eq!(bare.name, "dev-compose");
    assert_eq!(bare.broker_version, None);
    assert!(bare.tools.is_empty());

    let equipped = ClusterProfile::from_toml_str(
        r#"
name = "dev-compose"
bootstrap = "127.0.0.1:39092,127.0.0.1:39093"
broker_version = "4.3.1"
lifecycle = "externally managed by the caller"

[tools]
topic_create = ["target/release/kafkars-benchmark-adapter", "topics-create"]
topic_delete = ["target/release/kafkars-benchmark-adapter", "topics-delete"]
verify = ["target/librdkafka-benchmark-verifier"]
"#,
    )
    .unwrap();

    assert!(!equipped.tools.is_empty());
    assert_eq!(equipped.tools.topic_create.len(), 2);
    assert_eq!(equipped.tools.verify.len(), 1);
    assert_eq!(equipped.broker_version.as_deref(), Some("4.3.1"));
}

#[test]
fn a_mistyped_cluster_profile_key_is_refused() {
    let error = ClusterProfile::from_toml_str("name = \"x\"\nbootstrap = \"y\"\nbrokerz = 3\n")
        .unwrap_err();

    assert!(error.context().starts_with("cluster profile:"), "{error}");
}
