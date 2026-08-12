//! The field mapping, and every request the document builder refuses.
#![expect(clippy::unwrap_used, reason = "test assertions may unwrap")]

use bench_schema::{
    ArrivalModel, BudgetSpec, ClusterProfile, ExperimentKind, LoadMode, SourceExperiment,
    canonical_bytes, experiment_id,
};

use crate::error::CtlErrorKind;
use crate::resolve::{DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_BROKER, resolve_experiment};

use super::experiment_test::{CLUSTER, SCENARIO, inputs};

#[test]
fn the_scenario_maps_onto_the_resolved_document() {
    let resolved = resolve_experiment(&inputs()).unwrap();

    assert_eq!(resolved.name, "producer-miniature");
    assert_eq!(resolved.kind, ExperimentKind::Producer);
    assert_eq!(
        resolved.profile, "diagnostic",
        "profile is the status field"
    );
    assert!(!resolved.claim_eligible);
    assert_eq!(resolved.load_mode, LoadMode::ClosedLoop);
    assert_eq!(resolved.records, 10_000);
    assert_eq!(resolved.warmup_records, 1_000);
    assert_eq!(resolved.seed, 44, "the seed defaults to the payload seed");
    assert_eq!(resolved.application.admission_shape, "public-batch");
    assert_eq!(resolved.application.batch_records, 256);
    assert_eq!(resolved.payload.bytes, 1_024);
    assert_eq!(resolved.budget, BudgetSpec::default());
    assert_eq!(resolved.cluster.partitions, 12);
    assert!(!resolved.cluster.unclean_leader_election);
    assert!(resolved.slo.is_empty());
    assert_eq!(resolved.offered_records_per_second, None);
    assert_eq!(resolved.arrival, None);
}

#[test]
fn an_unstated_request_concurrency_takes_the_matched_default() {
    let resolved = resolve_experiment(&inputs()).unwrap();

    assert_eq!(
        resolved.producer.unwrap().max_in_flight_requests_per_broker,
        DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_BROKER
    );
}

#[test]
fn the_subjects_carry_what_each_adapter_said_about_itself() {
    let resolved = resolve_experiment(&inputs()).unwrap();

    let subject = resolved.subject("librdkafka-c").unwrap();
    assert_eq!(subject.adapter_name, "librdkafka-c");
    assert_eq!(subject.adapter_version, "2.15.0");
    assert_eq!(subject.command, vec!["target/release/librdkafka-c-adapter"]);
}

#[test]
fn resolution_is_deterministic() {
    let first = canonical_bytes(&resolve_experiment(&inputs()).unwrap()).unwrap();
    let second = canonical_bytes(&resolve_experiment(&inputs()).unwrap()).unwrap();

    assert_eq!(first, second);
}

#[test]
fn the_seed_override_replaces_the_payload_seed_and_changes_the_experiment() {
    let mut inputs = inputs();
    inputs.seed = Some(1_234);

    let overridden = resolve_experiment(&inputs).unwrap();

    assert_eq!(overridden.seed, 1_234);
    assert_ne!(
        experiment_id(&overridden).unwrap(),
        experiment_id(&resolve_experiment(&super::experiment_test::inputs()).unwrap()).unwrap(),
        "a different seed is a different experiment"
    );
}

#[test]
fn a_fixed_rate_scenario_resolves_its_rate_and_its_arrival_process() {
    let mut inputs = inputs();
    inputs.source = SourceExperiment::from_toml_str(&SCENARIO.replace(
        "load_mode = \"closed-loop-capacity-point\"",
        "load_mode = \"scheduled-open-loop-fixed-rate\"\noffered_records_per_second = 60000",
    ))
    .unwrap();

    let resolved = resolve_experiment(&inputs).unwrap();

    assert_eq!(resolved.load_mode, LoadMode::ScheduledOpenLoopFixedRate);
    assert_eq!(resolved.offered_records_per_second, Some(60_000));
    assert_eq!(resolved.arrival, Some(ArrivalModel::Deterministic));
}

#[test]
fn a_capacity_search_is_refused_rather_than_approximated() {
    let mut inputs = inputs();
    inputs.source = SourceExperiment::from_toml_str(&SCENARIO.replace(
        "load_mode = \"closed-loop-capacity-point\"",
        "load_mode = \"scheduled-open-loop-capacity-search\"",
    ))
    .unwrap();

    let error = resolve_experiment(&inputs).unwrap_err();

    assert_eq!(error.kind(), CtlErrorKind::InvalidExperiment);
    assert!(
        error.message().contains("capacity search"),
        "{}",
        error.message()
    );
}

#[test]
fn a_closed_loop_scenario_may_not_state_an_offered_rate() {
    let mut inputs = inputs();
    inputs.source = SourceExperiment::from_toml_str(&SCENARIO.replace(
        "records = 10000",
        "records = 10000\noffered_records_per_second = 60000",
    ))
    .unwrap();

    let error = resolve_experiment(&inputs).unwrap_err();

    assert_eq!(error.kind(), CtlErrorKind::InvalidExperiment);
}

#[test]
fn a_fixed_rate_scenario_without_a_rate_is_refused() {
    let mut inputs = inputs();
    inputs.source = SourceExperiment::from_toml_str(&SCENARIO.replace(
        "load_mode = \"closed-loop-capacity-point\"",
        "load_mode = \"scheduled-open-loop-fixed-rate\"",
    ))
    .unwrap();

    let error = resolve_experiment(&inputs).unwrap_err();

    assert_eq!(error.kind(), CtlErrorKind::InvalidExperiment);
    assert!(
        error.message().contains("offered_records_per_second"),
        "{}",
        error.message()
    );
}

#[test]
fn a_cluster_profile_that_contradicts_the_scenario_is_refused() {
    for contradiction in ["brokers = 1", "security = \"ssl\""] {
        let mut inputs = inputs();
        inputs.cluster =
            ClusterProfile::from_toml_str(&format!("{CLUSTER}{contradiction}\n")).unwrap();

        let error = resolve_experiment(&inputs).unwrap_err();

        assert_eq!(
            error.kind(),
            CtlErrorKind::InvalidExperiment,
            "{contradiction} was accepted"
        );
    }
}
