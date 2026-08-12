//! Tests pinning the resolved experiment document and the cross-field rules it
//! owns, plus the fixture every other experiment test is written against.
//!
//! The rules that belong to one child module are tested next to it, in
//! `experiment/`; what is here is the document as a whole.
#![expect(
    clippy::unwrap_used,
    reason = "experiment fixtures are exact; a bad one must fail the test immediately"
)]

use std::collections::BTreeMap;

use crate::{
    ApplicationSpec, ArrivalModel, BudgetSpec, ClusterSpec, ExperimentKind, LoadMode, PayloadSpec,
    ProducerSpec, ResolvedExperiment, RuntimeBinding, SchemaErrorKind, SloSpec, SubjectSpec,
    TopicPair, canonical_bytes, parse_json_slice,
};

/// A valid closed-loop producer experiment with two subjects and a binding.
///
/// Shared with the identity tests so that both talk about the same document.
pub(crate) fn sample_experiment() -> ResolvedExperiment {
    ResolvedExperiment {
        schema: ResolvedExperiment::SCHEMA.to_owned(),
        name: "producer-balanced-1k-diagnostic".to_owned(),
        kind: ExperimentKind::Producer,
        profile: "diagnostic".to_owned(),
        claim_eligible: false,
        load_mode: LoadMode::ClosedLoop,
        records: 10_000,
        warmup_records: 1_000,
        offered_records_per_second: None,
        arrival: None,
        seed: 44,
        application: ApplicationSpec {
            producer_instances: 1,
            callers_per_producer: 1,
            backpressure: "block-within-original-offer".to_owned(),
            queue_bytes: 67_108_864,
            max_outstanding_records: 8_192,
            admission_shape: "public-batch".to_owned(),
            completion_shape: "aggregate-batch-terminal".to_owned(),
            batch_records: 256,
        },
        payload: PayloadSpec {
            bytes: 1_024,
            profile: "deterministic-ascii-envelope".to_owned(),
            identity: None,
        },
        producer: Some(ProducerSpec {
            acks: "all".to_owned(),
            idempotence: true,
            compression: "none".to_owned(),
            linger_ms: 5,
            batch_records: 256,
            batch_bytes: 65_536,
            request_bytes: 1_048_576,
            delivery_timeout_ms: 60_000,
            partitioning: "explicit-round-robin".to_owned(),
            max_in_flight_requests_per_broker: 5,
            retry_max_replacements: 600,
            retry_backoff_ms: 100,
            warmup_partitioning: None,
            warmup_serialized_partition_primer_records: None,
        }),
        budget: BudgetSpec::default(),
        cluster: ClusterSpec {
            brokers: 3,
            partitions: 12,
            replication_factor: 3,
            min_in_sync_replicas: 2,
            security: "plaintext".to_owned(),
            unclean_leader_election: false,
        },
        slo: SloSpec::default(),
        subjects: vec![
            SubjectSpec {
                name: "kafkars".to_owned(),
                adapter_name: "kafkars".to_owned(),
                adapter_version: "0.1.0".to_owned(),
                command: vec!["target/release/kafkars-benchmark-adapter".to_owned()],
                role: None,
            },
            SubjectSpec {
                name: "librdkafka-c".to_owned(),
                adapter_name: "librdkafka-c".to_owned(),
                adapter_version: "2.15.0".to_owned(),
                command: vec!["target/release/bench-adapter-librdkafka".to_owned()],
                role: None,
            },
        ],
        runtime: Some(sample_runtime()),
    }
}

pub(crate) fn sample_runtime() -> RuntimeBinding {
    let mut topics = BTreeMap::new();
    topics.insert(
        "kafkars".to_owned(),
        TopicPair {
            measured: "kfb-0123456789abcdef-kafkars".to_owned(),
            warmup: "kfb-0123456789abcdef-kafkars-warmup".to_owned(),
        },
    );
    topics.insert(
        "librdkafka-c".to_owned(),
        TopicPair {
            measured: "kfb-0123456789abcdef-librdkafka-c".to_owned(),
            warmup: "kfb-0123456789abcdef-librdkafka-c-warmup".to_owned(),
        },
    );
    RuntimeBinding {
        bootstrap: "127.0.0.1:39092".to_owned(),
        run_id: "0123456789abcdef".to_owned(),
        topic_prefix: "kfb-0123456789abcdef".to_owned(),
        topics,
        execution_order: vec!["kafkars".to_owned(), "librdkafka-c".to_owned()],
    }
}

fn fixed_rate_experiment() -> ResolvedExperiment {
    let mut experiment = sample_experiment();
    experiment.load_mode = LoadMode::ScheduledOpenLoopFixedRate;
    experiment.offered_records_per_second = Some(100_000);
    experiment.arrival = Some(ArrivalModel::Deterministic);
    experiment
}

#[test]
fn the_fixture_is_valid() {
    assert!(sample_experiment().validate().is_ok());
    assert!(fixed_rate_experiment().validate().is_ok());
    assert!(sample_experiment().has_expected_schema());
}

#[test]
fn a_document_round_trips_through_canonical_bytes() {
    let experiment = sample_experiment();

    let bytes = canonical_bytes(&experiment).unwrap();
    let parsed: ResolvedExperiment = parse_json_slice(&bytes).unwrap();

    assert_eq!(parsed, experiment);
}

#[test]
fn an_unknown_field_is_refused() {
    let experiment = sample_experiment();
    let mut value = crate::to_canonical_value(&experiment).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .insert("recrods".to_owned(), serde_json::json!(10));

    let error =
        parse_json_slice::<ResolvedExperiment>(&serde_json::to_vec(&value).unwrap()).unwrap_err();

    assert_eq!(error.kind(), SchemaErrorKind::Parse);
}

#[test]
fn a_wrong_schema_id_is_refused_by_validate() {
    let mut experiment = sample_experiment();
    experiment.schema = "kafkars.experiment.v2".to_owned();

    assert_eq!(
        experiment.validate().unwrap_err().kind(),
        SchemaErrorKind::WrongSchema
    );
    assert!(!experiment.has_expected_schema());
}

#[test]
fn a_claim_eligible_experiment_is_refused() {
    let mut experiment = sample_experiment();
    experiment.claim_eligible = true;

    let error = experiment.validate().unwrap_err();

    assert_eq!(error.kind(), SchemaErrorKind::InvalidField);
    assert!(error.context().starts_with("claim_eligible"));
}

#[test]
fn a_fixed_rate_experiment_requires_a_rate_and_an_arrival_process() {
    let mut missing_rate = fixed_rate_experiment();
    missing_rate.offered_records_per_second = None;
    assert!(
        missing_rate
            .validate()
            .unwrap_err()
            .context()
            .starts_with("offered_records_per_second")
    );

    let mut zero_rate = fixed_rate_experiment();
    zero_rate.offered_records_per_second = Some(0);
    assert!(zero_rate.validate().is_err());

    let mut missing_arrival = fixed_rate_experiment();
    missing_arrival.arrival = None;
    assert!(
        missing_arrival
            .validate()
            .unwrap_err()
            .context()
            .starts_with("arrival")
    );
}

#[test]
fn a_closed_loop_experiment_refuses_a_rate_and_an_arrival_process() {
    let mut with_rate = sample_experiment();
    with_rate.offered_records_per_second = Some(100_000);
    assert!(with_rate.validate().is_err());

    let mut with_arrival = sample_experiment();
    with_arrival.arrival = Some(ArrivalModel::Deterministic);
    assert!(with_arrival.validate().is_err());
}

#[test]
fn a_producer_experiment_requires_a_producer_section() {
    let mut experiment = sample_experiment();
    experiment.producer = None;

    let error = experiment.validate().unwrap_err();

    assert_eq!(error.kind(), SchemaErrorKind::InvalidField);
    assert!(error.context().starts_with("producer"));
}

#[test]
fn a_measured_phase_of_zero_records_is_refused() {
    let mut experiment = sample_experiment();
    experiment.records = 0;

    assert!(experiment.validate().is_err());
}

#[test]
fn a_warmup_of_zero_records_is_allowed() {
    let mut experiment = sample_experiment();
    experiment.warmup_records = 0;

    assert!(experiment.validate().is_ok());
}

#[test]
fn the_cluster_shape_must_be_internally_consistent() {
    let mut too_few_replicas = sample_experiment();
    too_few_replicas.cluster.min_in_sync_replicas = 4;
    assert!(too_few_replicas.validate().is_err());

    let mut no_partitions = sample_experiment();
    no_partitions.cluster.partitions = 0;
    assert!(no_partitions.validate().is_err());
}

#[test]
fn a_subject_is_found_by_name() {
    let experiment = sample_experiment();

    assert_eq!(
        experiment.subject("librdkafka-c").unwrap().adapter_version,
        "2.15.0"
    );
    assert!(experiment.subject("nobody").is_none());
}
