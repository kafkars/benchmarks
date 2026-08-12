//! One resolved experiment the whole test suite shares.
//!
//! It is a faithful copy of the migrated `legacy-balanced-1k` scenario as the
//! resolver produces it — the same records, payload, partitions, and client
//! settings — bound to a fixed attempt so that argument vectors are exact
//! strings rather than patterns. The two load modes differ only where the
//! scenario pair differs: the fixed-rate variant states a rate and asks for the
//! four callers its shape demands.

use std::collections::BTreeMap;

use bench_schema::{
    ApplicationSpec, ArrivalModel, BudgetSpec, ClusterSpec, ExperimentKind, LoadMode, PayloadSpec,
    ProducerSpec, ResolvedExperiment, RuntimeBinding, SloSpec, SubjectSpec, TopicPair,
};

/// The output directory the control plane would hand this adapter.
pub(crate) const OUTPUT: &str = "/tmp/bundle/adapters/librdkafka-c";

/// The run id every fixture topic is named from.
pub(crate) const RUN_ID: &str = "0123456789abcdef";

/// Returns the shared experiment in one of its two load modes.
pub(crate) fn experiment(load_mode: LoadMode) -> ResolvedExperiment {
    let fixed = load_mode == LoadMode::ScheduledOpenLoopFixedRate;
    ResolvedExperiment {
        schema: ResolvedExperiment::SCHEMA.to_owned(),
        name: "producer-balanced-1k-diagnostic".to_owned(),
        kind: ExperimentKind::Producer,
        profile: "diagnostic".to_owned(),
        claim_eligible: false,
        load_mode,
        records: 10_000,
        warmup_records: 1_000,
        offered_records_per_second: fixed.then_some(100_000),
        arrival: fixed.then_some(ArrivalModel::Deterministic),
        seed: 44,
        application: ApplicationSpec {
            producer_instances: 1,
            callers_per_producer: if fixed { 4 } else { 1 },
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
            identity: Some("KFB1 plus 16-byte run ID plus 64-bit sequence".to_owned()),
        },
        producer: Some(producer()),
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
            subject("kafkars", "kafkars", "0.1.0"),
            subject("librdkafka-c", "librdkafka-c", "2.15.0"),
        ],
        runtime: Some(runtime()),
    }
}

/// The client configuration the C program hard-codes, stated explicitly.
pub(crate) fn producer() -> ProducerSpec {
    ProducerSpec {
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
        warmup_partitioning: Some("explicit-round-robin".to_owned()),
        warmup_serialized_partition_primer_records: Some(12),
    }
}

fn subject(name: &str, adapter_name: &str, version: &str) -> SubjectSpec {
    SubjectSpec {
        name: name.to_owned(),
        adapter_name: adapter_name.to_owned(),
        adapter_version: version.to_owned(),
        command: vec![format!("target/release/{name}")],
        role: None,
    }
}

fn runtime() -> RuntimeBinding {
    let mut topics = BTreeMap::new();
    for subject in ["kafkars", "librdkafka-c"] {
        let measured = format!("kfb-{RUN_ID}-{subject}");
        topics.insert(
            subject.to_owned(),
            TopicPair {
                warmup: format!("{measured}-warmup"),
                measured,
            },
        );
    }
    RuntimeBinding {
        bootstrap: "127.0.0.1:39092,127.0.0.1:39093".to_owned(),
        run_id: RUN_ID.to_owned(),
        topic_prefix: format!("kfb-{RUN_ID}"),
        topics,
        execution_order: vec!["kafkars".to_owned(), "librdkafka-c".to_owned()],
    }
}
