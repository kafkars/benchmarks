//! The protocol surface: what this adapter claims, what it declines, how a
//! resolved experiment becomes the legacy argument structs, and the guarantee
//! that both command surfaces emit the same result bytes.
#![expect(clippy::unwrap_used, reason = "test fixtures are exact")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bench_schema::{
    ApplicationSpec, ArrivalModel, BudgetSpec, ClusterSpec, ExperimentKind, LoadMode, PayloadSpec,
    ProducerSpec, ResolvedExperiment, RuntimeBinding, SloSpec, SubjectSpec, TopicPair,
    ValidateReport,
};
use serde::Serialize;

use crate::producer::{RunOutcome, V2_COMPLETION_MODE, V2_OWNERSHIP};
use crate::protocol::{
    ADAPTER_NAME, LEGACY_CLOSED_LOOP_RESULT_SCHEMA, LEGACY_FIXED_RATE_RESULT_SCHEMA, RESULT_SCHEMA,
    description, fixed_arguments, produce_arguments, report, subject_of, utc_rfc3339_millis,
};

const RUN_ID: &str = "0123456789abcdef";
const OUTPUT: &str = "/tmp/bundle/adapters/kafkars";

fn output() -> PathBuf {
    PathBuf::from(OUTPUT)
}

/// The migrated balanced-1k scenario as the resolver produces it.
fn experiment(load_mode: LoadMode) -> ResolvedExperiment {
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
        offered_records_per_second: if fixed { Some(100_000) } else { None },
        arrival: if fixed {
            Some(ArrivalModel::Deterministic)
        } else {
            None
        },
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
            warmup_partitioning: Some("explicit-round-robin".to_owned()),
            warmup_serialized_partition_primer_records: Some(12),
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
            subject("kafkars", "kafkars"),
            subject("librdkafka-c", "librdkafka-c"),
        ],
        runtime: Some(runtime()),
    }
}

fn subject(name: &str, adapter_name: &str) -> SubjectSpec {
    SubjectSpec {
        name: name.to_owned(),
        adapter_name: adapter_name.to_owned(),
        adapter_version: "0.1.0".to_owned(),
        command: vec![format!("target/release/{name}")],
        role: None,
    }
}

fn runtime() -> RuntimeBinding {
    let mut topics = BTreeMap::new();
    for name in ["kafkars", "librdkafka-c"] {
        let measured = format!("kfb-{RUN_ID}-{name}");
        topics.insert(
            name.to_owned(),
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

/// One field mutation applied to the shared fixture.
type Mutation = fn(&mut ResolvedExperiment);

fn declined(mutate: impl FnOnce(&mut ResolvedExperiment)) -> ValidateReport {
    let mut document = experiment(LoadMode::ClosedLoop);
    mutate(&mut document);
    report(&document, Some("kafkars"))
}

fn mentions(report: &ValidateReport, needle: &str) -> bool {
    report.reasons.iter().any(|reason| reason.contains(needle))
}

#[test]
fn the_capability_document_describes_what_this_adapter_configures() {
    let document = description();

    assert!(document.has_expected_schema());
    assert_eq!(document.name, ADAPTER_NAME);
    assert_eq!(document.version, env!("CARGO_PKG_VERSION"));
    assert!(document.capabilities.producer);
    assert!(document.capabilities.idempotence);
    assert!(!document.capabilities.consumer);
    assert!(!document.capabilities.transactions);
    assert!(
        !document.capabilities.tls,
        "the argument surface names no certificate and the client is built plaintext"
    );
    assert_eq!(document.capabilities.compression, vec!["none".to_owned()]);
    assert_eq!(
        document.result_schema(LoadMode::ClosedLoop),
        Some(RESULT_SCHEMA)
    );
    assert_eq!(
        document.result_schema(LoadMode::ScheduledOpenLoopFixedRate),
        Some(RESULT_SCHEMA)
    );
    assert_eq!(
        document.result_schema(LoadMode::ClosedLoop),
        Some("kafkars.producer-benchmark.v2"),
        "the protocol path writes v2 in both load modes; the load mode itself \
         distinguishes them inside the document"
    );
}

#[test]
fn the_capability_document_names_the_modes_a_v2_measurement_declares() {
    // A reader holding a `result.json` next to this adapter's `describe`
    // output should be able to look the measurement's declared execution up in
    // the capability list. If these drift, the two documents describe two
    // different adapters and neither says which one ran.
    let document = description();

    assert!(
        document
            .capabilities
            .completion_modes
            .contains(&V2_COMPLETION_MODE.to_owned()),
        "{:?}",
        document.capabilities.completion_modes
    );
    assert!(
        document
            .capabilities
            .ownership_modes
            .contains(&V2_OWNERSHIP.to_owned()),
        "{:?}",
        document.capabilities.ownership_modes
    );
}

#[test]
fn the_legacy_stdout_verbs_still_name_the_schema_ids_they_always_did() {
    // The protocol path moved to v2; `produce` and `produce-fixed` did not,
    // and the reports they print carry these ids by construction because
    // `producer.rs` and `fixed_report.rs` serialize these very constants.
    // Sealed bundles recorded under the legacy control plane stay readable
    // exactly because this pair never moves.
    assert_eq!(
        LEGACY_CLOSED_LOOP_RESULT_SCHEMA,
        "kafkars.producer-benchmark.v1"
    );
    assert_eq!(
        LEGACY_FIXED_RATE_RESULT_SCHEMA,
        "kafkars.producer-fixed-load.v1"
    );
    assert_ne!(
        RESULT_SCHEMA, LEGACY_CLOSED_LOOP_RESULT_SCHEMA,
        "a different measurement must not answer to the same schema id"
    );
    assert_ne!(RESULT_SCHEMA, LEGACY_FIXED_RATE_RESULT_SCHEMA);
}

#[test]
fn the_balanced_scenario_is_supported_in_both_load_modes() {
    for load_mode in [LoadMode::ClosedLoop, LoadMode::ScheduledOpenLoopFixedRate] {
        let verdict = report(&experiment(load_mode), Some("kafkars"));

        assert!(
            verdict.supported,
            "{load_mode:?} was declined: {:?}",
            verdict.reasons
        );
    }
}

#[test]
fn an_experiment_this_adapter_cannot_configure_is_declined_with_the_field() {
    let cases: [(&str, Mutation); 8] = [
        ("acks", |document| {
            document.producer.as_mut().unwrap().acks = "1".to_owned();
        }),
        ("compression", |document| {
            document.producer.as_mut().unwrap().compression = "gzip".to_owned();
        }),
        ("idempotence", |document| {
            document.producer.as_mut().unwrap().idempotence = false;
        }),
        ("linger_ms", |document| {
            document.producer.as_mut().unwrap().linger_ms = 50;
        }),
        ("request_bytes", |document| {
            document.producer.as_mut().unwrap().request_bytes = 4_194_304;
        }),
        ("max_in_flight_requests_per_broker", |document| {
            document
                .producer
                .as_mut()
                .unwrap()
                .max_in_flight_requests_per_broker = 1;
        }),
        ("retry_backoff_ms", |document| {
            document.producer.as_mut().unwrap().retry_backoff_ms = 250;
        }),
        ("delivery_timeout_ms", |document| {
            document.producer.as_mut().unwrap().delivery_timeout_ms = 1_000;
        }),
    ];

    for (field, mutate) in cases {
        let verdict = declined(mutate);

        assert!(!verdict.supported, "{field} was accepted");
        assert!(
            mentions(&verdict, field),
            "{field} was declined for the wrong reason: {:?}",
            verdict.reasons
        );
    }
}

#[test]
fn a_payload_too_small_for_the_identity_envelope_is_declined() {
    let verdict = declined(|document| document.payload.bytes = 32);

    assert!(!verdict.supported);
    assert!(mentions(&verdict, "identity envelope"));
}

#[test]
fn the_fixed_load_headline_requires_four_callers() {
    let mut document = experiment(LoadMode::ScheduledOpenLoopFixedRate);
    document.application.callers_per_producer = 2;

    let verdict = report(&document, Some("kafkars"));

    assert!(!verdict.supported);
    assert!(mentions(&verdict, "exactly 4 callers"));
}

#[test]
fn the_closed_loop_phase_admits_from_one_caller() {
    let verdict = declined(|document| document.application.callers_per_producer = 4);

    assert!(!verdict.supported);
    assert!(mentions(&verdict, "closed-loop phase"));
}

#[test]
fn a_poisson_arrival_process_is_declined() {
    let mut document = experiment(LoadMode::ScheduledOpenLoopFixedRate);
    document.arrival = Some(ArrivalModel::Poisson);

    let verdict = report(&document, Some("kafkars"));

    assert!(!verdict.supported);
    assert!(mentions(&verdict, "evenly spaced arrivals"));
}

#[test]
fn an_experiment_without_a_runtime_binding_is_declined() {
    let verdict = declined(|document| document.runtime = None);

    assert!(!verdict.supported);
    assert!(mentions(&verdict, "no runtime binding"));
}

#[test]
fn an_incoherent_experiment_is_declined_before_anything_else_is_read() {
    let verdict = declined(|document| document.records = 0);

    assert!(!verdict.supported);
    assert_eq!(verdict.reasons.len(), 1, "{:?}", verdict.reasons);
    assert!(mentions(&verdict, "not coherent"));
}

#[test]
fn the_closed_loop_arguments_are_the_legacy_positionals() {
    let arguments =
        produce_arguments(&experiment(LoadMode::ClosedLoop), "kafkars", &output()).unwrap();

    assert_eq!(arguments.bootstrap, "127.0.0.1:39092,127.0.0.1:39093");
    assert_eq!(
        arguments.warmup_topic,
        "kfb-0123456789abcdef-kafkars-warmup"
    );
    assert_eq!(arguments.topic, "kfb-0123456789abcdef-kafkars");
    assert_eq!(arguments.run_id, RUN_ID);
    assert_eq!(arguments.warmup_records, 1_000);
    assert_eq!(arguments.records, 10_000);
    assert_eq!(arguments.payload_bytes, 1_024);
    assert_eq!(arguments.partitions, 12);
    assert_eq!(arguments.max_outstanding, 8_192);
    assert_eq!(
        arguments.latency_path,
        Path::new("/tmp/bundle/adapters/kafkars/latency.csv"),
        "the latency evidence belongs inside the sealed output directory"
    );
}

#[test]
fn the_fixed_rate_arguments_add_the_rate_and_the_callers() {
    let arguments = fixed_arguments(
        &experiment(LoadMode::ScheduledOpenLoopFixedRate),
        "kafkars",
        &output(),
    )
    .unwrap();

    assert_eq!(arguments.offered_records_per_second, 100_000);
    assert_eq!(arguments.callers, 4);
    assert_eq!(arguments.common.topic, "kfb-0123456789abcdef-kafkars");
}

#[test]
fn a_subject_with_no_topics_cannot_be_translated() {
    let error = produce_arguments(&experiment(LoadMode::ClosedLoop), "sarama", &output())
        .unwrap_err()
        .to_string();

    assert!(error.contains("sarama"), "{error}");
}

#[test]
fn the_subject_is_the_name_of_the_output_directory() {
    let document = experiment(LoadMode::ClosedLoop);

    let subject = subject_of(&document, Some(Path::new("/tmp/b/adapters/librdkafka-c"))).unwrap();

    assert_eq!(subject.name, "librdkafka-c");
}

#[test]
fn without_an_output_directory_the_subject_is_the_only_one_this_adapter_could_be() {
    let document = experiment(LoadMode::ClosedLoop);

    let subject = subject_of(&document, None).unwrap();

    assert_eq!(subject.name, "kafkars");
}

#[test]
fn two_subjects_of_this_adapter_without_an_output_directory_are_ambiguous() {
    let mut document = experiment(LoadMode::ClosedLoop);
    let mut twin = document.subjects[0].clone();
    twin.name = "kafkars-baseline".to_owned();
    document.subjects.push(twin);

    let error = subject_of(&document, None).unwrap_err().to_string();

    assert!(error.contains("more than one"), "{error}");
}

/// A stand-in for a report, chosen so its serialization is checkable by eye.
#[derive(Serialize)]
struct StandInReport {
    schema: &'static str,
    acknowledged_records: u64,
    acknowledged_records_per_second: f64,
    valid: bool,
}

#[test]
fn the_rendered_line_is_exactly_what_the_legacy_arm_always_printed() {
    let report = StandInReport {
        schema: "kafkars.producer-benchmark.v1",
        acknowledged_records: 10_000,
        acknowledged_records_per_second: 1234.5,
        valid: true,
    };

    let outcome = RunOutcome::render(&report, true, "unused").unwrap();

    // The legacy arm was `println!("{}", serde_json::to_string(&report)?)`, so
    // holding this equality is what keeps sealed evidence comparable across the
    // two command surfaces.
    assert_eq!(outcome.json, serde_json::to_string(&report).unwrap());
    assert_eq!(
        outcome.json,
        "{\"schema\":\"kafkars.producer-benchmark.v1\",\"acknowledged_records\":10000,\
         \"acknowledged_records_per_second\":1234.5,\"valid\":true}"
    );
    assert!(
        !outcome.json.ends_with('\n'),
        "the newline belongs to the printer, not the rendering"
    );
}

#[test]
fn the_result_file_is_the_printed_line_plus_the_newline_the_shell_used_to_add() {
    let report = StandInReport {
        schema: "kafkars.producer-benchmark.v1",
        acknowledged_records: 1,
        acknowledged_records_per_second: 1.0,
        valid: true,
    };
    let outcome = RunOutcome::render(&report, true, "unused").unwrap();

    let file_bytes = format!("{}\n", outcome.json);

    assert_eq!(
        file_bytes,
        format!("{}\n", serde_json::to_string(&report).unwrap())
    );
}

#[test]
fn an_invalid_phase_carries_the_message_the_legacy_surface_reports() {
    let outcome = RunOutcome::render(&"ignored", false, "producer phase did not settle").unwrap();

    assert!(!outcome.valid);
    assert_eq!(outcome.invalid_reason, "producer phase did not settle");
}

#[test]
fn status_timestamps_are_utc_with_milliseconds() {
    assert_eq!(
        utc_rfc3339_millis(UNIX_EPOCH),
        "1970-01-01T00:00:00.000Z",
        "the epoch is the cheapest available vector"
    );
    assert_eq!(
        utc_rfc3339_millis(UNIX_EPOCH + Duration::from_millis(1_767_225_600_007)),
        "2026-01-01T00:00:00.007Z"
    );
    assert!(
        utc_rfc3339_millis(SystemTime::now()).ends_with('Z'),
        "the status document's timestamps are UTC by construction"
    );
}
