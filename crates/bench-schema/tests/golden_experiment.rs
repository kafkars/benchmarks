//! Golden bytes for the resolved experiment derived from the migrated
//! `scenarios/producer/legacy-balanced-1k.toml` scenario.
//!
//! Three artefacts are pinned, and they answer three different questions.
//!
//! - `golden/resolved-experiment.pretty.json` is what
//!   `experiment.resolved.json` looks like inside a sealed bundle: the whole
//!   document, two-space indented, newline terminated. A diff here is a change
//!   to what the bundle *contains*.
//! - `golden/resolved-experiment.canon.json` is the exact byte string the
//!   experiment id is the digest of: canonical, compact, key-sorted, with the
//!   runtime binding and every subject command removed. A diff here is a change
//!   to what "the same experiment" *means*.
//! - [`BALANCED_1K_EXPERIMENT_ID`] is the digest of those bytes. A change here
//!   without a change to the file above is impossible; a change to both is a
//!   schema-affecting change and must be treated as one, because every historic
//!   bundle keeps the old id forever.
//!
//! Set `BENCH_SCHEMA_BLESS=1` to rewrite the two files from the fixture. That
//! is an explicit act with a diff, which is the point; nothing rewrites them
//! implicitly.
#![expect(
    clippy::unwrap_used,
    reason = "golden fixtures are exact; a bad one must fail the test immediately"
)]

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use bench_schema::{
    ApplicationSpec, BudgetSpec, ClusterSpec, ExperimentKind, LoadMode, PayloadSpec, ProducerSpec,
    ResolvedExperiment, RuntimeBinding, SloSpec, SubjectSpec, TopicPair, canonical_bytes,
    experiment_id, identity_bytes, pretty_bytes, sha256_hex,
};
use serde::Serialize;

/// Experiment id of the balanced-1k fixture below.
///
/// Pinned rather than computed so that a change to canonicalization, to the
/// identity exclusions, or to the document shape fails here instead of silently
/// renaming every experiment in the results tree.
const BALANCED_1K_EXPERIMENT_ID: &str =
    "d2932f88ad348028796b43b929f2fca826b683d19f13c0d1ad30d676b25dac15";

fn golden_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
        .join(name)
}

fn golden_bytes(name: &str, produced: &[u8]) -> Vec<u8> {
    let path = golden_path(name);
    if std::env::var_os("BENCH_SCHEMA_BLESS").is_some() {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, produced).unwrap();
    }
    fs::read(&path).unwrap_or_else(|error| {
        panic!(
            "cannot read {}: {error}; run with BENCH_SCHEMA_BLESS=1 to create it",
            path.display()
        )
    })
}

/// The balanced-1k diagnostic scenario, resolved and bound to one attempt.
///
/// Every value is taken from `scenarios/producer/legacy-balanced-1k.toml`
/// except the four the resolver supplies: the run identity, the topics, the
/// execution order, and the budget.
fn balanced_1k() -> ResolvedExperiment {
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
            SubjectSpec {
                name: "kafkars".to_owned(),
                adapter_name: "kafkars".to_owned(),
                adapter_version: "0.1.0".to_owned(),
                command: vec![
                    "target/release/kafkars-benchmark-adapter".to_owned(),
                    "--adapter-protocol".to_owned(),
                ],
            },
            SubjectSpec {
                name: "librdkafka-c".to_owned(),
                adapter_name: "librdkafka-c".to_owned(),
                adapter_version: "2.15.0".to_owned(),
                command: vec![
                    "target/release/bench-adapter-librdkafka".to_owned(),
                    "--binary".to_owned(),
                    "target/librdkafka/librdkafka-producer-benchmark".to_owned(),
                ],
            },
        ],
        runtime: Some(RuntimeBinding {
            bootstrap: "127.0.0.1:39092,127.0.0.1:39093,127.0.0.1:39094".to_owned(),
            run_id: "0123456789abcdef".to_owned(),
            topic_prefix: "kfb-0123456789abcdef".to_owned(),
            topics: topics(),
            execution_order: vec!["kafkars".to_owned(), "librdkafka-c".to_owned()],
        }),
    }
}

fn topics() -> BTreeMap<String, TopicPair> {
    let mut topics = BTreeMap::new();
    for subject in ["kafkars", "librdkafka-c"] {
        topics.insert(
            subject.to_owned(),
            TopicPair {
                measured: format!("kfb-0123456789abcdef-{subject}"),
                warmup: format!("kfb-0123456789abcdef-{subject}-warmup"),
            },
        );
    }
    topics
}

#[test]
fn the_fixture_is_a_valid_experiment() {
    balanced_1k().validate().unwrap();
}

#[test]
fn the_pretty_bytes_match_the_golden_document() {
    let produced = pretty_bytes(&balanced_1k()).unwrap();

    let expected = golden_bytes("resolved-experiment.pretty.json", &produced);

    assert_eq!(
        String::from_utf8(produced).unwrap(),
        String::from_utf8(expected).unwrap()
    );
}

#[test]
fn the_golden_document_is_two_space_indented_and_newline_terminated() {
    let produced = pretty_bytes(&balanced_1k()).unwrap();
    let text =
        String::from_utf8(golden_bytes("resolved-experiment.pretty.json", &produced)).unwrap();

    assert!(
        text.starts_with("{\n  \"application\": {\n    \"admission_shape\""),
        "{text}"
    );
    assert!(text.ends_with("}\n"));
    assert!(!text.contains('\r'), "evidence bytes are LF only");
}

#[test]
fn the_golden_document_parses_back_into_the_fixture() {
    let produced = pretty_bytes(&balanced_1k()).unwrap();
    let expected = golden_bytes("resolved-experiment.pretty.json", &produced);

    let parsed: ResolvedExperiment = bench_schema::parse_json_slice(&expected).unwrap();

    assert_eq!(parsed, balanced_1k());
}

#[test]
fn the_identity_bytes_match_the_golden_canonical_form() {
    let produced = identity_bytes(&balanced_1k()).unwrap();

    let expected = golden_bytes("resolved-experiment.canon.json", &produced);

    assert_eq!(
        String::from_utf8(produced).unwrap(),
        String::from_utf8(expected).unwrap()
    );
}

#[test]
fn the_experiment_id_is_the_pinned_digest_of_the_golden_canonical_form() {
    let produced = identity_bytes(&balanced_1k()).unwrap();
    let expected = golden_bytes("resolved-experiment.canon.json", &produced);

    assert_eq!(sha256_hex(&expected), BALANCED_1K_EXPERIMENT_ID);
    assert_eq!(
        experiment_id(&balanced_1k()).unwrap().as_str(),
        BALANCED_1K_EXPERIMENT_ID
    );
    assert_eq!(
        experiment_id(&balanced_1k()).unwrap().short(),
        &BALANCED_1K_EXPERIMENT_ID[..16]
    );
}

#[test]
fn rebinding_the_runtime_keeps_the_experiment_id() {
    let mut rebound = balanced_1k();
    if let Some(runtime) = rebound.runtime.as_mut() {
        runtime.bootstrap = "broker-1.internal:9092".to_owned();
        runtime.run_id = "fedcba9876543210".to_owned();
        runtime.topic_prefix = "kfb-fedcba9876543210".to_owned();
        runtime.execution_order.reverse();
        runtime.topics = topics()
            .into_keys()
            .map(|subject| {
                let measured = format!("kfb-fedcba9876543210-{subject}");
                let warmup = format!("{measured}-warmup");
                (subject, TopicPair { measured, warmup })
            })
            .collect();
    }

    let mut unbound = balanced_1k();
    unbound.runtime = None;

    assert_eq!(
        experiment_id(&rebound).unwrap().as_str(),
        BALANCED_1K_EXPERIMENT_ID
    );
    assert_eq!(
        experiment_id(&unbound).unwrap().as_str(),
        BALANCED_1K_EXPERIMENT_ID
    );
}

#[test]
fn moving_a_subject_binary_keeps_the_experiment_id() {
    let mut moved = balanced_1k();
    moved.subjects[0].command = vec!["/opt/kafka-benchmarks/kafkars-adapter".to_owned()];
    moved.subjects[1].command = vec!["/opt/kafka-benchmarks/librdkafka-shim".to_owned()];

    assert_eq!(
        experiment_id(&moved).unwrap().as_str(),
        BALANCED_1K_EXPERIMENT_ID
    );
}

#[test]
fn changing_the_seed_changes_the_experiment_id() {
    let mut reseeded = balanced_1k();
    reseeded.seed = 45;

    assert_ne!(
        experiment_id(&reseeded).unwrap().as_str(),
        BALANCED_1K_EXPERIMENT_ID
    );
}

/// A struct whose fields are declared in an order no sort would produce.
#[derive(Serialize)]
struct UnsortedDeclaration {
    zulu: u32,
    alpha: u32,
    mike: u32,
}

#[test]
fn canonical_bytes_are_sorted_even_in_an_integration_build() {
    // The same guard the unit tests carry, repeated here because a test binary
    // links its own feature-unified dependency graph: if anything in this
    // workspace ever turns on `serde_json/preserve_order`, the goldens above
    // would move and this is the test that says why.
    let bytes = canonical_bytes(&UnsortedDeclaration {
        zulu: 1,
        alpha: 2,
        mike: 3,
    })
    .unwrap();

    assert_eq!(
        String::from_utf8(bytes).unwrap(),
        r#"{"alpha":2,"mike":3,"zulu":1}"#
    );
}
