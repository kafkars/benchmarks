//! The two passes and the subjects lock: what a lock records, and the two ways
//! a subject stops one from being built.
//!
//! The fixture is a miniature of the migrated balanced-1k scenario — the same
//! sections and the same vocabulary, small enough to mutate one key per test.
//! The full-size document is pinned byte for byte in `tests/golden_resolve.rs`;
//! these modules are about behaviour, that one is about bytes.
//!
//! This module also hosts the fixture the sibling test modules build on,
//! because every one of them resolves the same miniature scenario and a second
//! copy of it would drift.
#![expect(clippy::unwrap_used, reason = "test assertions may unwrap")]

use bench_schema::{
    AdapterCapabilities, AdapterDescription, BudgetSpec, ClusterProfile, SourceExperiment,
    SubjectEntry, ValidateReport,
};

use crate::error::CtlErrorKind;
use crate::resolve::{
    ResolveInputs, RuntimeInputs, SubjectProbe, resolve_experiment, resolve_pure,
};

pub(super) const SCENARIO: &str = r#"
name = "producer-miniature"
status = "diagnostic"
claim_eligible = false
load_mode = "closed-loop-capacity-point"
records = 10000
warmup_records = 1000

[application]
producer_instances = 1
callers_per_producer = 1
backpressure = "block-within-original-offer"
queue_bytes = 67108864
max_outstanding_records = 8192

[application_api]
admission_shape = "public-batch"
completion_shape = "aggregate-batch-terminal"
batch_records = 256

[payload]
bytes = 1024
profile = "deterministic-ascii-envelope"
seed = 44
identity = "KFB1 plus 16-byte run ID plus 64-bit sequence"

[producer]
acks = "all"
idempotence = true
compression = "none"
linger_ms = 5
batch_records = 256
batch_bytes = 65536
request_bytes = 1048576
delivery_timeout_ms = 60000
partitioning = "explicit-round-robin"
retry_max_replacements = 600
retry_backoff_ms = 100

[cluster]
brokers = 3
partitions = 12
replication_factor = 3
min_in_sync_replicas = 2
security = "plaintext"
"#;

pub(super) const CLUSTER: &str = r#"
name = "dev-compose"
bootstrap = "127.0.0.1:39092"
"#;

pub(super) const ATTEMPT: &str = "20260812T140305Z-1a2b3c4d";

fn scenario() -> SourceExperiment {
    SourceExperiment::from_toml_str(SCENARIO).unwrap()
}

fn cluster() -> ClusterProfile {
    ClusterProfile::from_toml_str(CLUSTER).unwrap()
}

pub(super) fn description(name: &str, version: &str) -> AdapterDescription {
    AdapterDescription {
        schema: AdapterDescription::SCHEMA.to_owned(),
        name: name.to_owned(),
        version: version.to_owned(),
        capabilities: AdapterCapabilities {
            producer: true,
            compression: vec!["none".to_owned()],
            ..AdapterCapabilities::default()
        },
        result_schemas: None,
    }
}

fn probe(name: &str, version: &str) -> SubjectProbe {
    SubjectProbe {
        subject: SubjectEntry {
            name: name.to_owned(),
            command: vec![format!("target/release/{name}-adapter")],
            role: None,
        },
        describe: description(name, version),
        validate: Some(ValidateReport::supported()),
        binary_sha256: None,
        argument_binary_sha256s: std::collections::BTreeMap::new(),
    }
}

pub(super) fn inputs() -> ResolveInputs {
    ResolveInputs {
        source: scenario(),
        cluster: cluster(),
        subjects: vec![probe("kafkars", "0.1.0"), probe("librdkafka-c", "2.15.0")],
        seed: None,
        budget: BudgetSpec::default(),
        runtime: RuntimeInputs {
            bootstrap: "127.0.0.1:39092,127.0.0.1:39093".to_owned(),
            attempt_id: ATTEMPT.to_owned(),
            order: None,
        },
    }
}

#[test]
fn the_lock_records_every_subject_as_it_was_probed() {
    let mut inputs = inputs();
    inputs.subjects[0].binary_sha256 = Some("a".repeat(64));

    let (_, lock) = resolve_pure(&inputs).unwrap();

    assert!(lock.has_expected_schema());
    assert_eq!(lock.subjects.len(), 2);
    let entry = lock.subject("kafkars").unwrap();
    assert_eq!(entry.command, vec!["target/release/kafkars-adapter"]);
    assert_eq!(
        entry.binary_sha256.as_deref(),
        Some("a".repeat(64).as_str())
    );
    assert_eq!(entry.describe, description("kafkars", "0.1.0"));
    assert!(entry.validate.supported);
    assert_eq!(lock.subject("librdkafka-c").unwrap().binary_sha256, None);
}

#[test]
fn a_declined_subject_stops_the_attempt_and_quotes_the_adapter() {
    let mut inputs = inputs();
    inputs.subjects[1].validate = Some(ValidateReport::unsupported(vec![
        "compression gzip is not implemented".to_owned(),
    ]));

    let error = resolve_pure(&inputs).unwrap_err();

    assert_eq!(error.kind(), CtlErrorKind::InvalidExperiment);
    assert!(
        error.message().contains("librdkafka-c"),
        "{}",
        error.message()
    );
    assert!(
        error
            .message()
            .contains("compression gzip is not implemented"),
        "{}",
        error.message()
    );
}

#[test]
fn a_subject_that_was_never_asked_cannot_be_locked() {
    let mut inputs = inputs();
    inputs.subjects[0].validate = None;

    let error = resolve_pure(&inputs).unwrap_err();

    assert_eq!(error.kind(), CtlErrorKind::InvalidExperiment);
    assert!(
        error.message().contains("never asked"),
        "{}",
        error.message()
    );
    assert!(
        resolve_experiment(&inputs).is_ok(),
        "the first pass exists precisely so validate has a document to read"
    );
}
