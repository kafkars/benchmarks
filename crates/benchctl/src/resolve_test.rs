//! Resolution: the field mapping, the run-id derivation, the execution order,
//! and every request the resolver refuses.
//!
//! The fixture is a miniature of the migrated balanced-1k scenario — the same
//! sections and the same vocabulary, small enough to mutate one key per test.
//! The full-size document is pinned byte for byte in `tests/golden_resolve.rs`;
//! this file is about behaviour, that one is about bytes.
#![expect(clippy::unwrap_used, reason = "test assertions may unwrap")]

use bench_schema::{
    AdapterCapabilities, AdapterDescription, ArrivalModel, BudgetSpec, ClusterProfile,
    ExperimentKind, LoadMode, SourceExperiment, SubjectEntry, ValidateReport, canonical_bytes,
    experiment_id,
};

use crate::error::CtlErrorKind;
use crate::resolve::{
    DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_BROKER, ResolveInputs, RuntimeInputs, SubjectProbe,
    derive_run_id, resolve_experiment, resolve_pure,
};

const SCENARIO: &str = r#"
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

const CLUSTER: &str = r#"
name = "dev-compose"
bootstrap = "127.0.0.1:39092"
"#;

const ATTEMPT: &str = "20260812T140305Z-1a2b3c4d";

fn scenario() -> SourceExperiment {
    SourceExperiment::from_toml_str(SCENARIO).unwrap()
}

fn cluster() -> ClusterProfile {
    ClusterProfile::from_toml_str(CLUSTER).unwrap()
}

fn description(name: &str, version: &str) -> AdapterDescription {
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

fn inputs() -> ResolveInputs {
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
fn the_topics_are_named_from_the_run_id_and_the_subject() {
    let resolved = resolve_experiment(&inputs()).unwrap();

    let runtime = resolved.runtime.clone().unwrap();
    assert_eq!(runtime.topic_prefix, format!("kfb-{}", runtime.run_id));
    let topics = runtime.topics.get("kafkars").unwrap();
    assert_eq!(topics.measured, format!("kfb-{}-kafkars", runtime.run_id));
    assert_eq!(
        topics.warmup,
        format!("kfb-{}-kafkars-warmup", runtime.run_id)
    );
    assert_eq!(runtime.bootstrap, "127.0.0.1:39092,127.0.0.1:39093");
}

#[test]
fn two_attempts_of_one_experiment_share_an_id_and_differ_in_run_id() {
    let first = resolve_experiment(&inputs()).unwrap();
    let mut later_inputs = inputs();
    later_inputs.runtime.attempt_id = "20260812T140900Z-99887766".to_owned();
    let second = resolve_experiment(&later_inputs).unwrap();

    assert_eq!(
        experiment_id(&first).unwrap(),
        experiment_id(&second).unwrap(),
        "the attempt must not change what experiment this is"
    );
    assert_ne!(
        first.runtime.unwrap().run_id,
        second.runtime.unwrap().run_id,
        "two attempts must not write records under one run id"
    );
}

#[test]
fn the_run_id_is_sixteen_lowercase_hex_characters_of_a_derived_digest() {
    let resolved = resolve_experiment(&inputs()).unwrap();
    let identity = experiment_id(&resolved).unwrap();

    let run_id = resolved.runtime.unwrap().run_id;

    assert_eq!(run_id.len(), 16);
    assert!(
        run_id
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    );
    assert_eq!(run_id, derive_run_id(identity.as_str(), ATTEMPT));
}

#[test]
fn binding_the_runtime_does_not_move_the_experiment_id() {
    let bound = resolve_experiment(&inputs()).unwrap();
    let mut unbound = bound.clone();
    unbound.runtime = None;

    assert_eq!(
        experiment_id(&bound).unwrap(),
        experiment_id(&unbound).unwrap()
    );
}

#[test]
fn resolution_is_deterministic() {
    let first = canonical_bytes(&resolve_experiment(&inputs()).unwrap()).unwrap();
    let second = canonical_bytes(&resolve_experiment(&inputs()).unwrap()).unwrap();

    assert_eq!(first, second);
}

#[test]
fn the_execution_order_defaults_to_the_subjects_file_order() {
    let resolved = resolve_experiment(&inputs()).unwrap();

    assert_eq!(
        resolved.runtime.unwrap().execution_order,
        vec!["kafkars".to_owned(), "librdkafka-c".to_owned()]
    );
}

#[test]
fn a_requested_order_is_recorded_verbatim() {
    let mut inputs = inputs();
    inputs.runtime.order = Some(vec!["librdkafka-c".to_owned(), "kafkars".to_owned()]);

    let resolved = resolve_experiment(&inputs).unwrap();

    assert_eq!(
        resolved.runtime.unwrap().execution_order,
        vec!["librdkafka-c".to_owned(), "kafkars".to_owned()]
    );
}

#[test]
fn an_order_that_is_not_a_permutation_of_the_subjects_is_refused() {
    for requested in [
        vec!["kafkars".to_owned()],
        vec!["kafkars".to_owned(), "kafkars".to_owned()],
        vec!["kafkars".to_owned(), "sarama".to_owned()],
    ] {
        let mut inputs = inputs();
        inputs.runtime.order = Some(requested.clone());

        let error = resolve_experiment(&inputs).unwrap_err();

        assert_eq!(
            error.kind(),
            CtlErrorKind::InvalidExperiment,
            "{requested:?} was accepted"
        );
    }
}

#[test]
fn the_seed_override_replaces_the_payload_seed_and_changes_the_experiment() {
    let mut inputs = inputs();
    inputs.seed = Some(1_234);

    let overridden = resolve_experiment(&inputs).unwrap();

    assert_eq!(overridden.seed, 1_234);
    assert_ne!(
        experiment_id(&overridden).unwrap(),
        experiment_id(&resolve_experiment(&self::inputs()).unwrap()).unwrap(),
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

#[test]
fn an_attempt_with_no_bootstrap_is_refused() {
    let mut inputs = inputs();
    inputs.runtime.bootstrap = "   ".to_owned();

    let error = resolve_experiment(&inputs).unwrap_err();

    assert_eq!(error.kind(), CtlErrorKind::InvalidExperiment);
}

#[test]
fn an_empty_subject_list_measures_nothing_and_is_refused() {
    let mut inputs = inputs();
    inputs.subjects.clear();

    let error = resolve_experiment(&inputs).unwrap_err();

    assert_eq!(error.kind(), CtlErrorKind::InvalidExperiment);
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

#[test]
fn the_run_id_is_a_function_of_both_identities() {
    let experiment = "d2932f88ad348028796b43b929f2fca826b683d19f13c0d1ad30d676b25dac15";

    assert_eq!(
        derive_run_id(experiment, ATTEMPT),
        derive_run_id(experiment, ATTEMPT),
        "derivation must be a function, not a sample"
    );
    assert_ne!(
        derive_run_id(experiment, ATTEMPT),
        derive_run_id(experiment, "20260812T140900Z-99887766")
    );
    assert_ne!(
        derive_run_id(experiment, ATTEMPT),
        derive_run_id(&"0".repeat(64), ATTEMPT)
    );
}

#[test]
fn an_adapter_identity_that_would_be_machine_specific_is_refused() {
    // Both fields are hashed into the experiment id. A version stamped with the
    // build directory it was compiled in — the shape a `cc` line or a
    // `CARGO_MANIFEST_DIR` leaks into a hand-pinned constant — would give the
    // same experiment a different identity on every checkout, and the suite
    // that aggregates repetitions would find one attempt each.
    for (label, version) in [
        ("a build path", "2.15.0-/Users/somebody/src/librdkafka"),
        ("a windows build path", "2.15.0-C:\\build\\librdkafka"),
        ("a compiler line", "2.15.0 (built with cc -O2)"),
        ("a trailing newline", "2.15.0\n"),
        ("nothing at all", ""),
    ] {
        let mut inputs = inputs();
        inputs.subjects[1].describe.version = version.to_owned();

        let error = resolve_experiment(&inputs)
            .err()
            .unwrap_or_else(|| panic!("{label} was accepted into an experiment id"));

        assert_eq!(error.kind(), crate::error::CtlErrorKind::InvalidExperiment);
        assert!(
            error.message().contains("librdkafka-c") && error.message().contains("adapter_version"),
            "{label}: {error}"
        );
    }
}

#[test]
fn an_adapter_name_with_a_path_separator_is_refused() {
    let mut inputs = inputs();
    inputs.subjects[0].describe.name = "adapters/kafkars".to_owned();

    let error = resolve_experiment(&inputs).unwrap_err();

    assert_eq!(error.kind(), crate::error::CtlErrorKind::InvalidExperiment);
    assert!(error.message().contains("adapter_name"), "{error}");
    assert!(error.message().contains('/'), "{error}");
}

#[test]
fn ordinary_adapter_identities_are_still_accepted() {
    // The rule must not reject the versions the two real adapters report.
    for version in ["0.1.0", "2.15.0", "2.3.0-RC3", "1.9.2+build.7"] {
        let mut inputs = inputs();
        inputs.subjects[1].describe.version = version.to_owned();
        assert!(
            resolve_experiment(&inputs).is_ok(),
            "{version} is an ordinary version"
        );
    }
}
