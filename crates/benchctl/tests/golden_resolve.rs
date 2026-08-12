//! Golden bytes for resolution: the migrated balanced-1k scenario, a committed
//! cluster profile, a committed subject list, and a fixed attempt, resolved
//! into pinned bytes and a pinned experiment id.
//!
//! # What each pinned artefact answers
//!
//! - `golden/resolved-experiment.pretty.json` is the document a sealed bundle
//!   carries as `experiment.resolved.json`. A diff means the resolver changed
//!   what it produces, or the migrated scenario changed what it asks for.
//! - `golden/subjects-lock.pretty.json` is `subjects.lock.json`. A diff means
//!   the record of *what was measured with* changed shape.
//! - [`BALANCED_1K_EXPERIMENT_ID`] and [`BALANCED_1K_RUN_ID`] pin the two
//!   identities. The experiment id must survive rebinding to another cluster or
//!   another attempt; the run id must not.
//!
//! The scenario is read from `scenarios/producer/` rather than copied here, so
//! a scenario edit is a golden diff rather than a silent divergence between the
//! file the harness runs and the file the test believes in. The cluster profile
//! and subject list are fixtures under `golden/` because no real one is
//! committed yet, and the subjects are deliberately fake: this test is about
//! the mapping, and it must not need a built adapter to run.
//!
//! Set `BENCHCTL_BLESS=1` to rewrite the golden files. That is an explicit act
//! that leaves a reviewable diff, which is the whole point of a golden.
#![expect(
    clippy::unwrap_used,
    reason = "golden fixtures are exact; a bad one must fail the test immediately"
)]

use std::fs;
use std::path::PathBuf;

use bench_schema::{
    AdapterCapabilities, AdapterDescription, BudgetSpec, ClusterProfile, LoadMode,
    ResolvedExperiment, SourceExperiment, SubjectsFile, SubjectsLock, ValidateReport,
    canonical_bytes, experiment_id, pretty_bytes,
};
use benchctl::resolve::{ResolveInputs, RuntimeInputs, SubjectProbe, resolve_pure};

/// Experiment id of the balanced-1k scenario as resolved below.
///
/// Pinned rather than computed: a change here without a deliberate change to
/// the resolver or the scenario means every historic bundle's id just stopped
/// matching the one this code would produce today.
///
/// It is the same digest `bench-schema`'s own golden pins, and that is the
/// point. That crate hand-builds a `ResolvedExperiment` and asserts its bytes;
/// this one *derives* the document from the scenario, the profile, and the
/// subject list. Agreement between the two means the resolver and the schema
/// have the same idea of what the balanced-1k experiment is, rather than two
/// ideas that happen to be tested separately.
const BALANCED_1K_EXPERIMENT_ID: &str =
    "d2932f88ad348028796b43b929f2fca826b683d19f13c0d1ad30d676b25dac15";

/// Run id derived from that experiment id and the fixed attempt id below.
const BALANCED_1K_RUN_ID: &str = "c87d9fde35a22e93";

/// The attempt every golden here is bound to.
const ATTEMPT_ID: &str = "20260812T140305Z-1a2b3c4d";

/// The bootstrap every golden here is bound to.
const BOOTSTRAP: &str = "127.0.0.1:39092,127.0.0.1:39093,127.0.0.1:39094";

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
}

fn golden_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
        .join(name)
}

fn golden_bytes(name: &str, produced: &[u8]) -> Vec<u8> {
    let path = golden_path(name);
    if std::env::var_os("BENCHCTL_BLESS").is_some() {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, produced).unwrap();
    }
    fs::read(&path).unwrap_or_else(|error| {
        panic!(
            "cannot read {}: {error}; run with BENCHCTL_BLESS=1 to create it",
            path.display()
        )
    })
}

fn scenario() -> SourceExperiment {
    let path = repository_root()
        .join("scenarios")
        .join("producer")
        .join("legacy-balanced-1k.toml");
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
    SourceExperiment::from_toml_str(&text).unwrap()
}

fn cluster() -> ClusterProfile {
    ClusterProfile::from_toml_str(&String::from_utf8(fixture("cluster-profile.toml")).unwrap())
        .unwrap()
}

fn subjects() -> SubjectsFile {
    SubjectsFile::from_toml_str(&String::from_utf8(fixture("subjects.toml")).unwrap()).unwrap()
}

fn fixture(name: &str) -> Vec<u8> {
    let path = golden_path(name);
    fs::read(&path).unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()))
}

/// The capability document each fake subject answers `describe` with.
fn description(name: &str, version: &str, result_schema: &str) -> AdapterDescription {
    let mut result_schemas = std::collections::BTreeMap::new();
    result_schemas.insert(LoadMode::ClosedLoop, result_schema.to_owned());
    AdapterDescription {
        schema: AdapterDescription::SCHEMA.to_owned(),
        name: name.to_owned(),
        version: version.to_owned(),
        capabilities: AdapterCapabilities {
            producer: true,
            consumer: false,
            idempotence: true,
            transactions: false,
            tls: false,
            compression: vec!["none".to_owned()],
            completion_modes: vec!["aggregate-batch-terminal".to_owned()],
            ownership_modes: vec!["client-owned".to_owned()],
            metric_families: vec!["latency".to_owned(), "throughput".to_owned()],
        },
        result_schemas: Some(result_schemas),
    }
}

fn inputs() -> ResolveInputs {
    let describes = [
        description("kafkars", "0.1.0", "kafkars.producer-benchmark.v1"),
        description("librdkafka-c", "2.15.0", "kafkars.producer-benchmark.v1"),
    ];
    let probes = subjects()
        .subjects
        .into_iter()
        .zip(describes)
        .enumerate()
        .map(|(index, (subject, describe))| SubjectProbe {
            subject,
            describe,
            validate: Some(ValidateReport::supported()),
            binary_sha256: Some(format!("{index}").repeat(64)),
            // The golden subjects are invoked directly, so there is no argument
            // that names a file and the sealed lock keeps its original bytes.
            argument_binary_sha256s: std::collections::BTreeMap::new(),
        })
        .collect();
    ResolveInputs {
        source: scenario(),
        cluster: cluster(),
        subjects: probes,
        seed: None,
        budget: BudgetSpec::default(),
        runtime: RuntimeInputs {
            bootstrap: BOOTSTRAP.to_owned(),
            attempt_id: ATTEMPT_ID.to_owned(),
            order: None,
        },
    }
}

fn resolved() -> (ResolvedExperiment, SubjectsLock) {
    resolve_pure(&inputs()).unwrap()
}

#[test]
fn the_resolved_document_matches_the_golden_bytes() {
    let (experiment, _) = resolved();
    let produced = pretty_bytes(&experiment).unwrap();

    let expected = golden_bytes("resolved-experiment.pretty.json", &produced);

    assert_eq!(
        String::from_utf8(produced).unwrap(),
        String::from_utf8(expected).unwrap()
    );
}

#[test]
fn the_subjects_lock_matches_the_golden_bytes() {
    let (_, lock) = resolved();
    let produced = pretty_bytes(&lock).unwrap();

    let expected = golden_bytes("subjects-lock.pretty.json", &produced);

    assert_eq!(
        String::from_utf8(produced).unwrap(),
        String::from_utf8(expected).unwrap()
    );
}

#[test]
fn the_golden_document_parses_back_into_the_resolved_experiment() {
    let (experiment, _) = resolved();
    let produced = pretty_bytes(&experiment).unwrap();
    let expected = golden_bytes("resolved-experiment.pretty.json", &produced);

    let parsed: ResolvedExperiment = bench_schema::parse_json_slice(&expected).unwrap();

    assert_eq!(parsed, experiment);
}

#[test]
fn the_experiment_id_and_the_run_id_are_pinned() {
    let (experiment, _) = resolved();

    assert_eq!(
        experiment_id(&experiment).unwrap().as_str(),
        BALANCED_1K_EXPERIMENT_ID
    );
    assert_eq!(
        experiment.runtime.as_ref().unwrap().run_id,
        BALANCED_1K_RUN_ID
    );
    assert_eq!(
        experiment.runtime.as_ref().unwrap().topic_prefix,
        format!("kfb-{BALANCED_1K_RUN_ID}")
    );
}

#[test]
fn resolving_twice_produces_byte_identical_output() {
    let first = pretty_bytes(&resolved().0).unwrap();
    let second = pretty_bytes(&resolved().0).unwrap();

    assert_eq!(first, second);
    assert_eq!(
        canonical_bytes(&resolved().0).unwrap(),
        canonical_bytes(&resolved().0).unwrap()
    );
}

#[test]
fn the_experiment_id_survives_a_different_cluster_and_a_different_attempt() {
    let mut rebound = inputs();
    rebound.runtime.bootstrap = "broker-1.internal:9092".to_owned();
    rebound.runtime.attempt_id = "20270101T000000Z-deadbeef".to_owned();
    let (experiment, _) = resolve_pure(&rebound).unwrap();

    assert_eq!(
        experiment_id(&experiment).unwrap().as_str(),
        BALANCED_1K_EXPERIMENT_ID,
        "the identity must aggregate repetitions, not separate them"
    );
    assert_ne!(
        experiment.runtime.as_ref().unwrap().run_id,
        BALANCED_1K_RUN_ID,
        "the run id must separate what the identity aggregates"
    );
}

#[test]
fn the_bundle_directory_is_named_by_the_short_experiment_id() {
    let (experiment, _) = resolved();

    assert_eq!(
        experiment_id(&experiment).unwrap().short(),
        &BALANCED_1K_EXPERIMENT_ID[..16]
    );
}
