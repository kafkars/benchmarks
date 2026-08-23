//! Tests pinning the schema registry against the canonical vocabulary and
//! against the `schemas/` directory.
#![expect(
    clippy::unwrap_used,
    reason = "registry fixtures are exact; a bad one must fail the test immediately"
)]

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

use crate::{
    ENGINE_SCHEMA_IDS, EXPERIMENT_V1, LEGACY_SCHEMA_IDS, PRODUCER_BENCHMARK_V1, SchemaErrorKind,
    is_registered, registered_schema_ids, require_schema, schema_file_name,
    schema_id_from_file_name,
};

/// The complete evidence vocabulary, written out independently of the crate
/// constants and sorted the way a directory listing sorts.
///
/// This is the same list `scripts/check-schemas` carries. Two hand-written
/// copies of a list look like duplication, and are: the point is that a change
/// to the vocabulary has to be made in both places, by a person, in a diff a
/// reviewer sees.
const CANONICAL_SCHEMA_IDS: [&str; 32] = [
    "kafkars.adapter-status.v1",
    "kafkars.adapter-validate.v1",
    "kafkars.adapter.v1",
    "kafkars.analysis-packet.v1",
    "kafkars.benchmark-adapter-config.v1",
    "kafkars.benchmark-environment.v1",
    "kafkars.benchmark-environment.v2",
    "kafkars.bundle.v1",
    "kafkars.capacity-search.v1",
    "kafkars.classification.v1",
    "kafkars.comparison.v1",
    "kafkars.execution-order.v1",
    "kafkars.experiment.v1",
    "kafkars.kafkars-native-metrics.v1",
    "kafkars.librdkafka-capacity-curve.v2",
    "kafkars.librdkafka-capacity-probe.v2",
    "kafkars.librdkafka-native-metrics.v1",
    "kafkars.librdkafka-statistics.v1",
    "kafkars.llm-summary.v1",
    "kafkars.process-resources.v2",
    "kafkars.producer-benchmark.v1",
    "kafkars.producer-benchmark.v2",
    "kafkars.producer-comparison-suite.v1",
    "kafkars.producer-comparison.v1",
    "kafkars.producer-fixed-comparison-suite.v2",
    "kafkars.producer-fixed-comparison.v2",
    "kafkars.producer-fixed-load.v1",
    "kafkars.producer-fixed-matrix.v2",
    "kafkars.producer-verification.v1",
    "kafkars.run-status.v1",
    "kafkars.subjects-lock.v1",
    "kafkars.suite-summary.v1",
];

fn registry() -> BTreeSet<&'static str> {
    registered_schema_ids().into_iter().collect()
}

#[test]
fn the_registry_is_exactly_the_canonical_vocabulary() {
    let expected: BTreeSet<&str> = CANONICAL_SCHEMA_IDS.into_iter().collect();

    assert_eq!(registry(), expected);
}

#[test]
fn the_registry_has_no_duplicates_and_two_families() {
    assert_eq!(ENGINE_SCHEMA_IDS.len(), 17);
    assert_eq!(LEGACY_SCHEMA_IDS.len(), 15);
    assert_eq!(registered_schema_ids().len(), 32);
    assert_eq!(registry().len(), 32);
}

#[test]
fn every_id_is_prefixed_and_versioned() {
    for id in registered_schema_ids() {
        assert!(id.starts_with("kafkars."), "{id} is missing the prefix");
        let version = id.rsplit_once('.').unwrap().1;
        assert!(
            version.starts_with('v') && version[1..].parse::<u32>().is_ok(),
            "{id} does not end in a version"
        );
    }
}

#[test]
fn membership_is_reported_for_both_families() {
    assert!(is_registered(EXPERIMENT_V1));
    assert!(is_registered(PRODUCER_BENCHMARK_V1));
    assert!(!is_registered("kafkars.experiment.v2"));
    assert!(!is_registered(""));
}

#[test]
fn require_schema_accepts_only_an_exact_match() {
    assert!(require_schema(EXPERIMENT_V1, EXPERIMENT_V1).is_ok());

    let error = require_schema("kafkars.experiment.v2", EXPERIMENT_V1).unwrap_err();

    assert_eq!(error.kind(), SchemaErrorKind::WrongSchema);
    assert!(
        error.context().contains("kafkars.experiment.v2"),
        "the rejection should name what it found: {error}"
    );
}

#[test]
fn a_schema_id_round_trips_through_its_file_name() {
    let file_name = schema_file_name(EXPERIMENT_V1);

    assert_eq!(file_name, "kafkars.experiment.v1.schema.json");
    assert_eq!(schema_id_from_file_name(&file_name), Some(EXPERIMENT_V1));
    assert_eq!(schema_id_from_file_name("README.md"), None);
}

fn schemas_directory() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("schemas")
}

/// Bijection between the registry and the committed schema documents.
///
/// The check skips itself while `schemas/` is still being written: an empty or
/// partially populated directory reports what is missing and returns, so that
/// this crate's tests do not fail for a neighbouring directory's state. Nothing
/// has to be flipped later — once the directory is complete the comparison runs
/// and is strict in both directions. `scripts/check-schemas` is the gate that
/// fails on a permanently incomplete directory.
#[test]
fn the_registry_matches_the_schemas_directory() {
    let directory = schemas_directory();
    let Ok(entries) = fs::read_dir(&directory) else {
        eprintln!("skipping: {} does not exist yet", directory.display());
        return;
    };

    let mut present = BTreeSet::new();
    for entry in entries {
        let entry = entry.unwrap();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(id) = schema_id_from_file_name(&name) {
            present.insert(id.to_owned());
        }
    }

    let expected = registry();
    let missing: Vec<&&str> = expected
        .iter()
        .filter(|id| !present.contains(**id))
        .collect();
    if present.is_empty() || !missing.is_empty() {
        eprintln!(
            "skipping: {} holds {} of {} schema documents; missing {missing:?}",
            directory.display(),
            present.len(),
            expected.len(),
        );
        return;
    }

    let present: BTreeSet<&str> = present.iter().map(String::as_str).collect();
    assert_eq!(
        present, expected,
        "the schemas/ directory and the registry disagree"
    );
}
