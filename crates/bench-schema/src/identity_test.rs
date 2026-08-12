//! Tests pinning the experiment identity and its two deliberate exclusions.
#![expect(
    clippy::unwrap_used,
    reason = "identity fixtures are exact; a bad one must fail the test immediately"
)]

use crate::experiment_test::sample_experiment;
use crate::{
    ExperimentId, SchemaErrorKind, experiment_id, identity_bytes, identity_document, is_digest_hex,
    sha256_hex,
};

#[test]
fn the_digest_helper_matches_the_published_vectors() {
    assert_eq!(
        sha256_hex(b""),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    assert_eq!(
        sha256_hex(b"abc"),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}

#[test]
fn a_digest_is_lowercase_hex_of_the_right_length() {
    let digest = sha256_hex(b"kafka-benchmarks");

    assert!(is_digest_hex(&digest));
    assert!(!is_digest_hex(&digest.to_uppercase()));
    assert!(!is_digest_hex(&digest[..63]));
    assert!(!is_digest_hex(""));
}

#[test]
fn an_experiment_id_shortens_to_sixteen_characters() {
    let id = ExperimentId::parse(&sha256_hex(b"anything")).unwrap();

    assert_eq!(id.short().len(), 16);
    assert!(id.as_str().starts_with(id.short()));
    assert_eq!(id.to_string(), id.as_str());
}

#[test]
fn a_malformed_experiment_id_is_refused() {
    let error = ExperimentId::parse("not-a-digest").unwrap_err();

    assert_eq!(error.kind(), SchemaErrorKind::Identity);
}

#[test]
fn an_experiment_id_serializes_as_a_bare_string() {
    let id = ExperimentId::parse(&sha256_hex(b"anything")).unwrap();

    let encoded = serde_json::to_string(&id).unwrap();

    assert_eq!(encoded, format!("\"{id}\""));
    assert_eq!(serde_json::from_str::<ExperimentId>(&encoded).unwrap(), id);
    assert!(serde_json::from_str::<ExperimentId>("\"short\"").is_err());
}

#[test]
fn the_identity_document_drops_the_runtime_binding_and_every_command() {
    let experiment = sample_experiment();

    let document = identity_document(&experiment).unwrap();
    let object = document.as_object().unwrap();

    assert!(!object.contains_key("runtime"));
    assert!(object.contains_key("subjects"));
    for subject in object["subjects"].as_array().unwrap() {
        let subject = subject.as_object().unwrap();
        assert!(!subject.contains_key("command"));
        assert!(subject.contains_key("adapter_version"));
    }
}

#[test]
fn the_identity_ignores_the_runtime_binding() {
    let experiment = sample_experiment();
    let baseline = experiment_id(&experiment).unwrap();

    let mut rebound = sample_experiment();
    if let Some(runtime) = rebound.runtime.as_mut() {
        runtime.bootstrap = "10.0.0.1:9092,10.0.0.2:9092".to_owned();
        runtime.run_id = "fedcba9876543210".to_owned();
        runtime.topic_prefix = "kfb-fedcba9876543210".to_owned();
        runtime.execution_order.reverse();
        for topics in runtime.topics.values_mut() {
            topics.measured = format!("kfb-fedcba9876543210-{}", topics.measured.len());
            topics.warmup = format!("kfb-fedcba9876543210-{}-warmup", topics.warmup.len());
        }
    }

    let mut unbound = sample_experiment();
    unbound.runtime = None;

    assert_eq!(experiment_id(&rebound).unwrap(), baseline);
    assert_eq!(experiment_id(&unbound).unwrap(), baseline);
}

#[test]
fn the_identity_ignores_every_subject_command() {
    let experiment = sample_experiment();
    let baseline = experiment_id(&experiment).unwrap();

    let mut moved = sample_experiment();
    moved.subjects[0].command = vec![
        "/usr/local/bin/kafkars-benchmark-adapter".to_owned(),
        "--verbose".to_owned(),
    ];
    moved.subjects[1].command = vec!["/opt/bench/librdkafka".to_owned()];

    assert_eq!(experiment_id(&moved).unwrap(), baseline);
}

#[test]
fn the_identity_follows_the_seed() {
    let baseline = experiment_id(&sample_experiment()).unwrap();

    let mut reseeded = sample_experiment();
    reseeded.seed += 1;

    assert_ne!(experiment_id(&reseeded).unwrap(), baseline);
}

#[test]
fn the_identity_follows_every_other_intent_field() {
    let baseline = experiment_id(&sample_experiment()).unwrap();

    let mut more_records = sample_experiment();
    more_records.records += 1;
    assert_ne!(experiment_id(&more_records).unwrap(), baseline);

    let mut renamed_subject = sample_experiment();
    renamed_subject.subjects[0].name = "kafkars-next".to_owned();
    renamed_subject.runtime = None;
    let mut renamed_baseline = sample_experiment();
    renamed_baseline.runtime = None;
    assert_ne!(
        experiment_id(&renamed_subject).unwrap(),
        experiment_id(&renamed_baseline).unwrap()
    );

    let mut other_version = sample_experiment();
    other_version.subjects[1].adapter_version = "2.16.0".to_owned();
    assert_ne!(experiment_id(&other_version).unwrap(), baseline);

    let mut wider_budget = sample_experiment();
    wider_budget.budget.run_timeout_seconds += 1;
    assert_ne!(experiment_id(&wider_budget).unwrap(), baseline);
}

#[test]
fn the_identity_bytes_are_canonical_and_carry_no_floats() {
    let bytes = identity_bytes(&sample_experiment()).unwrap();
    let text = String::from_utf8(bytes).unwrap();

    assert!(
        text.starts_with(r#"{"application":{"admission_shape""#),
        "{text}"
    );
    assert!(!text.contains("runtime"));
    assert!(!text.contains("command"));
    assert!(text.contains(r#""seed":44"#));
}

#[test]
fn an_invalid_experiment_gets_no_identity() {
    let mut experiment = sample_experiment();
    experiment.claim_eligible = true;

    let error = experiment_id(&experiment).unwrap_err();

    assert_eq!(error.kind(), SchemaErrorKind::InvalidField);
}
