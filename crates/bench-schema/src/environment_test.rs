//! Tests pinning the environment identity rule inherited from the legacy
//! harness.
#![expect(
    clippy::unwrap_used,
    reason = "environment fixtures are exact; a bad one must fail the test immediately"
)]

use std::collections::BTreeMap;

use crate::{
    BrokerFacts, EnvironmentDocument, HostFacts, RepositoryState, UNAVAILABLE, canonical_bytes,
    is_digest_hex, parse_json_slice,
};

fn environment() -> EnvironmentDocument {
    let mut source = BTreeMap::new();
    source.insert(
        "kafka-client".to_owned(),
        RepositoryState {
            commit: "cf2b4a59ef8a5cb10145b94b5d9b5b0c90eec074".to_owned(),
            dirty: true,
        },
    );
    source.insert(
        "kafka-driver".to_owned(),
        RepositoryState {
            commit: "0a5378d6e3620d5198a07c6b65f022287edcf6ad".to_owned(),
            dirty: false,
        },
    );

    let mut toolchain = BTreeMap::new();
    toolchain.insert("cargo".to_owned(), "cargo 1.88.0".to_owned());
    toolchain.insert("rustc".to_owned(), "rustc 1.88.0".to_owned());
    toolchain.insert("cc".to_owned(), UNAVAILABLE.to_owned());

    EnvironmentDocument {
        schema: EnvironmentDocument::SCHEMA.to_owned(),
        captured_at: "2026-08-12T10:15:00Z".to_owned(),
        source,
        toolchain,
        host: HostFacts {
            platform: "darwin".to_owned(),
            release: "24.1.0".to_owned(),
            architecture: "arm64".to_owned(),
            cpu: "Apple M3 Max".to_owned(),
            logical_cpus: 16,
            memory_bytes: 68_719_476_736,
            uname: "Darwin 24.1.0".to_owned(),
        },
        broker: BrokerFacts {
            version: "4.3.1".to_owned(),
            bootstrap: "127.0.0.1:39092".to_owned(),
            lifecycle: "externally managed by the caller".to_owned(),
        },
    }
}

#[test]
fn the_schema_id_is_the_second_version() {
    assert_eq!(
        EnvironmentDocument::SCHEMA,
        "kafkars.benchmark-environment.v2"
    );
    assert!(environment().has_expected_schema());
}

#[test]
fn the_identity_ignores_the_capture_time() {
    // This is the v1 rule from `benchmarkEnvironmentIdentity` in
    // `legacy/benchctl/environment.mjs`: hash the document with `captured_at`
    // removed, so that two attempts on an unchanged machine share an identity.
    let baseline = environment();
    let mut later = environment();
    later.captured_at = "2027-01-01T00:00:00Z".to_owned();

    assert_eq!(later.identity().unwrap(), baseline.identity().unwrap());
    assert!(is_digest_hex(&baseline.identity().unwrap()));
}

#[test]
fn the_identity_bytes_do_not_mention_the_capture_time() {
    let text = String::from_utf8(environment().identity_bytes().unwrap()).unwrap();

    assert!(!text.contains("captured_at"), "{text}");
    assert!(text.starts_with(r#"{"broker":{"bootstrap""#), "{text}");
}

#[test]
fn the_identity_follows_every_other_fact() {
    let baseline = environment().identity().unwrap();

    let mut other_commit = environment();
    other_commit.source.get_mut("kafka-client").unwrap().commit =
        "0000000000000000000000000000000000000000".to_owned();
    assert_ne!(other_commit.identity().unwrap(), baseline);

    let mut clean_tree = environment();
    clean_tree.source.get_mut("kafka-client").unwrap().dirty = false;
    assert_ne!(clean_tree.identity().unwrap(), baseline);

    let mut other_host = environment();
    other_host.host.cpu = "Apple M4".to_owned();
    assert_ne!(other_host.identity().unwrap(), baseline);

    let mut other_broker = environment();
    other_broker.broker.bootstrap = "10.0.0.1:9092".to_owned();
    assert_ne!(other_broker.identity().unwrap(), baseline);

    let mut other_toolchain = environment();
    other_toolchain
        .toolchain
        .insert("cc".to_owned(), "Apple clang 17".to_owned());
    assert_ne!(other_toolchain.identity().unwrap(), baseline);
}

#[test]
fn the_source_map_names_whatever_repositories_the_caller_declared() {
    // Version 1 hard-coded three Kafka repositories; a generic lab cannot know
    // its subjects in advance, so the map is open.
    let mut environment = environment();
    environment.source.insert(
        "some-other-client".to_owned(),
        RepositoryState {
            commit: UNAVAILABLE.to_owned(),
            dirty: false,
        },
    );

    let bytes = canonical_bytes(&environment).unwrap();
    let parsed: EnvironmentDocument = parse_json_slice(&bytes).unwrap();

    assert_eq!(parsed, environment);
    assert_eq!(parsed.source.len(), 3);
}

#[test]
fn an_unreadable_fact_is_recorded_rather_than_omitted() {
    let mut environment = environment();
    environment.host.cpu = UNAVAILABLE.to_owned();
    environment.host.logical_cpus = 0;

    let text = String::from_utf8(canonical_bytes(&environment).unwrap()).unwrap();

    assert!(text.contains(r#""cpu":"unavailable""#), "{text}");
    assert!(text.contains(r#""logical_cpus":0"#), "{text}");
}
