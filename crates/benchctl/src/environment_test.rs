//! Environment capture: the fallbacks, the identity rule, and the sibling map.
//!
//! These tests assert shape and behaviour rather than values, because the values
//! are the machine the tests happen to run on. The two properties that do have
//! to hold everywhere are that an unreadable fact is recorded as unreadable
//! rather than omitted, and that two captures of one unchanged machine share an
//! environment identity.
#![expect(clippy::unwrap_used, reason = "test assertions may unwrap")]

use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bench_schema::{BENCHMARK_ENVIRONMENT_V2, UNAVAILABLE};

use crate::environment::{BROKER_LIFECYCLE, BROKER_VERSION_UNKNOWN, capture, default_repositories};

const BOOTSTRAP: &str = "127.0.0.1:39092,127.0.0.1:39093";

fn at(seconds: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(seconds)
}

fn this_repository() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
}

#[test]
fn the_document_declares_the_second_environment_schema() {
    let document = capture(&[], BOOTSTRAP, at(1_770_000_000));

    assert_eq!(document.schema, BENCHMARK_ENVIRONMENT_V2);
    assert!(document.has_expected_schema());
    assert_eq!(document.captured_at, "2026-02-02T02:40:00.000Z");
}

#[test]
fn the_broker_facts_record_the_bootstrap_and_the_legacy_lifecycle_prose() {
    let document = capture(&[], BOOTSTRAP, SystemTime::now());

    assert_eq!(document.broker.bootstrap, BOOTSTRAP);
    assert_eq!(document.broker.version, BROKER_VERSION_UNKNOWN);
    assert_eq!(document.broker.lifecycle, BROKER_LIFECYCLE);
    assert_eq!(
        document.broker.lifecycle,
        "externally managed by the caller"
    );
}

#[test]
fn a_repository_that_cannot_be_read_is_unavailable_and_never_clean() {
    let repositories = vec![(
        "absent".to_owned(),
        PathBuf::from("/nonexistent/checkout-that-is-not-here"),
    )];

    let document = capture(&repositories, BOOTSTRAP, SystemTime::now());

    let state = document.source.get("absent").unwrap();
    assert_eq!(state.commit, UNAVAILABLE);
    assert!(
        state.dirty,
        "a working tree nobody could read must not be reported as clean"
    );
}

#[test]
fn a_readable_checkout_records_a_commit_digest() {
    let repositories = vec![("kafka_benchmarks".to_owned(), this_repository())];

    let document = capture(&repositories, BOOTSTRAP, SystemTime::now());

    let state = document.source.get("kafka_benchmarks").unwrap();
    if state.commit != UNAVAILABLE {
        assert_eq!(state.commit.len(), 40, "{}", state.commit);
        assert!(
            state.commit.chars().all(|c| c.is_ascii_hexdigit()),
            "{}",
            state.commit
        );
    }
}

#[test]
fn the_toolchain_records_one_line_for_each_tool_it_looked_for() {
    let document = capture(&[], BOOTSTRAP, SystemTime::now());

    for tool in ["rustc", "cargo", "cc"] {
        let version = document.toolchain.get(tool).unwrap();
        assert!(!version.is_empty(), "{tool} recorded an empty version");
        assert!(
            !version.contains('\n'),
            "{tool} recorded more than its first line: {version}"
        );
    }
    assert_ne!(
        document.toolchain.get("rustc").unwrap(),
        UNAVAILABLE,
        "the test suite is running under cargo, so rustc must be findable"
    );
}

#[test]
fn the_toolchain_records_the_build_identity_a_version_alone_does_not_carry() {
    let document = capture(&[], BOOTSTRAP, SystemTime::now());

    // These tests are compiled with debug assertions on, so the profile the
    // capture reads off this binary is the one it is running as.
    assert_eq!(
        document.toolchain.get("build_profile").map(String::as_str),
        Some(if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        })
    );
    // A constant, and documented as one: every build path in this repository
    // passes --locked, so the honest record is "true" rather than a probe of a
    // flag nobody passes at capture time.
    assert_eq!(
        document.toolchain.get("cargo_locked").map(String::as_str),
        Some("true")
    );
    // Unset and empty are the same fact, and both read as the string every
    // other unavailable probe uses rather than as an empty value.
    let flags = document.toolchain.get("rustflags").unwrap();
    assert!(!flags.is_empty(), "an empty string would read as no answer");
    if std::env::var("RUSTFLAGS").is_err() {
        assert_eq!(flags, UNAVAILABLE);
    }
}

#[test]
fn the_host_facts_are_filled_in_or_explicitly_unavailable() {
    let document = capture(&[], BOOTSTRAP, SystemTime::now());

    assert_eq!(document.host.platform, std::env::consts::OS);
    assert_eq!(document.host.architecture, std::env::consts::ARCH);
    assert!(document.host.logical_cpus >= 1);
    assert!(!document.host.release.is_empty());
    assert!(!document.host.cpu.is_empty());
    assert!(!document.host.uname.is_empty());
}

#[test]
fn the_identity_ignores_when_the_capture_happened() {
    let earlier = capture(&[], BOOTSTRAP, at(1_700_000_000));
    let later = capture(&[], BOOTSTRAP, at(1_800_000_000));

    assert_ne!(earlier.captured_at, later.captured_at);
    assert_eq!(earlier.identity().unwrap(), later.identity().unwrap());
}

#[test]
fn a_different_bootstrap_is_a_different_environment() {
    let one = capture(&[], "a:9092", at(1_700_000_000));
    let other = capture(&[], "b:9092", at(1_700_000_000));

    assert_ne!(one.identity().unwrap(), other.identity().unwrap());
}

#[test]
fn the_default_repository_map_names_this_repository_and_its_three_siblings() {
    let root = PathBuf::from("/checkouts/kafka-benchmarks");

    let repositories = default_repositories(&root);

    let names: Vec<&str> = repositories.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "kafka_benchmarks",
            "kafka_client",
            "kafka_driver",
            "kafka_protocol"
        ],
        "the v2 source map keeps the legacy sibling names"
    );
    assert_eq!(repositories[0].1, root);
    assert!(repositories[2].1.ends_with("kafka-driver"));
    assert!(repositories[3].1.ends_with("kafka-protocol"));
}
