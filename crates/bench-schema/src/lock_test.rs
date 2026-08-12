//! Tests pinning the subjects lock, the home of everything the experiment id
//! deliberately excludes.
#![expect(
    clippy::unwrap_used,
    reason = "lock fixtures are exact; a bad one must fail the test immediately"
)]

use crate::{
    AdapterCapabilities, AdapterDescription, SubjectLockEntry, SubjectsLock, ValidateReport,
    canonical_bytes, parse_json_slice, sha256_hex,
};

fn describe(name: &str, version: &str) -> AdapterDescription {
    AdapterDescription {
        schema: AdapterDescription::SCHEMA.to_owned(),
        name: name.to_owned(),
        version: version.to_owned(),
        capabilities: AdapterCapabilities {
            producer: true,
            ..AdapterCapabilities::default()
        },
        result_schemas: None,
    }
}

fn lock() -> SubjectsLock {
    SubjectsLock::new(vec![
        SubjectLockEntry {
            name: "kafkars".to_owned(),
            command: vec!["target/release/kafkars-benchmark-adapter".to_owned()],
            binary_sha256: Some(sha256_hex(b"kafkars binary")),
            describe: describe("kafkars", "0.1.0"),
            validate: ValidateReport::supported(),
        },
        SubjectLockEntry {
            name: "librdkafka-c".to_owned(),
            command: vec!["sh".to_owned(), "-c".to_owned(), "exec adapter".to_owned()],
            binary_sha256: None,
            describe: describe("librdkafka-c", "2.15.0"),
            validate: ValidateReport::unsupported(vec!["compression lz4 unavailable".to_owned()]),
        },
    ])
}

#[test]
fn a_lock_round_trips() {
    let lock = lock();

    let bytes = canonical_bytes(&lock).unwrap();

    assert_eq!(parse_json_slice::<SubjectsLock>(&bytes).unwrap(), lock);
    assert!(lock.has_expected_schema());
    assert_eq!(SubjectsLock::SCHEMA, "kafkars.subjects-lock.v1");
}

#[test]
fn a_subject_is_found_by_name() {
    let lock = lock();

    assert_eq!(lock.subject("kafkars").unwrap().describe.version, "0.1.0");
    assert!(lock.subject("nobody").is_none());
}

#[test]
fn an_unreadable_binary_omits_its_digest_rather_than_faking_one() {
    let lock = lock();

    let text = String::from_utf8(canonical_bytes(&lock).unwrap()).unwrap();
    let entry = lock.subject("librdkafka-c").unwrap();

    assert!(entry.binary_sha256.is_none());
    assert_eq!(text.matches("binary_sha256").count(), 1);
}

#[test]
fn the_lock_records_the_command_the_identity_excludes() {
    let lock = lock();

    let text = String::from_utf8(canonical_bytes(&lock).unwrap()).unwrap();

    assert!(
        text.contains("target/release/kafkars-benchmark-adapter"),
        "{text}"
    );
    assert!(text.contains(r#""supported":false"#), "{text}");
}
