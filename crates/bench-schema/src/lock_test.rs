//! Tests pinning the subjects lock, the home of everything the experiment id
//! deliberately excludes.
#![expect(
    clippy::unwrap_used,
    reason = "lock fixtures are exact; a bad one must fail the test immediately"
)]

use std::collections::BTreeMap;

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
            argument_binary_sha256s: BTreeMap::new(),
            describe: describe("kafkars", "0.1.0"),
            validate: ValidateReport::supported(),
        },
        SubjectLockEntry {
            name: "librdkafka-c".to_owned(),
            command: vec!["sh".to_owned(), "-c".to_owned(), "exec adapter".to_owned()],
            binary_sha256: None,
            argument_binary_sha256s: BTreeMap::new(),
            describe: describe("librdkafka-c", "2.15.0"),
            validate: ValidateReport::unsupported(vec!["compression lz4 unavailable".to_owned()]),
        },
    ])
}

/// A lock whose subject is invoked through a wrapper, which is the shape the
/// argument digests exist for.
fn shim_lock() -> SubjectsLock {
    SubjectsLock::new(vec![SubjectLockEntry {
        name: "librdkafka-c".to_owned(),
        command: vec![
            "target/release/shim".to_owned(),
            "--binary".to_owned(),
            "target/release/librdkafka-producer".to_owned(),
        ],
        binary_sha256: Some(sha256_hex(b"the shim")),
        argument_binary_sha256s: BTreeMap::from([(
            "target/release/librdkafka-producer".to_owned(),
            sha256_hex(b"the C producer"),
        )]),
        describe: describe("librdkafka-c", "2.15.0"),
        validate: ValidateReport::supported(),
    }])
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
    assert_eq!(text.matches(r#""binary_sha256""#).count(), 1);
}

#[test]
fn an_ordinary_subject_adds_no_argument_digests_to_the_document() {
    // The field is append-only in the strongest sense: a lock for a subject
    // invoked directly serializes to exactly the bytes it did before the field
    // existed.
    let text = String::from_utf8(canonical_bytes(&lock()).unwrap()).unwrap();

    assert!(!text.contains("argument_binary_sha256s"), "{text}");
}

#[test]
fn a_wrapped_subject_records_the_binary_behind_the_wrapper() {
    let lock = shim_lock();

    let bytes = canonical_bytes(&lock).unwrap();
    let text = String::from_utf8(bytes.clone()).unwrap();
    let entry = lock.subject("librdkafka-c").unwrap();

    assert_eq!(
        entry.binary_sha256.as_deref(),
        Some(sha256_hex(b"the shim").as_str()),
        "the program digest still covers the program"
    );
    assert_eq!(
        entry
            .argument_binary_sha256s
            .get("target/release/librdkafka-producer")
            .map(String::as_str),
        Some(sha256_hex(b"the C producer").as_str()),
        "the digest is keyed by the argument it covered: {:?}",
        entry.argument_binary_sha256s
    );
    assert_eq!(entry.argument_binary_sha256s.len(), 1);
    assert!(text.contains("argument_binary_sha256s"), "{text}");
    assert_eq!(parse_json_slice::<SubjectsLock>(&bytes).unwrap(), lock);
}

#[test]
fn a_lock_written_before_the_field_existed_still_parses() {
    // Built by deleting the member from a document that has it, so the fixture
    // cannot drift away from the shape the writer actually produces.
    let mut document: serde_json::Value =
        serde_json::from_slice(&canonical_bytes(&shim_lock()).unwrap()).unwrap();
    let subject = &mut document["subjects"][0];
    assert!(
        subject
            .as_object_mut()
            .unwrap()
            .remove("argument_binary_sha256s")
            .is_some(),
        "the fixture must have carried the field to begin with"
    );
    let older = serde_json::to_vec(&document).unwrap();

    let lock: SubjectsLock = parse_json_slice(&older).unwrap();

    assert!(
        lock.subject("librdkafka-c")
            .unwrap()
            .argument_binary_sha256s
            .is_empty(),
        "an absent field reads as no argument digests, not as a parse failure"
    );
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
