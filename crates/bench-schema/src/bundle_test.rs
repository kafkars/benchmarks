//! Tests pinning the checksum line format and the bundle digest.
#![expect(
    clippy::unwrap_used,
    reason = "checksum fixtures are exact; a bad one must fail the test immediately"
)]

use crate::{
    BundleManifest, CHECKSUM_SEPARATOR, ChecksumEntry, SchemaErrorKind, canonical_bytes,
    parse_checksums, render_checksums, sha256_hex,
};

fn digest_of(text: &str) -> String {
    sha256_hex(text.as_bytes())
}

fn entries() -> Vec<ChecksumEntry> {
    vec![
        ChecksumEntry::new(digest_of("status"), "status.json").unwrap(),
        ChecksumEntry::new(digest_of("result"), "adapters/kafkars/result.json").unwrap(),
        ChecksumEntry::new(digest_of("environment"), "environment.json").unwrap(),
    ]
}

#[test]
fn the_separator_is_exactly_two_spaces() {
    assert_eq!(CHECKSUM_SEPARATOR, "  ");
}

#[test]
fn lines_are_sorted_by_path_bytes_and_newline_terminated() {
    let rendered = render_checksums(&entries());

    let paths: Vec<&str> = rendered
        .lines()
        .map(|line| line.split_once(CHECKSUM_SEPARATOR).unwrap().1)
        .collect();

    assert_eq!(
        paths,
        vec![
            "adapters/kafkars/result.json",
            "environment.json",
            "status.json"
        ]
    );
    assert!(rendered.ends_with("status.json\n"));
    assert_eq!(rendered.matches('\n').count(), 3);
}

#[test]
fn a_line_is_a_digest_two_spaces_and_a_relative_path() {
    let entry = ChecksumEntry::new(digest_of("status"), "status.json").unwrap();

    let rendered = render_checksums(std::slice::from_ref(&entry));

    assert_eq!(rendered, format!("{}  status.json\n", entry.digest()));
    assert_eq!(entry.path(), "status.json");
}

#[test]
fn rendering_and_parsing_round_trip() {
    let rendered = render_checksums(&entries());

    let parsed = parse_checksums(&rendered).unwrap();

    assert_eq!(parsed.len(), 3);
    assert_eq!(render_checksums(&parsed), rendered);
    assert_eq!(parsed[0].path(), "adapters/kafkars/result.json");
}

#[test]
fn a_path_that_would_not_round_trip_is_refused() {
    for path in [
        "",
        "/absolute/path",
        " leading-space",
        "adapters\\kafkars\\result.json",
        "../escape.json",
        "two\nlines",
    ] {
        assert!(
            ChecksumEntry::new(digest_of("x"), path).is_err(),
            "{path:?} should not be a legal bundle path"
        );
    }
}

#[test]
fn a_malformed_digest_is_refused() {
    assert!(ChecksumEntry::new("abc", "status.json").is_err());
    assert!(ChecksumEntry::new(digest_of("x").to_uppercase(), "status.json").is_err());
}

#[test]
fn a_malformed_line_is_refused() {
    let single_space = format!("{}{}", digest_of("x"), " status.json");
    let error = parse_checksums(&single_space).unwrap_err();
    assert_eq!(error.kind(), SchemaErrorKind::Parse);
    assert!(error.context().contains("line 1"), "{error}");

    assert!(parse_checksums("\n").is_err());
    assert!(parse_checksums(&format!("{}  \n", digest_of("x"))).is_err());
}

#[test]
fn a_duplicated_path_is_refused() {
    let line = format!("{}  status.json\n", digest_of("x"));
    let doubled = format!("{line}{line}");

    let error = parse_checksums(&doubled).unwrap_err();

    assert!(error.context().contains("listed twice"), "{error}");
}

#[test]
fn the_bundle_digest_is_the_digest_of_the_manifest_bytes() {
    let rendered = render_checksums(&entries());

    let manifest = BundleManifest::from_checksums_bytes(rendered.as_bytes(), 4_096).unwrap();

    assert_eq!(manifest.bundle_digest, sha256_hex(rendered.as_bytes()));
    assert_eq!(manifest.file_count, 3);
    assert_eq!(manifest.total_bytes, 4_096);
    assert!(manifest.has_expected_schema());
}

#[test]
fn the_manifest_serializes_with_sorted_keys() {
    let rendered = render_checksums(&entries());
    let manifest = BundleManifest::from_checksums_bytes(rendered.as_bytes(), 7).unwrap();

    let text = String::from_utf8(canonical_bytes(&manifest).unwrap()).unwrap();

    assert!(
        text.starts_with(&format!(
            r#"{{"bundle_digest":"{}""#,
            manifest.bundle_digest
        )),
        "{text}"
    );
    assert!(text.ends_with(r#""total_bytes":7}"#), "{text}");
}

#[test]
fn a_manifest_over_unparseable_bytes_is_refused() {
    assert!(BundleManifest::from_checksums_bytes(b"nonsense\n", 0).is_err());
    assert!(BundleManifest::from_checksums_bytes(&[0xff, 0xfe], 0).is_err());
}

#[test]
fn an_empty_bundle_manifest_is_representable() {
    let manifest = BundleManifest::from_checksums_bytes(b"", 0).unwrap();

    assert_eq!(manifest.file_count, 0);
    assert_eq!(
        manifest.bundle_digest,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
}
