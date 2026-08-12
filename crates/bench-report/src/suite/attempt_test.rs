//! Tests for reading one bundle: what makes an attempt unusable, and what
//! makes a bundle unreadable.
#![expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "test fixtures assert exact outcomes and must fail immediately"
)]

use crate::fixture::{BundleFixture, ResultFixture, scratch_directory};

use super::fixture::options;
use super::summarize_suite;

#[test]
fn a_subject_that_disqualifies_itself_invalidates_its_attempt() {
    let root = scratch_directory("suite-self-invalid");
    let roots = vec![
        BundleFixture::new("attempt-0")
            .subject("base", Some("base"), ResultFixture::default())
            .subject(
                "head",
                Some("head"),
                ResultFixture {
                    valid: false,
                    ..ResultFixture::default()
                },
            )
            .write(&root),
    ];

    let summary = summarize_suite(&roots, &options()).unwrap();

    assert!(!summary.attempts[0].run_valid);
    assert!(
        summary
            .notes
            .iter()
            .any(|note| note.contains("declared itself invalid")),
        "{:?}",
        summary.notes
    );
    assert!(summary.medians.is_empty());
    assert!(summary.pairs.is_empty());
    assert!(!summary.gates[0].passed);
}

#[test]
fn bundles_of_different_experiments_are_refused() {
    let root = scratch_directory("suite-mismatch");
    let mut other = BundleFixture::new("attempt-1")
        .subject("base", Some("base"), ResultFixture::default())
        .subject("head", Some("head"), ResultFixture::default());
    other.scenario = "a-different-scenario".to_owned();
    let roots = vec![
        BundleFixture::new("attempt-0")
            .subject("base", Some("base"), ResultFixture::default())
            .subject("head", Some("head"), ResultFixture::default())
            .write(&root),
        other.write(&root),
    ];

    let error = summarize_suite(&roots, &options()).unwrap_err();

    assert_eq!(error.kind(), crate::ReportErrorKind::Document);
    assert!(error.context().contains("different experiment"));
}

#[test]
fn a_histogram_naming_an_impossible_bucket_is_refused_rather_than_fatal() {
    // `8_320` is the first bucket index whose scale is 64: reading a percentile
    // out of it used to shift a `u64` by its own width, so a hand-edited bundle
    // could take down the summarizer. Only the index is hostile — the counts
    // still total the terminal count, so nothing else in the document objects.
    let root = scratch_directory("suite-impossible-bucket");
    let bundle = BundleFixture::new("attempt-0")
        .subject("base", Some("base"), ResultFixture::default())
        .subject("head", Some("head"), ResultFixture::default())
        .write(&root);
    let path = bundle.join("adapters").join("head").join("result.json");
    let mut document: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    document["timing"]["intended_to_terminal"]["counts"] = serde_json::json!([[8_320, 1_000]]);
    std::fs::write(&path, serde_json::to_vec_pretty(&document).unwrap()).unwrap();

    let error = summarize_suite(&[bundle], &options()).unwrap_err();

    assert_eq!(error.kind(), crate::ReportErrorKind::Document);
    assert!(error.context().contains("8320"), "{error}");
}

#[test]
fn a_missing_bundle_names_the_file_it_wanted() {
    let missing = std::env::temp_dir().join("bench-report-suite-absent");

    let error = summarize_suite(&[missing], &options()).unwrap_err();

    assert_eq!(error.kind(), crate::ReportErrorKind::Io);
    assert!(error.context().contains("status.json"));
}
