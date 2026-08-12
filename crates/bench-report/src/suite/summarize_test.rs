//! Tests for the assembled summary: what it carries, what it refuses, and
//! that two runs of it are the same bytes.
#![expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "test fixtures assert exact outcomes and must fail immediately"
)]

use std::path::PathBuf;

use crate::fixture::{BundleFixture, ResultFixture, scratch_directory};

use super::fixture::{options, roles_suite};
use super::{UNSEALED_DIGEST, summarize_suite, summarize_suite_report};

#[test]
fn an_empty_suite_summarizes_nothing() {
    let error = summarize_suite(&[], &options()).unwrap_err();

    assert_eq!(error.kind(), crate::ReportErrorKind::Sample);
}

#[test]
fn every_attempt_appears_in_the_summary() {
    let roots = roles_suite("suite-attempts", 5, 1.20);

    let summary = summarize_suite(&roots, &options()).unwrap();

    assert_eq!(summary.repetitions, 5);
    assert_eq!(summary.attempts.len(), 5);
    assert_eq!(summary.attempts[0].attempt_id, "attempt-0");
    assert_eq!(summary.attempts[0].bundle_digest, UNSEALED_DIGEST);
    assert!(summary.attempts.iter().all(|attempt| attempt.run_valid));
    assert!(!summary.claim_eligible);
    assert_eq!(summary.scenario_name, "producer-comparison");
    assert_eq!(summary.seed, 1_234);
    assert_eq!(summary.resamples, 500);
}

#[test]
fn an_invalid_attempt_is_kept_excluded_and_named() {
    let root = scratch_directory("suite-invalid");
    let mut roots: Vec<PathBuf> = (0..4)
        .map(|index| {
            BundleFixture::new(&format!("attempt-{index}"))
                .subject("base", Some("base"), ResultFixture::default())
                .subject(
                    "head",
                    Some("head"),
                    ResultFixture {
                        goodput: 120_000.0,
                        ..ResultFixture::default()
                    },
                )
                .write(&root)
        })
        .collect();
    let mut broken = BundleFixture::new("attempt-broken")
        .subject("base", Some("base"), ResultFixture::default())
        .subject(
            "head",
            Some("head"),
            ResultFixture {
                goodput: 1.0,
                ..ResultFixture::default()
            },
        );
    broken.run_valid = false;
    roots.push(broken.write(&root));

    let summary = summarize_suite(&roots, &options()).unwrap();

    assert_eq!(summary.attempts.len(), 5);
    let excluded = summary
        .attempts
        .iter()
        .find(|attempt| attempt.attempt_id == "attempt-broken")
        .unwrap();
    assert!(!excluded.run_valid);
    assert!(
        summary
            .notes
            .iter()
            .any(|note| note.contains("attempt-broken") && note.contains("excluded")),
        "{:?}",
        summary.notes
    );
    // The excluded attempt's ruinous goodput must not move the median.
    let head = summary
        .medians
        .iter()
        .find(|entry| entry.name == "head")
        .unwrap();
    assert!(head.acknowledged_records_per_second > 100_000.0);
}

#[test]
fn two_summaries_of_the_same_bundles_are_byte_identical() {
    let roots = roles_suite("suite-determinism", 5, 1.13);

    let first = summarize_suite(&roots, &options()).unwrap();
    let second = summarize_suite(&roots, &options()).unwrap();

    let left = serde_json::to_vec(&first).unwrap();
    let right = serde_json::to_vec(&second).unwrap();
    assert_eq!(left, right);
    assert_eq!(first, second);
}

#[test]
fn a_summary_round_trips_through_its_own_schema() {
    let roots = roles_suite("suite-round-trip", 5, 1.13);

    let summary = summarize_suite(&roots, &options()).unwrap();
    let bytes = serde_json::to_vec(&summary).unwrap();
    let parsed = bench_schema::SuiteSummary::from_slice(&bytes).unwrap();

    assert_eq!(parsed, summary);
}

#[test]
fn a_subject_without_native_statistics_has_no_economics_and_says_so() {
    let roots = roles_suite("suite-no-economics", 3, 1.10);

    let report = summarize_suite_report(&roots, &options()).unwrap();

    assert!(report.economics.is_empty());
    assert!(
        report
            .summary
            .notes
            .iter()
            .any(|note| note.contains("no native client statistics")),
        "{:?}",
        report.summary.notes
    );
}

#[test]
fn native_statistics_are_totalled_across_the_valid_attempts() {
    let root = scratch_directory("suite-economics");
    let stream = concat!(
        r#"{"phase":"baseline","statistics":{"tx":0,"tx_bytes":0,"txmsgs":0,"txmsg_bytes":0,"#,
        r#""brokers":{"b/1":{"nodeid":1,"req":{"Produce":0}}}}}"#,
        "\n",
        r#"{"phase":"final","statistics":{"tx":100,"tx_bytes":1200,"txmsgs":1000,"#,
        r#""txmsg_bytes":1000,"brokers":{"b/1":{"nodeid":1,"req":{"Produce":100}}}}}"#,
        "\n"
    );
    let roots: Vec<PathBuf> = (0..2)
        .map(|index| {
            let mut bundle = BundleFixture::new(&format!("attempt-{index}"))
                .subject("base", Some("base"), ResultFixture::default())
                .subject("head", Some("head"), ResultFixture::default());
            bundle
                .native_metrics
                .push(("head".to_owned(), stream.to_owned()));
            bundle.write(&root)
        })
        .collect();

    let report = summarize_suite_report(&roots, &options()).unwrap();

    assert_eq!(report.economics.len(), 1);
    let head = &report.economics[0];
    assert_eq!(head.subject, "head");
    assert_eq!(head.attempts_reported, 2);
    assert_eq!(head.totals.produce_requests, Some(200));
    assert_eq!(head.totals.transmitted_records, Some(2_000));
    // 200 produce requests for the 2000 acknowledged records of two attempts.
    assert!(
        (head
            .totals
            .produce_requests_per_million_acknowledged
            .unwrap()
            - 100_000.0)
            .abs()
            < 1e-6
    );
    assert!((head.totals.records_per_produce_request.unwrap() - 10.0).abs() < 1e-9);
}
