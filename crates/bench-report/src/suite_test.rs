//! Tests for the aggregation across repetitions, including determinism and the
//! treatment of attempts that must not reach a median.
#![expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "test fixtures assert exact outcomes and must fail immediately"
)]

use std::path::PathBuf;

use crate::fixture::{BundleFixture, ResultFixture, scratch_directory};
use crate::suite::{
    SuiteMetric, SuiteOptions, UNSEALED_DIGEST, metric_of_field, summarize_suite,
    summarize_suite_report,
};

/// A resample count that keeps the suite fast without changing any semantics.
fn options() -> SuiteOptions {
    SuiteOptions {
        seed: 1_234,
        resamples: 500,
        practical_threshold: 0.05,
    }
}

/// Writes `count` attempts where the head subject is faster by `head_gain`.
fn roles_suite(name: &str, count: usize, head_gain: f64) -> Vec<PathBuf> {
    let root = scratch_directory(name);
    (0..count)
        .map(|index| {
            #[expect(
                clippy::cast_precision_loss,
                reason = "the index is a small loop counter"
            )]
            let jitter = 1.0 + (index as f64) * 0.001;
            BundleFixture::new(&format!("attempt-{index}"))
                .subject(
                    "base",
                    Some("base"),
                    ResultFixture {
                        goodput: 100_000.0 * jitter,
                        terminal_ns: 10_000_000,
                        ..ResultFixture::default()
                    },
                )
                .subject(
                    "head",
                    Some("head"),
                    ResultFixture {
                        goodput: 100_000.0 * head_gain * jitter,
                        terminal_ns: 8_000_000,
                        ..ResultFixture::default()
                    },
                )
                .write(&root)
        })
        .collect()
}

#[test]
fn the_metric_order_is_fixed_and_round_trips_through_its_field_names() {
    assert_eq!(SuiteMetric::ALL.len(), 7);
    assert_eq!(SuiteMetric::ALL[0], SuiteMetric::Goodput);
    assert_eq!(SuiteMetric::ALL[2], SuiteMetric::P99Latency);
    for metric in SuiteMetric::ALL {
        assert_eq!(metric_of_field(metric.field()), Some(metric));
    }
    assert_eq!(metric_of_field("not_a_metric"), None);
    assert!(SuiteMetric::Goodput.higher_is_better());
    assert!(!SuiteMetric::P99Latency.higher_is_better());
}

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
fn percentiles_are_derived_from_the_sealed_histograms() {
    let roots = roles_suite("suite-percentiles", 5, 1.20);

    let summary = summarize_suite(&roots, &options()).unwrap();
    let head = summary
        .medians
        .iter()
        .find(|entry| entry.name == "head")
        .unwrap();

    assert_eq!(head.p50_intended_to_terminal_ns, 8_000_000);
    assert_eq!(head.p99_intended_to_terminal_ns, 8_000_000);
    assert_eq!(head.p999_intended_to_terminal_ns, 8_000_000);
    assert_eq!(head.p99_admission_wait_ns, 1_000);
    assert_eq!(head.role.as_deref(), Some("head"));
    assert_eq!(head.max_rss_bytes, Some(64 * 1024 * 1024));
    assert!((head.cpu_core_seconds.unwrap() - 5.0).abs() < 1e-9);
}

#[test]
fn roles_decide_the_pairing() {
    let roots = roles_suite("suite-roles", 5, 1.20);

    let summary = summarize_suite(&roots, &options()).unwrap();

    assert!(!summary.pairs.is_empty());
    for pair in &summary.pairs {
        assert_eq!(pair.numerator_subject, "head");
        assert_eq!(pair.denominator_subject, "base");
    }
}

#[test]
fn without_roles_every_subject_is_compared_against_the_first() {
    let root = scratch_directory("suite-no-roles");
    let roots: Vec<PathBuf> = (0..5)
        .map(|index| {
            BundleFixture::new(&format!("attempt-{index}"))
                .subject("alpha", None, ResultFixture::default())
                .subject("beta", None, ResultFixture::default())
                .subject("gamma", None, ResultFixture::default())
                .write(&root)
        })
        .collect();

    let summary = summarize_suite(&roots, &options()).unwrap();

    let numerators: Vec<&str> = summary
        .pairs
        .iter()
        .map(|pair| pair.numerator_subject.as_str())
        .collect();
    assert!(numerators.contains(&"beta"));
    assert!(numerators.contains(&"gamma"));
    assert!(
        summary
            .pairs
            .iter()
            .all(|pair| pair.denominator_subject == "alpha")
    );
}

#[test]
fn a_clear_win_passes_its_gate_and_a_parity_result_does_not() {
    let win = summarize_suite(&roles_suite("suite-win", 5, 1.20), &options()).unwrap();
    let parity = summarize_suite(&roles_suite("suite-parity", 5, 1.0), &options()).unwrap();

    let gate_of = |summary: &bench_schema::SuiteSummary| {
        summary
            .gates
            .iter()
            .find(|gate| gate.name == "head-over-base:acknowledged_records_per_second")
            .unwrap()
            .passed
    };

    assert!(gate_of(&win));
    assert!(!gate_of(&parity));
}

#[test]
fn a_gate_states_the_rule_it_enforces() {
    let summary = summarize_suite(&roles_suite("suite-gate-text", 5, 1.20), &options()).unwrap();

    let goodput = summary
        .gates
        .iter()
        .find(|gate| gate.name == "head-over-base:acknowledged_records_per_second")
        .unwrap();
    let latency = summary
        .gates
        .iter()
        .find(|gate| gate.name == "head-over-base:p99_intended_to_terminal_ns")
        .unwrap();

    assert!(
        goodput.description.contains("above 1.050"),
        "{}",
        goodput.description
    );
    assert!(goodput.description.contains("larger is better"));
    assert!(
        latency.description.contains("below 0.950"),
        "{}",
        latency.description
    );
    assert!(latency.description.contains("smaller is better"));
    assert!(latency.passed, "{}", latency.detail);
}

#[test]
fn the_structural_gates_are_always_present() {
    let summary = summarize_suite(&roles_suite("suite-structural", 2, 1.20), &options()).unwrap();

    let names: Vec<&str> = summary
        .gates
        .iter()
        .map(|gate| gate.name.as_str())
        .collect();
    assert_eq!(names[0], "attempts-valid");
    assert_eq!(names[1], "paired-repetitions");
    assert_eq!(names[2], "dispersion-within-budget");
    assert!(summary.gates[0].passed);
    // Two attempts is below the five paired repetitions a comparison needs.
    assert!(!summary.gates[1].passed);
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
fn dispersion_is_absent_rather_than_zero_for_a_single_attempt() {
    let roots = roles_suite("suite-dispersion-one", 1, 1.20);

    let summary = summarize_suite(&roots, &options()).unwrap();

    let goodput = summary
        .dispersion
        .iter()
        .find(|entry| entry.name == "head" && entry.metric == "acknowledged_records_per_second")
        .unwrap();
    assert_eq!(goodput.coefficient_of_variation, None);
    assert!(!summary.gates[2].passed);
}

#[test]
fn dispersion_is_reported_for_every_metric_and_subject() {
    let roots = roles_suite("suite-dispersion-many", 5, 1.20);

    let summary = summarize_suite(&roots, &options()).unwrap();

    assert_eq!(summary.dispersion.len(), 2 * SuiteMetric::ALL.len());
    let goodput = summary
        .dispersion
        .iter()
        .find(|entry| entry.name == "head" && entry.metric == "acknowledged_records_per_second")
        .unwrap();
    assert!(goodput.coefficient_of_variation.unwrap() < 0.05);
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
fn a_missing_bundle_names_the_file_it_wanted() {
    let missing = std::env::temp_dir().join("bench-report-suite-absent");

    let error = summarize_suite(&[missing], &options()).unwrap_err();

    assert_eq!(error.kind(), crate::ReportErrorKind::Io);
    assert!(error.context().contains("status.json"));
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
