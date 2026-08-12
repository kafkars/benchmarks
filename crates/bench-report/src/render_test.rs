//! Tests for the renderers, including golden files that pin the exact bytes.
//!
//! The goldens exist because a report is read by people who will not re-derive
//! the numbers: a silent layout or rounding change is a silent change to what
//! those people believe. Regenerating them is a deliberate act, and the diff is
//! the review.
#![expect(
    clippy::unwrap_used,
    reason = "test fixtures assert exact outcomes and must fail immediately"
)]

use std::path::PathBuf;

use bench_schema::SuiteSummary;

use crate::fixture::{BundleFixture, ResultFixture, scratch_directory};
use crate::render::{
    DIAGNOSTIC_BANNER, NOT_REPORTED, render_html_suite, render_markdown_bundle,
    render_markdown_suite,
};
use crate::suite::{SuiteOptions, SuiteReport, summarize_suite_report};

/// The Markdown a golden suite renders to.
const GOLDEN_MARKDOWN: &str = include_str!("../golden/suite.md");

/// The HTML a golden suite renders to.
const GOLDEN_HTML: &str = include_str!("../golden/suite.html");

/// The native statistics stream the head subject emits in the golden suite.
const GOLDEN_STATISTICS: &str = concat!(
    r#"{"phase":"baseline","statistics":{"tx":0,"tx_bytes":0,"rx_bytes":0,"txmsgs":0,"#,
    r#""txmsg_bytes":0,"brokers":{"b/1":{"nodeid":1,"txretries":0,"req_timeouts":0,"#,
    r#""req":{"Produce":0}}},"topics":{"measured":{"batchcnt":{"cnt":0,"sum":0,"min":0,"#,
    r#""max":0},"batchsize":{"cnt":0,"sum":0,"min":0,"max":0}}}}}"#,
    "\n",
    r#"{"phase":"final","statistics":{"tx":104,"tx_bytes":1230000,"rx_bytes":9000,"#,
    r#""txmsgs":1000,"txmsg_bytes":1024000,"brokers":{"b/1":{"nodeid":1,"txretries":2,"#,
    r#""req_timeouts":0,"req":{"Produce":100}}},"topics":{"measured":{"batchcnt":"#,
    r#"{"cnt":100,"sum":1000,"min":5,"max":20},"batchsize":{"cnt":100,"sum":1024000,"#,
    r#""min":5120,"max":20480}}}}}"#,
    "\n"
);

/// Options that keep the goldens fast and fixed.
fn options() -> SuiteOptions {
    SuiteOptions {
        seed: 20_240_601,
        resamples: 1_000,
        practical_threshold: 0.05,
    }
}

/// Builds the five-attempt suite both goldens are rendered from.
///
/// `name` names this test's own scratch directory: tests run in parallel, and
/// two of them clearing the same directory would race.
fn golden_report(name: &str) -> SuiteReport {
    let root = scratch_directory(name);
    let goodputs = [
        (100_000.0, 118_000.0),
        (101_000.0, 119_500.0),
        (99_500.0, 117_000.0),
        (100_500.0, 118_500.0),
        (100_200.0, 119_000.0),
    ];
    let latencies = [
        (12_000_000, 9_000_000),
        (12_500_000, 9_200_000),
        (11_800_000, 8_900_000),
        (12_200_000, 9_100_000),
        (12_100_000, 9_050_000),
    ];
    let roots: Vec<PathBuf> = goodputs
        .iter()
        .zip(latencies)
        .enumerate()
        .map(|(index, ((base_rate, head_rate), (base_ns, head_ns)))| {
            let mut bundle = BundleFixture::new(&format!("attempt-{index}"))
                .subject(
                    "librdkafka",
                    Some("base"),
                    ResultFixture {
                        goodput: *base_rate,
                        terminal_ns: base_ns,
                        ..ResultFixture::default()
                    },
                )
                .subject(
                    "kafkars",
                    Some("head"),
                    ResultFixture {
                        goodput: *head_rate,
                        terminal_ns: head_ns,
                        resources: None,
                        ..ResultFixture::default()
                    },
                );
            bundle
                .native_metrics
                .push(("librdkafka".to_owned(), GOLDEN_STATISTICS.to_owned()));
            bundle.write(&root)
        })
        .collect();
    summarize_suite_report(&roots, &options()).unwrap()
}

/// The summary alone, for the renderers that take one.
fn golden_summary(name: &str) -> SuiteSummary {
    golden_report(name).summary
}

#[test]
fn the_markdown_matches_its_golden() {
    let rendered = golden_report("render-golden-markdown").markdown();

    assert_eq!(rendered, GOLDEN_MARKDOWN);
}

#[test]
fn the_html_matches_its_golden() {
    let rendered = golden_report("render-golden-html").html();

    assert_eq!(rendered, GOLDEN_HTML);
}

#[test]
fn rendering_is_deterministic() {
    let summary = golden_summary("render-deterministic");

    assert_eq!(
        render_markdown_suite(&summary),
        render_markdown_suite(&summary)
    );
    assert_eq!(render_html_suite(&summary), render_html_suite(&summary));
}

#[test]
fn every_report_carries_the_diagnostic_banner() {
    let summary = golden_summary("render-banner");

    assert!(render_markdown_suite(&summary).contains(DIAGNOSTIC_BANNER));
    assert!(render_html_suite(&summary).contains("never claim-eligible"));
}

#[test]
fn numeric_columns_are_right_aligned_in_markdown() {
    let markdown = render_markdown_suite(&golden_summary("render-alignment"));

    assert!(markdown.contains("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"));
    assert!(markdown.contains("| --- | --- | ---: | ---: | ---: | --- | --- |"));
}

#[test]
fn the_html_is_self_contained() {
    let html = render_html_suite(&golden_summary("render-self-contained"));

    assert!(html.starts_with("<!doctype html>"));
    assert!(html.contains("<style>"));
    assert!(!html.contains("http://"));
    assert!(!html.contains("https://"));
    assert!(!html.contains("<script"));
    assert!(!html.contains("src="));
    assert!(!html.contains("@import"));
}

#[test]
fn the_html_tolerates_both_color_schemes() {
    let html = render_html_suite(&golden_summary("render-color-scheme"));

    assert!(html.contains("color-scheme:light dark"));
    assert!(html.contains("@media (prefers-color-scheme:dark)"));
}

#[test]
fn the_html_draws_one_inline_bar_per_pair() {
    let summary = golden_summary("render-bars");

    let html = render_html_suite(&summary);

    assert_eq!(
        html.matches("<svg class=\"bar\"").count(),
        summary.pairs.len()
    );
    assert!(html.contains("class=\"interval good\""));
}

#[test]
fn an_absent_measurement_never_renders_as_zero() {
    let summary = golden_summary("render-absent");

    let markdown = render_markdown_suite(&summary);

    // The head subject reports no process resources in this fixture.
    assert!(markdown.contains(NOT_REPORTED));
}

#[test]
fn a_summary_with_no_valid_attempt_says_so_rather_than_showing_a_table() {
    let root = scratch_directory("render-invalid");
    let mut bundle = BundleFixture::new("attempt-0")
        .subject("base", Some("base"), ResultFixture::default())
        .subject("head", Some("head"), ResultFixture::default());
    bundle.run_valid = false;
    let roots = vec![bundle.write(&root)];
    let summary = crate::suite::summarize_suite(&roots, &options()).unwrap();

    let markdown = render_markdown_suite(&summary);
    let html = render_html_suite(&summary);

    assert!(markdown.contains("No attempt was valid"));
    assert!(html.contains("No attempt was valid"));
    assert!(markdown.contains("No pair had enough valid attempts"));
}

#[test]
fn the_bundle_view_reports_one_attempt() {
    let root = scratch_directory("render-bundle");
    let bundle = BundleFixture::new("attempt-single")
        .subject("librdkafka", Some("base"), ResultFixture::default())
        .subject(
            "kafkars",
            Some("head"),
            ResultFixture {
                acknowledged: 995,
                failed: 5,
                resources: None,
                ..ResultFixture::default()
            },
        )
        .write(&root);

    let markdown = render_markdown_bundle(&bundle).unwrap();

    assert!(markdown.starts_with("# Attempt attempt-single"));
    assert!(markdown.contains(DIAGNOSTIC_BANNER));
    assert!(markdown.contains("## Offer accounting"));
    assert!(markdown.contains("| kafkars | 1000 | 1000 | 995 | 5 | 0 | 0 | 0 | true |"));
    assert!(markdown.contains("## Checks this attempt did not perform"));
    assert!(markdown.contains(NOT_REPORTED));
}

#[test]
fn the_bundle_view_names_why_an_attempt_is_unusable() {
    let root = scratch_directory("render-bundle-invalid");
    let mut bundle = BundleFixture::new("attempt-bad").subject(
        "librdkafka",
        Some("base"),
        ResultFixture::default(),
    );
    bundle.run_valid = false;
    let written = bundle.write(&root);

    let markdown = render_markdown_bundle(&written).unwrap();

    assert!(markdown.contains("## Why this attempt is not usable evidence"));
    assert!(markdown.contains("- run valid: `false`"));
}

#[test]
fn a_missing_bundle_is_an_error_not_an_empty_report() {
    let missing = std::env::temp_dir().join("bench-report-render-absent");

    let error = render_markdown_bundle(&missing).unwrap_err();

    assert_eq!(error.kind(), crate::ReportErrorKind::Io);
}

/// Rewrites both goldens from the fixture above.
///
/// Ignored, and never run by the gate: regenerating a golden is a decision, not
/// a build step. When a deliberate change to the renderers or to the summary
/// makes the two assertions above fail, run
///
/// ```text
/// cargo test -p bench-report regenerate_the_goldens -- --ignored
/// ```
///
/// and then *read the diff*. That diff is the review — it is the only place a
/// reader will see that a number they were shown last release is a different
/// number now. A regeneration whose diff nobody looked at is worse than no
/// golden at all, because it launders the change through a green build.
#[test]
#[ignore = "regeneration is a deliberate act; run it explicitly and review the diff"]
fn regenerate_the_goldens() {
    let report = golden_report("render-golden-regen");
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("golden");
    std::fs::write(root.join("suite.md"), report.markdown()).unwrap();
    std::fs::write(root.join("suite.html"), report.html()).unwrap();
}
