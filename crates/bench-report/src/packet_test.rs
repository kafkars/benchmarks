//! Tests for the analysis packet: the numbering, the verdict rule, and the one
//! check that keeps prose bound to the numbers.
#![expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "test fixtures assert exact outcomes and must fail immediately"
)]

use std::path::PathBuf;

use bench_schema::{Confidence, LlmFinding, LlmHypothesis, LlmSummary, SuiteSummary, Verdict};

use crate::fixture::{BundleFixture, ResultFixture, scratch_directory};
use crate::packet::{build_packet, validate_llm_summary};
use crate::suite::{SuiteOptions, SuiteReport, summarize_suite, summarize_suite_report};

/// Options that keep the packet tests fast.
fn options() -> SuiteOptions {
    SuiteOptions {
        seed: 99,
        resamples: 400,
        practical_threshold: 0.05,
    }
}

/// Writes a suite where the head subject is `gain` times the base subject's
/// goodput and `latency` times its latency.
fn suite(name: &str, gain: f64, latency: f64) -> SuiteSummary {
    suite_of(name, gain, latency, 5)
}

/// The same suite at an explicit repetition count.
fn suite_of(name: &str, gain: f64, latency: f64, repetitions: u32) -> SuiteSummary {
    let root = scratch_directory(name);
    let roots: Vec<PathBuf> = (0..repetitions)
        .map(|index| {
            let jitter = 0.002f64.mul_add(f64::from(index), 1.0);
            #[expect(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "the fixture's latency values are small and exact"
            )]
            let head_ns = (10_000_000.0 * latency * jitter) as u64;
            #[expect(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "the fixture's latency values are small and exact"
            )]
            let base_ns = (10_000_000.0 * jitter) as u64;
            BundleFixture::new(&format!("attempt-{index}"))
                .subject(
                    "base",
                    Some("base"),
                    ResultFixture {
                        goodput: 100_000.0 * jitter,
                        terminal_ns: base_ns,
                        ..ResultFixture::default()
                    },
                )
                .subject(
                    "head",
                    Some("head"),
                    ResultFixture {
                        goodput: 100_000.0 * gain * jitter,
                        terminal_ns: head_ns,
                        ..ResultFixture::default()
                    },
                )
                .write(&root)
        })
        .collect();
    summarize_suite(&roots, &options()).unwrap()
}

#[test]
fn the_packet_declares_its_schema_and_validates() {
    let packet = build_packet(&suite("packet-schema", 1.20, 0.80));

    assert!(packet.has_expected_schema());
    packet.validate().unwrap();
    assert!(!packet.validity.claim_eligible);
    assert_eq!(packet.validity.runs_total, 5);
    assert_eq!(packet.validity.runs_valid, 5);
    assert_eq!(packet.scenario_name, "producer-comparison");
}

#[test]
fn metric_ids_are_assigned_in_the_documented_order() {
    let summary = suite("packet-ids", 1.20, 0.80);

    let packet = build_packet(&summary);

    // Medians come first: the base subject's goodput is the first id.
    assert_eq!(
        packet.metrics["M001"].name,
        "base acknowledged goodput (median)"
    );
    assert_eq!(packet.metrics["M001"].unit, "records/s");
    assert_eq!(
        packet.metrics["M002"].name,
        "base p50 offer-to-terminal (median)"
    );
    assert_eq!(packet.metrics["M002"].unit, "ns");
    // Then the pairs, three ids each, in summary order.
    let first_pair = packet
        .metrics
        .values()
        .find(|metric| metric.name.contains("ratio of medians"))
        .unwrap();
    assert_eq!(first_pair.unit, "ratio");
    assert!(
        packet
            .metrics
            .values()
            .any(|metric| metric.name.contains("interval low"))
    );
    assert!(
        packet
            .metrics
            .values()
            .any(|metric| metric.name.contains("coefficient of variation"))
    );
}

#[test]
fn the_numbering_is_stable_across_identical_runs() {
    let summary = suite("packet-stability", 1.20, 0.80);

    let first = build_packet(&summary);
    let second = build_packet(&summary);

    assert_eq!(first, second);
    assert_eq!(
        serde_json::to_vec(&first).unwrap(),
        serde_json::to_vec(&second).unwrap()
    );
}

#[test]
fn every_finding_cites_a_metric_the_packet_defines() {
    let packet = build_packet(&suite("packet-citations", 1.20, 0.80));

    for finding in &packet.deterministic_findings {
        for reference in &finding.metric_refs {
            assert!(
                packet.metrics.contains_key(reference),
                "{reference} is not defined"
            );
        }
    }
    assert!(
        packet.deterministic_findings[0]
            .text
            .contains("5 of 5 attempts")
    );
}

#[test]
fn an_unambiguous_win_is_improved() {
    let packet = build_packet(&suite("packet-improved", 1.20, 0.80));

    assert_eq!(packet.verdict, Verdict::Improved);
}

#[test]
fn an_unambiguous_loss_is_regressed() {
    let packet = build_packet(&suite("packet-regressed", 0.80, 1.30));

    assert_eq!(packet.verdict, Verdict::Regressed);
}

#[test]
fn a_win_on_one_axis_and_a_loss_on_another_is_mixed() {
    // Faster goodput, slower latency.
    let packet = build_packet(&suite("packet-mixed", 1.20, 1.30));

    assert_eq!(packet.verdict, Verdict::Mixed);
}

#[test]
fn parity_on_every_axis_is_inconclusive() {
    let packet = build_packet(&suite("packet-inconclusive", 1.0, 1.0));

    assert_eq!(packet.verdict, Verdict::Inconclusive);
}

#[test]
fn a_suite_with_no_valid_attempt_is_invalid() {
    let root = scratch_directory("packet-invalid");
    let mut bundle = BundleFixture::new("attempt-0")
        .subject("base", Some("base"), ResultFixture::default())
        .subject("head", Some("head"), ResultFixture::default());
    bundle.run_valid = false;
    let summary = summarize_suite(&[bundle.write(&root)], &options()).unwrap();

    let packet = build_packet(&summary);

    assert_eq!(packet.verdict, Verdict::Invalid);
    assert_eq!(packet.validity.runs_valid, 0);
    assert_eq!(packet.subjects.len(), 2);
    packet.validate().unwrap();
}

#[test]
fn unresolved_comparisons_are_named_as_anomalies() {
    let packet = build_packet(&suite("packet-anomalies", 1.20, 1.0));

    assert!(
        packet
            .anomalies
            .iter()
            .any(|anomaly| anomaly.contains("unresolved") && anomaly.contains("straddles")),
        "{:?}",
        packet.anomalies
    );
    assert_eq!(packet.verdict, Verdict::Improved);
}

#[test]
fn thin_evidence_is_an_anomaly() {
    let root = scratch_directory("packet-thin");
    let roots = vec![
        BundleFixture::new("attempt-0")
            .subject("base", Some("base"), ResultFixture::default())
            .subject("head", Some("head"), ResultFixture::default())
            .write(&root),
    ];
    let summary = summarize_suite(&roots, &options()).unwrap();

    let packet = build_packet(&summary);

    assert!(
        packet
            .anomalies
            .iter()
            .any(|anomaly| anomaly.contains("below the 5 paired repetitions")),
        "{:?}",
        packet.anomalies
    );
}

#[test]
fn evidence_references_point_at_bundles_and_files() {
    let packet = build_packet(&suite("packet-evidence", 1.20, 0.80));

    assert_eq!(packet.evidence_refs["A001"], "unsealed");
    assert_eq!(packet.evidence_refs["R001"], "adapters/base/result.json");
    assert_eq!(packet.source.bundle_digests.len(), 5);
    assert!(packet.source.suite.is_some());
}

#[test]
fn request_economics_reach_the_packet_only_through_a_report() {
    let root = scratch_directory("packet-economics");
    let stream = concat!(
        r#"{"phase":"baseline","statistics":{"tx":0,"tx_bytes":0,"txmsgs":0,"txmsg_bytes":0,"#,
        r#""brokers":{"b/1":{"nodeid":1,"req":{"Produce":0}}}}}"#,
        "\n",
        r#"{"phase":"final","statistics":{"tx":100,"tx_bytes":1200,"txmsgs":1000,"#,
        r#""txmsg_bytes":1000,"brokers":{"b/1":{"nodeid":1,"req":{"Produce":100}}}}}"#,
        "\n"
    );
    let roots: Vec<PathBuf> = (0..5)
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
    let report: SuiteReport = summarize_suite_report(&roots, &options()).unwrap();

    let with_economics = report.packet();
    let without = build_packet(&report.summary);

    assert!(
        with_economics
            .metrics
            .values()
            .any(|metric| metric.name.contains("produce requests per million")),
        "{:?}",
        with_economics.metrics
    );
    assert!(
        !without
            .metrics
            .values()
            .any(|metric| metric.name.contains("produce requests per million"))
    );
    assert!(
        with_economics
            .deterministic_findings
            .iter()
            .any(|finding| finding.text.contains("head spent")),
    );
    assert_eq!(
        with_economics.evidence_refs["S001"],
        "adapters/head/client-metrics.jsonl"
    );
    with_economics.validate().unwrap();
}

#[test]
fn a_summary_that_copies_the_verdict_and_cites_the_packet_is_accepted() {
    let packet = build_packet(&suite("packet-llm-ok", 1.20, 0.80));
    let summary = LlmSummary {
        schema: LlmSummary::SCHEMA.to_owned(),
        verdict: packet.verdict,
        executive_summary: "The head subject delivered more records per second.".to_owned(),
        findings: vec![LlmFinding {
            text: "Goodput improved.".to_owned(),
            metric_refs: vec!["M001".to_owned()],
            evidence_refs: vec!["A001".to_owned()],
        }],
        hypotheses: vec![LlmHypothesis {
            text: "Batching may explain it.".to_owned(),
            confidence: Confidence::Low,
            evidence_refs: vec!["R001".to_owned()],
        }],
        next_experiments: vec!["Repeat at a larger payload size.".to_owned()],
        caveats: vec!["Diagnostic only.".to_owned()],
        provenance: None,
    };

    validate_llm_summary(&summary, &packet).unwrap();
}

#[test]
fn a_summary_that_overrules_the_verdict_is_rejected() {
    let packet = build_packet(&suite("packet-llm-verdict", 1.0, 1.0));
    let summary = LlmSummary {
        schema: LlmSummary::SCHEMA.to_owned(),
        verdict: Verdict::Improved,
        executive_summary: "It got faster.".to_owned(),
        findings: Vec::new(),
        hypotheses: Vec::new(),
        next_experiments: Vec::new(),
        caveats: Vec::new(),
        provenance: None,
    };

    let error = validate_llm_summary(&summary, &packet).unwrap_err();

    assert!(error.to_string().contains("verdict"), "{error}");
}

#[test]
fn a_summary_that_invents_a_metric_is_rejected() {
    let packet = build_packet(&suite("packet-llm-metric", 1.20, 0.80));
    let summary = LlmSummary {
        schema: LlmSummary::SCHEMA.to_owned(),
        verdict: packet.verdict,
        executive_summary: "It got faster.".to_owned(),
        findings: vec![LlmFinding {
            text: "Invented.".to_owned(),
            metric_refs: vec!["M999".to_owned()],
            evidence_refs: Vec::new(),
        }],
        hypotheses: Vec::new(),
        next_experiments: Vec::new(),
        caveats: Vec::new(),
        provenance: None,
    };

    let error = validate_llm_summary(&summary, &packet).unwrap_err();

    assert!(error.to_string().contains("M999"), "{error}");
}

#[test]
fn a_run_below_the_repetition_minimum_is_inconclusive_and_says_which_way_it_leaned() {
    let packet = build_packet(&suite_of("packet-under-repeated", 1.30, 0.70, 2));

    assert_eq!(packet.verdict, Verdict::Inconclusive);
    let directional = packet
        .deterministic_findings
        .iter()
        .find(|finding| finding.text.starts_with("Directionally"))
        .unwrap_or_else(|| panic!("{:?}", packet.deterministic_findings));
    assert!(
        directional.text.contains("improved"),
        "{}",
        directional.text
    );
    assert!(directional.text.contains("n=2"), "{}", directional.text);
    assert!(
        directional.text.contains("inconclusive"),
        "{}",
        directional.text
    );
    // The same run at the minimum states the direction as the verdict.
    assert_eq!(
        build_packet(&suite_of("packet-enough-reps", 1.30, 0.70, 5)).verdict,
        Verdict::Improved
    );
}

#[test]
fn an_attribution_metric_never_decides_the_verdict() {
    // The accepted-to-terminal pair is numbered, so prose may cite it, and is
    // excluded from the verdict and from the findings that carry a direction.
    let packet = build_packet(&suite("packet-attribution", 1.0, 1.0));

    assert!(
        packet
            .metrics
            .values()
            .any(|metric| metric.name.contains("p99_accepted_to_terminal_ns")),
        "the attribution metric is still citable"
    );
    assert!(
        !packet
            .deterministic_findings
            .iter()
            .any(|finding| finding.text.contains("p99_accepted_to_terminal_ns")),
        "{:?}",
        packet.deterministic_findings
    );
    assert_eq!(packet.verdict, Verdict::Inconclusive);
}

#[test]
fn an_unmatched_execution_surface_is_a_finding_not_only_an_anomaly() {
    let root = scratch_directory("packet-unmatched-surface");
    let roots: Vec<PathBuf> = (0..5)
        .map(|index| {
            BundleFixture::new(&format!("attempt-{index}"))
                .subject("base", Some("base"), ResultFixture::default())
                .subject(
                    "head",
                    Some("head"),
                    ResultFixture {
                        declared: bench_schema::DeclaredExecution {
                            payload_construction: "built-per-offer".to_owned(),
                            ..crate::fixture::matched_declaration()
                        },
                        ..ResultFixture::default()
                    },
                )
                .write(&root)
        })
        .collect();

    let packet = build_packet(&summarize_suite(&roots, &options()).unwrap());

    assert!(
        packet.deterministic_findings.iter().any(|finding| finding
            .text
            .contains("did not declare the same measured work")),
        "{:?}",
        packet.deterministic_findings
    );
}
