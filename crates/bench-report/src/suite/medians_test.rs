//! Tests for the medians and for the two kinds of dispersion row beside them.
#![expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "test fixtures assert exact outcomes and must fail immediately"
)]

use super::fixture::{options, roles_suite};
use super::{SuiteMetric, summarize_suite};

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
fn dispersion_is_reported_for_every_metric_of_every_subject_and_every_pair() {
    let roots = roles_suite("suite-dispersion-many", 5, 1.20);

    let summary = summarize_suite(&roots, &options()).unwrap();

    // Two subjects and one comparison, each over all seven metrics. The
    // per-subject rows are informational; the `head/base` rows are the ones the
    // budget gate reads.
    assert_eq!(summary.dispersion.len(), 3 * SuiteMetric::ALL.len());
    let row = |name: &str| {
        summary
            .dispersion
            .iter()
            .find(|entry| entry.name == name && entry.metric == "acknowledged_records_per_second")
            .unwrap_or_else(|| panic!("no goodput dispersion for {name}"))
            .coefficient_of_variation
            .unwrap()
    };
    assert!(row("head") < 0.05);
    assert!(row("base") < 0.05);
    assert!(row("head/base") < 0.05);
    assert!(
        summary
            .dispersion
            .iter()
            .position(|entry| entry.name == "head/base")
            .unwrap()
            > summary
                .dispersion
                .iter()
                .rposition(|entry| entry.name == "base")
                .unwrap(),
        "the gated rows come after the informational ones"
    );
}
