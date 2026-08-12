//! Tests for the gates, including the two boundary cases the noise budget
//! exists to get right.
#![expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "test fixtures assert exact outcomes and must fail immediately"
)]

use std::path::PathBuf;

use crate::fixture::{BundleFixture, ResultFixture, scratch_directory};
use crate::summary::COEFFICIENT_OF_VARIATION_BUDGET;

use super::fixture::{options, roles_suite};
use super::summarize_suite;

/// Writes a five-attempt suite from explicit per-attempt goodputs.
///
/// Latency is held constant so that only goodput can move a dispersion figure,
/// which keeps the two boundary cases below about one number each.
fn suite_from_goodputs(name: &str, series: &[(f64, f64)]) -> Vec<PathBuf> {
    let root = scratch_directory(name);
    series
        .iter()
        .enumerate()
        .map(|(index, (base, head))| {
            BundleFixture::new(&format!("attempt-{index}"))
                .subject(
                    "base",
                    Some("base"),
                    ResultFixture {
                        goodput: *base,
                        terminal_ns: 10_000_000,
                        ..ResultFixture::default()
                    },
                )
                .subject(
                    "head",
                    Some("head"),
                    ResultFixture {
                        goodput: *head,
                        terminal_ns: 10_000_000,
                        ..ResultFixture::default()
                    },
                )
                .write(&root)
        })
        .collect()
}

/// The `dispersion-within-budget` gate of a summary.
fn budget_gate(summary: &bench_schema::SuiteSummary) -> &bench_schema::GateOutcome {
    summary
        .gates
        .iter()
        .find(|gate| gate.name == "dispersion-within-budget")
        .unwrap()
}

/// One dispersion row's coefficient of variation.
fn dispersion_of(summary: &bench_schema::SuiteSummary, name: &str) -> Option<f64> {
    summary
        .dispersion
        .iter()
        .find(|entry| entry.name == name && entry.metric == "acknowledged_records_per_second")
        .unwrap_or_else(|| panic!("no goodput dispersion for {name}"))
        .coefficient_of_variation
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
fn anti_correlated_subjects_fail_the_budget_their_raw_values_would_have_passed() {
    // Each subject moves by ±3.75% of its own mean and they move in opposite
    // directions. Each subject's raw dispersion is therefore about 0.030 —
    // comfortably inside the 0.05 budget — while the ratio swings roughly twice
    // as far, to about 0.059. The comparison is the noisiest thing in the run
    // and gating on raw values called it quiet.
    let drift = [-0.0375, -0.018_75, 0.0, 0.018_75, 0.0375];
    let series: Vec<(f64, f64)> = drift
        .iter()
        .map(|d| (100_000.0 * (1.0 + d), 100_000.0 * (1.0 - d)))
        .collect();

    let summary = summarize_suite(&suite_from_goodputs("suite-anti", &series), &options()).unwrap();

    let base = dispersion_of(&summary, "base").unwrap();
    let head = dispersion_of(&summary, "head").unwrap();
    let ratio = dispersion_of(&summary, "head/base").unwrap();
    assert!(
        base < COEFFICIENT_OF_VARIATION_BUDGET && head < COEFFICIENT_OF_VARIATION_BUDGET,
        "the raw values look quiet: base {base}, head {head}"
    );
    assert!(
        (0.055..0.065).contains(&ratio),
        "the ratio series is the noisy one: {ratio}"
    );

    let gate = budget_gate(&summary);
    assert!(!gate.passed, "{}", gate.detail);
    assert!(
        gate.detail
            .contains("head/base acknowledged_records_per_second"),
        "the gate names the series that failed: {}",
        gate.detail
    );
}

#[test]
fn subjects_that_drift_together_pass_the_budget_their_raw_values_would_have_failed() {
    // A thermal ramp, or a noisy neighbour: both subjects scale by the same
    // factor in each attempt. Every raw dispersion is far outside the budget and
    // every ratio is exactly constant, which is the entire reason the design
    // pairs subjects within an attempt in the first place.
    let scale = [0.85, 0.925, 1.0, 1.075, 1.15];
    let series: Vec<(f64, f64)> = scale
        .iter()
        .map(|f| (100_000.0 * f, 120_000.0 * f))
        .collect();

    let summary =
        summarize_suite(&suite_from_goodputs("suite-drift", &series), &options()).unwrap();

    let base = dispersion_of(&summary, "base").unwrap();
    let ratio = dispersion_of(&summary, "head/base").unwrap();
    assert!(
        base > COEFFICIENT_OF_VARIATION_BUDGET,
        "the machine really did move: {base}"
    );
    assert!(ratio < 1e-12, "every ratio is the same 1.2: {ratio}");

    let gate = budget_gate(&summary);
    assert!(
        gate.passed,
        "pairing is what makes a drifting machine usable: {}",
        gate.detail
    );
}

#[test]
fn a_suite_with_nothing_to_compare_does_not_pass_the_budget_by_default() {
    // One subject means no ratio series, and a gate that passed because it had
    // nothing to judge would be reporting a dispersion nobody measured.
    let root = scratch_directory("suite-single-subject");
    let roots: Vec<PathBuf> = (0..5)
        .map(|index| {
            BundleFixture::new(&format!("attempt-{index}"))
                .subject("only", None, ResultFixture::default())
                .write(&root)
        })
        .collect();

    let summary = summarize_suite(&roots, &options()).unwrap();

    let gate = budget_gate(&summary);
    assert!(!gate.passed, "{}", gate.detail);
    assert!(gate.detail.contains("no compared pair"), "{}", gate.detail);
    assert!(
        summary
            .dispersion
            .iter()
            .all(|entry| !entry.name.contains('/')),
        "there is no ratio series to report"
    );
}
