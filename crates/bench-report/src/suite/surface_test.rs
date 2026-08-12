//! Tests for the matched-execution-surface gate: which declared differences
//! stop a comparison, and which are reported and kept.
#![expect(
    clippy::unwrap_used,
    reason = "test fixtures assert exact outcomes and must fail immediately"
)]

use std::path::PathBuf;

use bench_schema::{DeclaredExecution, GateOutcome, SuiteSummary};

use crate::fixture::{BundleFixture, ResultFixture, matched_declaration, scratch_directory};

use super::fixture::options;
use super::summarize_suite;
use super::surface::GATE_NAME;

/// Writes a two-attempt role-labelled suite whose head subject declares
/// `head_declaration`.
fn suite_with(name: &str, head_declaration: &DeclaredExecution) -> Vec<PathBuf> {
    let root = scratch_directory(name);
    (0..2)
        .map(|index| {
            BundleFixture::new(&format!("attempt-{index}"))
                .subject("base", Some("base"), ResultFixture::default())
                .subject(
                    "head",
                    Some("head"),
                    ResultFixture {
                        declared: head_declaration.clone(),
                        ..ResultFixture::default()
                    },
                )
                .write(&root)
        })
        .collect()
}

/// The matched-execution-surface gate of a summary.
fn surface_gate(summary: &SuiteSummary) -> &GateOutcome {
    summary
        .gates
        .iter()
        .find(|gate| gate.name == GATE_NAME)
        .unwrap()
}

#[test]
fn two_subjects_declaring_the_same_work_pass_the_gate() {
    let summary = summarize_suite(
        &suite_with("surface-matched", &matched_declaration()),
        &options(),
    )
    .unwrap();

    let gate = surface_gate(&summary);

    assert!(gate.passed, "{}", gate.detail);
    assert!(
        gate.detail.contains("declare the same measured work"),
        "{}",
        gate.detail
    );
    assert!(
        !summary
            .notes
            .iter()
            .any(|note| note.contains("product-surface difference")),
        "{:?}",
        summary.notes
    );
}

#[test]
fn a_payload_construction_mismatch_fails_and_names_both_values() {
    let summary = summarize_suite(
        &suite_with(
            "surface-construction",
            &DeclaredExecution {
                payload_construction: "built-per-offer-inside-the-measured-interval".to_owned(),
                ..matched_declaration()
            },
        ),
        &options(),
    )
    .unwrap();

    let gate = surface_gate(&summary);

    assert!(!gate.passed, "{}", gate.detail);
    assert!(
        gate.detail.contains("payload_construction"),
        "{}",
        gate.detail
    );
    assert!(
        gate.detail
            .contains("built-per-offer-inside-the-measured-interval"),
        "the gate names what head declared: {}",
        gate.detail
    );
    assert!(
        gate.detail.contains("prebuilt-pool"),
        "the gate names what base declared: {}",
        gate.detail
    );
}

#[test]
fn a_serialization_mismatch_fails_too() {
    let summary = summarize_suite(
        &suite_with(
            "surface-serialization",
            &DeclaredExecution {
                serialization: "included".to_owned(),
                ..matched_declaration()
            },
        ),
        &options(),
    )
    .unwrap();

    let gate = surface_gate(&summary);

    assert!(!gate.passed, "{}", gate.detail);
    assert!(gate.detail.contains("serialization"), "{}", gate.detail);
    assert!(gate.detail.contains("included"), "{}", gate.detail);
    assert!(gate.detail.contains("excluded"), "{}", gate.detail);
}

#[test]
fn an_ownership_difference_is_noted_and_not_failed() {
    // The two shipped adapters really do differ here, because their clients'
    // public APIs differ. Refusing the comparison would refuse to compare the
    // clients as they exist; erasing the difference would hide it.
    let summary = summarize_suite(
        &suite_with(
            "surface-ownership",
            &DeclaredExecution {
                ownership: "owned-per-offer-from-pool".to_owned(),
                completion_mode: "delivery-callback".to_owned(),
                ..matched_declaration()
            },
        ),
        &options(),
    )
    .unwrap();

    let gate = surface_gate(&summary);

    assert!(gate.passed, "{}", gate.detail);
    let notes: Vec<&String> = summary
        .notes
        .iter()
        .filter(|note| note.contains("product-surface difference"))
        .collect();
    assert_eq!(notes.len(), 2, "{:?}", summary.notes);
    assert!(
        notes.iter().any(|note| note.contains("ownership")),
        "{notes:?}"
    );
    assert!(
        notes.iter().any(|note| note.contains("completion_mode")),
        "{notes:?}"
    );
    assert!(
        notes
            .iter()
            .any(|note| note.contains("owned-per-offer-from-pool")),
        "{notes:?}"
    );
    // The pairs are still there: a noted difference qualifies a comparison, it
    // does not withdraw one.
    assert!(!summary.pairs.is_empty());
}

#[test]
fn the_two_attribution_metrics_are_paired_but_never_gated() {
    let summary = summarize_suite(
        &suite_with("surface-attribution", &matched_declaration()),
        &options(),
    )
    .unwrap();

    for field in [
        "p99_accepted_to_terminal_ns",
        "p99_intended_to_call_start_ns",
    ] {
        assert!(
            !summary.gates.iter().any(|gate| gate.name.contains(field)),
            "{field} must not carry a pass/fail gate"
        );
    }
    assert!(
        summary
            .pairs
            .iter()
            .any(|pair| pair.metric == "p99_accepted_to_terminal_ns"),
        "the client-internal portion is still compared, for locating a difference"
    );
    // The closed-loop fixture has no schedule, so lateness is absent rather
    // than zero, and no pair can be formed from it.
    assert!(
        !summary
            .pairs
            .iter()
            .any(|pair| pair.metric == "p99_intended_to_call_start_ns"),
        "a closed-loop run has no schedule to be late against"
    );
    assert!(
        summary
            .medians
            .iter()
            .all(|median| median.p99_intended_to_call_start_ns.is_none())
    );
}
