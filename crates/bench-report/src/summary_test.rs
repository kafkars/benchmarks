//! Tests pinning the summary semantics inherited from `statistics.mjs`.
#![expect(
    clippy::unwrap_used,
    reason = "test fixtures assert exact summary outcomes and must fail immediately"
)]

use crate::{
    COEFFICIENT_OF_VARIATION_BUDGET, MINIMUM_PAIRED_REPETITIONS, SummaryError,
    summarize_positive_values,
};

/// Tolerance for comparing computed statistics against hand-checked values.
const TOLERANCE: f64 = 1e-12;

fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < TOLERANCE,
        "expected {expected}, got {actual}"
    );
}

#[test]
fn thresholds_match_the_legacy_control_plane() {
    assert_eq!(MINIMUM_PAIRED_REPETITIONS, 5);
    assert_close(COEFFICIENT_OF_VARIATION_BUDGET, 0.05);
}

#[test]
fn an_empty_sample_is_an_error() {
    assert_eq!(summarize_positive_values(&[]), Err(SummaryError::Empty));
}

#[test]
fn a_zero_value_is_rejected() {
    assert_eq!(
        summarize_positive_values(&[1.0, 0.0, 2.0]),
        Err(SummaryError::NotFinitePositive)
    );
}

#[test]
fn a_negative_value_is_rejected() {
    assert_eq!(
        summarize_positive_values(&[-1.0]),
        Err(SummaryError::NotFinitePositive)
    );
}

#[test]
fn a_non_finite_value_is_rejected() {
    assert_eq!(
        summarize_positive_values(&[1.0, f64::NAN]),
        Err(SummaryError::NotFinitePositive)
    );
    assert_eq!(
        summarize_positive_values(&[f64::INFINITY]),
        Err(SummaryError::NotFinitePositive)
    );
}

#[test]
fn a_single_repetition_leaves_dispersion_undefined() {
    let summary = summarize_positive_values(&[42.0]).unwrap();

    assert_eq!(summary.repetitions, 1);
    assert_close(summary.arithmetic_mean, 42.0);
    assert_eq!(summary.coefficient_of_variation, None);
    assert!(!summary.inside_noise_budget());
    assert!(!summary.enough_repetitions());
}

#[test]
fn dispersion_uses_the_sample_standard_deviation() {
    // mean 4, squared differences 4 + 1 + 0 + 1 + 4 = 10, n - 1 = 4,
    // sample standard deviation sqrt(2.5), coefficient of variation
    // sqrt(2.5) / 4.
    let summary = summarize_positive_values(&[2.0, 3.0, 4.0, 5.0, 6.0]).unwrap();

    assert_eq!(summary.repetitions, 5);
    assert_close(summary.arithmetic_mean, 4.0);
    assert_close(
        summary.coefficient_of_variation.unwrap(),
        2.5_f64.sqrt() / 4.0,
    );
    assert!(summary.enough_repetitions());
    assert!(!summary.inside_noise_budget());
}

#[test]
fn an_identical_sample_has_no_dispersion() {
    let summary = summarize_positive_values(&[1000.0; 5]).unwrap();

    assert_close(summary.coefficient_of_variation.unwrap(), 0.0);
    assert!(summary.inside_noise_budget());
    assert!(summary.enough_repetitions());
}

#[test]
fn the_noise_budget_boundary_is_inclusive() {
    // Two values symmetric about 100 whose sample standard deviation is exactly
    // five percent of the mean: sd = |a - b| / sqrt(2).
    let half_spread = 0.05 * 100.0 * 2.0_f64.sqrt() / 2.0;
    let summary = summarize_positive_values(&[100.0 - half_spread, 100.0 + half_spread]).unwrap();

    assert_close(
        summary.coefficient_of_variation.unwrap(),
        COEFFICIENT_OF_VARIATION_BUDGET,
    );
    assert!(summary.inside_noise_budget());
    assert!(!summary.enough_repetitions());
}

#[test]
fn a_summary_serializes_with_the_legacy_keys() {
    let summary = summarize_positive_values(&[1000.0; 5]).unwrap();

    let encoded = serde_json::to_string(&summary).unwrap();

    assert_eq!(
        encoded,
        r#"{"repetitions":5,"arithmetic_mean":1000.0,"coefficient_of_variation":0.0}"#
    );
}

#[test]
fn undefined_dispersion_serializes_as_null() {
    let summary = summarize_positive_values(&[7.5]).unwrap();

    let encoded = serde_json::to_string(&summary).unwrap();

    assert_eq!(
        encoded,
        r#"{"repetitions":1,"arithmetic_mean":7.5,"coefficient_of_variation":null}"#
    );
}
