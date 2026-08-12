//! Tests for the two central tendencies, including the cases where absence is
//! the right answer.
#![expect(
    clippy::unwrap_used,
    reason = "test fixtures assert exact outcomes and must fail immediately"
)]

use crate::stats::{geometric_mean, median};

/// Tolerance for comparing computed statistics against hand-checked values.
const TOLERANCE: f64 = 1e-12;

#[test]
fn an_empty_sample_has_no_median() {
    assert_eq!(median(&[]), None);
}

#[test]
fn an_odd_sample_takes_the_middle_value() {
    assert!((median(&[3.0, 1.0, 2.0]).unwrap() - 2.0).abs() < TOLERANCE);
}

#[test]
fn an_even_sample_averages_the_two_middle_values() {
    assert!((median(&[4.0, 1.0, 3.0, 2.0]).unwrap() - 2.5).abs() < TOLERANCE);
}

#[test]
fn the_median_does_not_reorder_the_caller_s_slice() {
    let values = [3.0, 1.0, 2.0];

    let _ = median(&values);

    for (actual, expected) in values.iter().zip([3.0, 1.0, 2.0]) {
        assert!((actual - expected).abs() < TOLERANCE);
    }
}

#[test]
fn an_empty_sample_has_no_geometric_mean() {
    assert_eq!(geometric_mean(&[]), None);
}

#[test]
fn a_non_positive_value_has_no_geometric_mean() {
    assert_eq!(geometric_mean(&[1.0, 0.0]), None);
    assert_eq!(geometric_mean(&[1.0, -2.0]), None);
    assert_eq!(geometric_mean(&[1.0, f64::NAN]), None);
    assert_eq!(geometric_mean(&[f64::INFINITY]), None);
}

#[test]
fn the_geometric_mean_is_the_nth_root_of_the_product() {
    assert!((geometric_mean(&[2.0, 8.0]).unwrap() - 4.0).abs() < TOLERANCE);
    assert!((geometric_mean(&[1.0, 1.0, 1.0]).unwrap() - 1.0).abs() < TOLERANCE);
}

#[test]
fn the_geometric_mean_is_symmetric_under_inversion() {
    let forward = geometric_mean(&[2.0, 0.5, 4.0]).unwrap();
    let backward = geometric_mean(&[0.5, 2.0, 0.25]).unwrap();

    assert!((forward * backward - 1.0).abs() < 1e-12);
}
