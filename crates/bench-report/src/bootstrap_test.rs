//! Properties the paired-block bootstrap has to keep: determinism, coverage of
//! the point estimate, and honesty about degenerate samples.
#![expect(
    clippy::unwrap_used,
    reason = "test fixtures assert exact outcomes and must fail immediately"
)]

use crate::bootstrap::{
    BOOTSTRAP_METHOD, BootstrapOptions, DEFAULT_BOOTSTRAP_RESAMPLES, PairedObservation,
    paired_ratio_interval, paired_ratios,
};
use crate::error::ReportErrorKind;

/// Tolerance for comparing computed statistics against hand-checked values.
const TOLERANCE: f64 = 1e-12;

fn blocks(pairs: &[(f64, f64)]) -> Vec<PairedObservation> {
    pairs
        .iter()
        .map(|(baseline, candidate)| PairedObservation {
            baseline: *baseline,
            candidate: *candidate,
        })
        .collect()
}

/// A small resample count keeps the test suite fast; determinism does not
/// depend on the count, and the default is exercised separately.
fn fast() -> BootstrapOptions {
    BootstrapOptions {
        seed: 42,
        resamples: 2_000,
    }
}

#[test]
fn the_legacy_resample_count_is_preserved() {
    assert_eq!(DEFAULT_BOOTSTRAP_RESAMPLES, 50_000);
    assert_eq!(BOOTSTRAP_METHOD, "paired-block-percentile-bootstrap");
    assert_eq!(
        BootstrapOptions::default().resamples,
        DEFAULT_BOOTSTRAP_RESAMPLES
    );
    assert_eq!(BootstrapOptions::default().seed, 0x6B61_666B);
}

#[test]
fn an_empty_sample_is_an_error() {
    let error = paired_ratio_interval(&[], &fast()).unwrap_err();

    assert_eq!(error.kind(), ReportErrorKind::Sample);
}

#[test]
fn a_zero_or_negative_observation_is_rejected() {
    for pair in [(0.0, 1.0), (1.0, 0.0), (-1.0, 1.0), (1.0, f64::NAN)] {
        let error = paired_ratio_interval(&blocks(&[pair]), &fast()).unwrap_err();

        assert_eq!(error.kind(), ReportErrorKind::Sample);
    }
}

#[test]
fn zero_resamples_is_an_error() {
    let options = BootstrapOptions {
        seed: 1,
        resamples: 0,
    };

    let error = paired_ratio_interval(&blocks(&[(1.0, 1.0)]), &options).unwrap_err();

    assert_eq!(error.kind(), ReportErrorKind::Sample);
}

#[test]
fn ratios_are_candidate_over_baseline() {
    let ratios = paired_ratios(&blocks(&[(100.0, 110.0), (200.0, 180.0)])).unwrap();

    assert!((ratios[0] - 1.1).abs() < TOLERANCE);
    assert!((ratios[1] - 0.9).abs() < TOLERANCE);
}

#[test]
fn the_point_estimate_is_the_geometric_mean_of_the_block_ratios() {
    let interval = paired_ratio_interval(&blocks(&[(1.0, 2.0), (1.0, 8.0)]), &fast()).unwrap();

    // geometric mean of 2 and 8 is 4, where the arithmetic mean would be 5.
    assert!((interval.point_ratio - 4.0).abs() < TOLERANCE);
}

#[test]
fn the_same_inputs_produce_an_identical_interval() {
    let sample = blocks(&[
        (1000.0, 1100.0),
        (1010.0, 1120.0),
        (990.0, 1080.0),
        (1005.0, 1095.0),
        (995.0, 1105.0),
    ]);

    let first = paired_ratio_interval(&sample, &fast()).unwrap();
    let second = paired_ratio_interval(&sample, &fast()).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.lower.to_bits(), second.lower.to_bits());
    assert_eq!(first.upper.to_bits(), second.upper.to_bits());
}

#[test]
fn a_different_seed_may_move_the_interval_but_not_the_point_estimate() {
    let sample = blocks(&[
        (1000.0, 1100.0),
        (1010.0, 1050.0),
        (990.0, 1180.0),
        (1005.0, 1015.0),
        (995.0, 1305.0),
    ]);
    let other = BootstrapOptions {
        seed: 9_999,
        resamples: 2_000,
    };

    let first = paired_ratio_interval(&sample, &fast()).unwrap();
    let second = paired_ratio_interval(&sample, &other).unwrap();

    assert!((first.point_ratio - second.point_ratio).abs() < TOLERANCE);
    assert_eq!(first.seed, 42);
    assert_eq!(second.seed, 9_999);
}

#[test]
fn the_interval_contains_the_point_estimate_for_stable_data() {
    let sample = blocks(&[
        (1000.0, 1100.0),
        (1002.0, 1101.0),
        (998.0, 1099.0),
        (1001.0, 1102.0),
        (999.0, 1098.0),
        (1000.0, 1100.0),
        (1003.0, 1103.0),
    ]);

    let interval = paired_ratio_interval(&sample, &fast()).unwrap();

    assert!(
        interval.contains(interval.point_ratio),
        "interval [{}, {}] excludes its own point estimate {}",
        interval.lower,
        interval.upper,
        interval.point_ratio
    );
    assert!(interval.lower <= interval.upper);
}

#[test]
fn the_interval_contains_the_point_estimate_for_noisy_data() {
    let sample = blocks(&[
        (1000.0, 400.0),
        (1000.0, 2500.0),
        (1000.0, 900.0),
        (1000.0, 1600.0),
        (1000.0, 700.0),
    ]);

    let interval = paired_ratio_interval(&sample, &fast()).unwrap();

    assert!(interval.contains(interval.point_ratio));
}

#[test]
fn an_unambiguous_win_keeps_the_whole_interval_above_parity() {
    let sample = blocks(&[
        (1000.0, 1300.0),
        (1000.0, 1310.0),
        (1000.0, 1290.0),
        (1000.0, 1305.0),
        (1000.0, 1295.0),
    ]);

    let interval = paired_ratio_interval(&sample, &fast()).unwrap();

    // The blocks sit around a 1.30 ratio, so the interval clears a five percent
    // practical threshold outright and falls short of a thirty-five percent one.
    assert!(interval.entirely_above(1.0));
    assert!(!interval.entirely_below(1.0));
    assert!(interval.entirely_above(1.05));
    assert!(!interval.entirely_above(1.35));
}

#[test]
fn an_unambiguous_regression_keeps_the_whole_interval_below_parity() {
    let sample = blocks(&[
        (1000.0, 700.0),
        (1000.0, 710.0),
        (1000.0, 690.0),
        (1000.0, 705.0),
        (1000.0, 695.0),
    ]);

    let interval = paired_ratio_interval(&sample, &fast()).unwrap();

    assert!(interval.entirely_below(1.0));
    assert!(!interval.entirely_above(1.0));
}

#[test]
fn a_mixed_sample_straddles_parity() {
    let sample = blocks(&[
        (1000.0, 1300.0),
        (1000.0, 700.0),
        (1000.0, 1200.0),
        (1000.0, 800.0),
        (1000.0, 1000.0),
    ]);

    let interval = paired_ratio_interval(&sample, &fast()).unwrap();

    assert!(interval.contains(1.0));
    assert!(!interval.entirely_above(1.0));
    assert!(!interval.entirely_below(1.0));
}

#[test]
fn identical_blocks_produce_a_zero_width_interval() {
    let sample = blocks(&[(1000.0, 1100.0); 5]);

    let interval = paired_ratio_interval(&sample, &fast()).unwrap();

    assert!((interval.lower - 1.1).abs() < TOLERANCE);
    assert!((interval.upper - 1.1).abs() < TOLERANCE);
    assert!((interval.log_midpoint() - 1.1).abs() < TOLERANCE);
}

#[test]
fn a_single_block_is_degenerate_and_says_so() {
    let interval = paired_ratio_interval(&blocks(&[(1000.0, 1234.0)]), &fast()).unwrap();

    assert_eq!(interval.blocks, 1);
    assert!((interval.lower - interval.upper).abs() < TOLERANCE);
    assert!((interval.point_ratio - 1.234).abs() < TOLERANCE);
}

#[test]
fn inverting_the_comparison_inverts_the_point_estimate() {
    let forward = paired_ratio_interval(
        &blocks(&[(1000.0, 1300.0), (1100.0, 1200.0), (900.0, 1400.0)]),
        &fast(),
    )
    .unwrap();
    let backward = paired_ratio_interval(
        &blocks(&[(1300.0, 1000.0), (1200.0, 1100.0), (1400.0, 900.0)]),
        &fast(),
    )
    .unwrap();

    assert!((forward.point_ratio * backward.point_ratio - 1.0).abs() < 1e-9);
}

#[test]
fn the_default_resample_count_still_produces_a_usable_interval() {
    let sample = blocks(&[
        (1000.0, 1100.0),
        (1010.0, 1120.0),
        (990.0, 1080.0),
        (1005.0, 1095.0),
        (995.0, 1105.0),
    ]);

    let interval = paired_ratio_interval(&sample, &BootstrapOptions::default()).unwrap();

    assert_eq!(interval.resamples, DEFAULT_BOOTSTRAP_RESAMPLES);
    assert_eq!(interval.blocks, 5);
    assert!(interval.contains(interval.point_ratio));
    assert!(interval.entirely_above(1.0));
}
