//! Timestamp vectors, including the leap-year and pre-epoch corners.
//!
//! The formatter is a copy of the control plane's, so it is tested against the
//! same kind of hand-checked vectors rather than against the original — two
//! implementations that agree because they were tested the same way are worth
//! more than two that agree because one imported the other.
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::time::utc_rfc3339_millis;

fn at(seconds: u64, millis: u32) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(seconds) + Duration::from_millis(u64::from(millis))
}

#[test]
fn the_epoch_formats_as_1970() {
    assert_eq!(utc_rfc3339_millis(UNIX_EPOCH), "1970-01-01T00:00:00.000Z");
}

#[test]
fn milliseconds_are_kept_and_zero_padded() {
    assert_eq!(utc_rfc3339_millis(at(0, 7)), "1970-01-01T00:00:00.007Z");
    assert_eq!(utc_rfc3339_millis(at(0, 999)), "1970-01-01T00:00:00.999Z");
}

#[test]
fn a_leap_day_is_exact() {
    // 2024-02-29T12:34:56Z
    assert_eq!(
        utc_rfc3339_millis(at(1_709_210_096, 0)),
        "2024-02-29T12:34:56.000Z"
    );
}

#[test]
fn a_year_boundary_rolls_over() {
    // 2025-12-31T23:59:59Z and one second later.
    assert_eq!(
        utc_rfc3339_millis(at(1_767_225_599, 0)),
        "2025-12-31T23:59:59.000Z"
    );
    assert_eq!(
        utc_rfc3339_millis(at(1_767_225_600, 0)),
        "2026-01-01T00:00:00.000Z"
    );
}

#[test]
fn a_pre_epoch_time_saturates_rather_than_panicking() {
    let before = UNIX_EPOCH - Duration::from_secs(86_400);

    assert_eq!(utc_rfc3339_millis(before), "1970-01-01T00:00:00.000Z");
}
