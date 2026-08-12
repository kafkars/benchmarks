//! Vector tests for the hand-rolled UTC formatting, including the epoch,
//! a leap day, and pre-epoch saturation.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::time::{utc_compact_seconds, utc_rfc3339_millis};

fn at(seconds: u64, millis: u32) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(seconds) + Duration::from_millis(u64::from(millis))
}

#[test]
fn epoch_formats_as_1970() {
    assert_eq!(utc_rfc3339_millis(UNIX_EPOCH), "1970-01-01T00:00:00.000Z");
    assert_eq!(utc_compact_seconds(UNIX_EPOCH), "19700101T000000Z");
}

#[test]
fn leap_day_2024_is_exact() {
    // 2024-02-29T12:34:56Z: 2024-01-01 is 1704067200; +31 (Jan) +28 (Feb) days
    // reaches 2024-02-29 00:00 = 1709164800; +45296 seconds is 12:34:56.
    let time = at(1_709_164_800 + 45_296, 789);
    assert_eq!(utc_rfc3339_millis(time), "2024-02-29T12:34:56.789Z");
    assert_eq!(utc_compact_seconds(time), "20240229T123456Z");
}

#[test]
fn august_2026_matches_known_epoch_arithmetic() {
    // 2026-01-01 is 1767225600; Jan..Jul sum to 212 days; +11 days is Aug 12.
    let midnight = 1_767_225_600 + (212 + 11) * 86_400;
    assert_eq!(
        utc_rfc3339_millis(at(midnight, 0)),
        "2026-08-12T00:00:00.000Z"
    );
    assert_eq!(utc_compact_seconds(at(midnight, 0)), "20260812T000000Z");
}

#[test]
fn year_boundary_rolls_over() {
    // 2025-12-31T23:59:59.999Z is one second before 2026-01-01.
    let time = at(1_767_225_600 - 1, 999);
    assert_eq!(utc_rfc3339_millis(time), "2025-12-31T23:59:59.999Z");
    assert_eq!(utc_compact_seconds(time), "20251231T235959Z");
}

#[test]
fn pre_epoch_saturates_to_epoch() {
    let before = UNIX_EPOCH - Duration::from_secs(86_400);
    assert_eq!(utc_rfc3339_millis(before), "1970-01-01T00:00:00.000Z");
    assert_eq!(utc_compact_seconds(before), "19700101T000000Z");
}
