//! Wall-clock formatting for the status document: the only place this adapter
//! names a calendar.
//!
//! A wall clock names things and a monotonic clock enforces deadlines, so
//! nothing measured passes through here — these strings say when an attempt
//! started and ended, and are never subtracted from one another.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Returns the current time in the shape the status document wants.
pub(super) fn now() -> String {
    utc_rfc3339_millis(SystemTime::now())
}

/// Formats a system time as `2026-08-12T14:03:05.123Z`.
///
/// Duplicated from the control plane on purpose: an adapter that imported
/// `benchctl` to print a timestamp would couple the thing being measured to the
/// thing measuring it, and this adapter is deliberately buildable without the
/// control plane in the graph at all.
pub(crate) fn utc_rfc3339_millis(time: SystemTime) -> String {
    let since_epoch = time.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    let total_seconds = i64::try_from(since_epoch.as_secs()).unwrap_or(i64::MAX);
    let days = total_seconds.div_euclid(86_400);
    let seconds_of_day = total_seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day / 60) % 60;
    let second = seconds_of_day % 60;
    let millisecond = since_epoch.subsec_millis();
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millisecond:03}Z")
}

/// Splits days-since-epoch into a civil (year, month, day).
fn civil_from_days(days: i64) -> (i64, u8, u8) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_position = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_position + 2) / 5 + 1;
    let month = if month_position < 10 {
        month_position + 3
    } else {
        month_position - 9
    };
    let year = if month <= 2 { year + 1 } else { year };
    (
        year,
        u8::try_from(month).unwrap_or(1),
        u8::try_from(day).unwrap_or(1),
    )
}
