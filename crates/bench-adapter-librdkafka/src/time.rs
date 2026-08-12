//! UTC RFC 3339 timestamps with millisecond precision, for the adapter status
//! document.
//!
//! This is a deliberate duplicate of the control plane's formatter rather than
//! a shared dependency. An adapter that imported `benchctl` to print a
//! timestamp would couple the thing being measured to the thing measuring it,
//! and the protocol's whole claim is that an adapter is a separate program
//! somebody else could have written. Fifty lines of civil-date arithmetic is a
//! cheaper price than that coupling.
//!
//! The algorithm is Howard Hinnant's `civil_from_days`, exact across the whole
//! supported range. Times before the Unix epoch saturate to the epoch: an
//! adapter never runs before 1970, and a wildly wrong clock should produce a
//! legible timestamp rather than a panic.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Formats a system time as `2026-08-12T14:03:05.123Z`.
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

/// Returns the current time formatted for a status document.
pub(crate) fn now() -> String {
    utc_rfc3339_millis(SystemTime::now())
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
