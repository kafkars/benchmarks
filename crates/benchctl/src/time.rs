//! UTC timestamp formatting from [`SystemTime`], hand-rolled so the control
//! plane needs no calendar dependency.
//!
//! Two shapes are produced: RFC 3339 with millisecond precision for document
//! fields (`2026-08-12T14:03:05.123Z`) and a compact seconds form for
//! filesystem names (`20260812T140305Z`). The civil-date arithmetic is Howard
//! Hinnant's `civil_from_days` algorithm, exact over the whole supported range.
//! Times before the Unix epoch saturate to the epoch: the control plane never
//! runs before 1970, and a wildly wrong clock should produce a legible, sorted
//! name rather than a panic.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A civil UTC date-time split into its components.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Civil {
    year: i64,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    millisecond: u16,
}

/// Splits days-since-epoch into (year, month, day) — Hinnant `civil_from_days`.
fn civil_from_days(days: i64) -> (i64, u8, u8) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year + 1 } else { year };
    (
        year,
        u8::try_from(month).unwrap_or(1),
        u8::try_from(day).unwrap_or(1),
    )
}

/// Converts a [`SystemTime`] to civil UTC components, saturating pre-epoch
/// times to the epoch.
fn civil_utc(time: SystemTime) -> Civil {
    let since_epoch = time.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    let total_seconds = i64::try_from(since_epoch.as_secs()).unwrap_or(i64::MAX);
    let (days, seconds_of_day) = (
        total_seconds.div_euclid(86_400),
        total_seconds.rem_euclid(86_400),
    );
    let (year, month, day) = civil_from_days(days);
    Civil {
        year,
        month,
        day,
        hour: u8::try_from(seconds_of_day / 3600).unwrap_or(0),
        minute: u8::try_from((seconds_of_day / 60) % 60).unwrap_or(0),
        second: u8::try_from(seconds_of_day % 60).unwrap_or(0),
        millisecond: u16::try_from(u128::from(since_epoch.subsec_millis())).unwrap_or(0),
    }
}

/// Formats a [`SystemTime`] as RFC 3339 UTC with milliseconds,
/// e.g. `2026-08-12T14:03:05.123Z`.
#[must_use]
pub fn utc_rfc3339_millis(time: SystemTime) -> String {
    let c = civil_utc(time);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        c.year, c.month, c.day, c.hour, c.minute, c.second, c.millisecond
    )
}

/// Formats a [`SystemTime`] as compact UTC seconds, e.g. `20260812T140305Z` —
/// the shape attempt directory names embed.
#[must_use]
pub fn utc_compact_seconds(time: SystemTime) -> String {
    let c = civil_utc(time);
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        c.year, c.month, c.day, c.hour, c.minute, c.second
    )
}
