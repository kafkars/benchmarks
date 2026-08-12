//! The reset-on-emit rolling windows librdkafka reports batching through.
//!
//! These are the one part of the statistics stream that is *not* differenced
//! between two snapshots. librdkafka clears a window every time it emits it, so
//! the run's batching is the sum over the snapshots inside the measured window.
//! Getting that backwards silently reports one snapshot's worth of batching as
//! the whole run's.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::snapshot::{counter, statistics_of};

/// One reset-on-emit rolling window aggregated across the measured snapshots.
///
/// librdkafka clears these windows every time it emits them, so the aggregate
/// is a sum over snapshots, not a difference between two of them. Getting that
/// backwards silently reports one snapshot's worth of batching as the whole
/// run's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchWindow {
    /// Observations the windows counted in total: the number of batches.
    pub samples: u64,
    /// Sum of the observed values across every window.
    pub total: u64,
    /// Smallest value any window reported, when any window reported one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum: Option<u64>,
    /// Largest value any window reported, when any window reported one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum: Option<u64>,
}

impl BatchWindow {
    /// Returns the mean observation, or `None` when nothing was observed.
    #[must_use]
    #[expect(
        clippy::cast_precision_loss,
        reason = "reporting statistic over counter values, not identity arithmetic"
    )]
    pub fn mean(&self) -> Option<f64> {
        if self.samples == 0 {
            None
        } else {
            Some(self.total as f64 / self.samples as f64)
        }
    }
}

/// Totals one batch window across several attempts, absent when any part is.
pub(super) fn total_window(
    parts: &[super::RequestEconomics],
    read: fn(&super::RequestEconomics) -> Option<BatchWindow>,
) -> Option<BatchWindow> {
    let mut total = BatchWindow {
        samples: 0,
        total: 0,
        minimum: None,
        maximum: None,
    };
    for part in parts {
        let window = read(part)?;
        total.samples = total.samples.saturating_add(window.samples);
        total.total = total.total.saturating_add(window.total);
        total.minimum = match (total.minimum, window.minimum) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        };
        total.maximum = match (total.maximum, window.maximum) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (left, right) => left.or(right),
        };
    }
    Some(total)
}

/// Aggregates one reset-on-emit topic window across the measured snapshots.
pub(super) fn aggregate_topic_window(
    windows: &[&Value],
    measured_topic: Option<&str>,
    key: &str,
) -> Option<BatchWindow> {
    let mut samples = 0u64;
    let mut total = 0u64;
    let mut minimum: Option<u64> = None;
    let mut maximum: Option<u64> = None;
    let mut seen = false;
    for snapshot in windows {
        let Some(topics) = statistics_of(Some(snapshot))
            .and_then(|statistics| statistics.get("topics"))
            .and_then(Value::as_object)
        else {
            continue;
        };
        for (name, topic) in topics {
            if measured_topic.is_some_and(|wanted| wanted != name) {
                continue;
            }
            let Some(window) = topic.get(key) else {
                continue;
            };
            seen = true;
            let count = counter(Some(window), "cnt").unwrap_or(0);
            if count == 0 {
                continue;
            }
            samples = samples.saturating_add(count);
            total = total.saturating_add(counter(Some(window), "sum").unwrap_or(0));
            if let Some(low) = counter(Some(window), "min") {
                minimum = Some(minimum.map_or(low, |current: u64| current.min(low)));
            }
            if let Some(high) = counter(Some(window), "max") {
                maximum = Some(maximum.map_or(high, |current: u64| current.max(high)));
            }
        }
    }
    seen.then_some(BatchWindow {
        samples,
        total,
        minimum,
        maximum,
    })
}
