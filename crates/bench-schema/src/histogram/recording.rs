//! The recording side: what an adapter accumulates while a run is happening.

use std::collections::BTreeMap;

use crate::error::SchemaResult;

use super::{
    encoded::EncodedHistogram,
    layout::{HISTOGRAM_LAYOUT_V1, SUB_BUCKET_BITS, bucket_high, bucket_index},
};

/// A recording, mergeable log-linear histogram of nanosecond durations.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Histogram {
    counts: BTreeMap<u32, u64>,
    total: u64,
    min: Option<u64>,
    max: Option<u64>,
    sum: u64,
}

impl Histogram {
    /// Creates an empty histogram.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records one value.
    pub fn record(&mut self, value: u64) {
        *self.counts.entry(bucket_index(value)).or_insert(0) += 1;
        self.total = self.total.saturating_add(1);
        self.sum = self.sum.saturating_add(value);
        self.min = Some(self.min.map_or(value, |m| m.min(value)));
        self.max = Some(self.max.map_or(value, |m| m.max(value)));
    }

    /// Number of recorded values.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.total
    }

    /// Exact smallest recorded value, if any was recorded.
    #[must_use]
    pub fn min(&self) -> Option<u64> {
        self.min
    }

    /// Exact largest recorded value, if any was recorded.
    #[must_use]
    pub fn max(&self) -> Option<u64> {
        self.max
    }

    /// Saturating sum of all recorded values.
    #[must_use]
    pub fn sum(&self) -> u64 {
        self.sum
    }

    /// Merges another histogram into this one.
    pub fn merge(&mut self, other: &Self) {
        for (index, count) in &other.counts {
            *self.counts.entry(*index).or_insert(0) += count;
        }
        self.total = self.total.saturating_add(other.total);
        self.sum = self.sum.saturating_add(other.sum);
        self.min = match (self.min, other.min) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        self.max = match (self.max, other.max) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
    }

    /// The value at or below which `quantile` (in `[0, 1]`) of recordings
    /// fall: the inclusive upper bound of the bucket holding that rank,
    /// clamped to the exact recorded extremes.
    #[must_use]
    pub fn value_at_quantile(&self, quantile: f64) -> Option<u64> {
        if self.total == 0 {
            return None;
        }
        let clamped = quantile.clamp(0.0, 1.0);
        if clamped == 0.0 {
            return self.min;
        }
        #[expect(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "rank arithmetic is exact for every total this harness can record"
        )]
        let rank = ((clamped * self.total as f64).ceil() as u64).max(1);
        let mut cumulative = 0u64;
        for (index, count) in &self.counts {
            // Saturating, because `validate` sums bucket counts the same way:
            // two buckets whose counts overflow a `u64` are accepted there, and
            // a plain `+` here would turn that document into a panic in the
            // reader rather than a number in the report.
            cumulative = cumulative.saturating_add(*count);
            if cumulative >= rank {
                let high = bucket_high(*index);
                return Some(high.min(self.max.unwrap_or(high)));
            }
        }
        self.max
    }

    /// The mean of recorded values, from the exact sum.
    #[must_use]
    pub fn mean(&self) -> Option<f64> {
        if self.total == 0 {
            return None;
        }
        #[expect(
            clippy::cast_precision_loss,
            reason = "reporting statistic, not identity-bearing arithmetic"
        )]
        Some(self.sum as f64 / self.total as f64)
    }

    /// The serializable form of this histogram.
    #[must_use]
    pub fn encode(&self) -> EncodedHistogram {
        EncodedHistogram {
            layout: HISTOGRAM_LAYOUT_V1.to_owned(),
            unit: "ns".to_owned(),
            sub_bucket_bits: SUB_BUCKET_BITS,
            total: self.total,
            min: self.min,
            max: self.max,
            sum: self.sum,
            counts: self.counts.iter().map(|(i, c)| (*i, *c)).collect(),
        }
    }

    /// Rebuilds a histogram from its serialized form.
    ///
    /// # Errors
    ///
    /// Returns an error when the layout, bit width, ordering, or totals are
    /// not internally consistent.
    pub fn decode(encoded: &EncodedHistogram) -> SchemaResult<Self> {
        encoded.validate()?;
        Ok(Self {
            counts: encoded.counts.iter().copied().collect(),
            total: encoded.total,
            min: encoded.min,
            max: encoded.max,
            sum: encoded.sum,
        })
    }
}
