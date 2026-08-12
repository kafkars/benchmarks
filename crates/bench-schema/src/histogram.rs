//! The shared latency histogram: log-linear bucketing that the Rust and C
//! adapters must implement bit-for-bit identically, and that readers use to
//! derive every percentile.
//!
//! # Why a hand-rolled layout
//!
//! Evidence memory must not scale with run length, and two languages must
//! produce byte-identical encodings so cross-language conformance can assert
//! equality instead of tolerance. Both rule out an off-the-shelf dependency:
//! the layout has to be small enough to restate in portable C and stable
//! enough to pin with golden vectors.
//!
//! # Layout `kafkars.log-linear.v1`
//!
//! With `SUB_BUCKET_BITS = 7` (128 linear sub-buckets per power of two):
//!
//! - values below 128 get their own exact bucket: `index = value`;
//! - for larger values, `k = bit_length(value) - 1 - 7` halvings bring the
//!   value into `[128, 256)`, and `index = (k + 1) << 7 | ((value >> k) - 128)`.
//!
//! Every bucket at scale `k` spans `2^k` values, so the worst-case relative
//! error is `2^-7` (0.78%). Percentiles report a bucket's inclusive **upper**
//! bound — the highest value the recorded sample could have had — so derived
//! latencies err conservative, never flattering. `min`, `max`, and `sum` are
//! tracked exactly, outside the buckets.
//!
//! The serialized form is an embedded object (not a standalone document):
//! fields in the fixed order `layout`, `unit`, `sub_bucket_bits`, `total`,
//! `min`, `max`, `sum`, `counts`, where `counts` is a sparse
//! `[[index, count], …]` with strictly ascending indexes and no zero counts.
//! Compact serde output of this struct is the byte-exact reference encoding
//! the C implementation must reproduce.
//!
//! # The index range is finite, and validation enforces it
//!
//! `u64` is a bounded domain, so the layout is too: the largest index any value
//! can land in is [`MAX_BUCKET_INDEX`], which is `bucket_index(u64::MAX)`.
//! Indexes above it are not "rare"; they are impossible to record and can only
//! arrive from a document somebody wrote by hand. Decoding one would produce
//! bounds no value could have had, and far enough out it would shift by more
//! than a `u64` has bits. [`EncodedHistogram::validate`] therefore rejects any
//! index above [`MAX_BUCKET_INDEX`], and [`bucket_low`]/[`bucket_high`] are
//! total over every `u32` regardless, so that a caller who skipped validation
//! still cannot be made to panic by evidence it was handed.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::{SchemaError, SchemaResult};

/// The layout discriminator every histogram carries.
pub const HISTOGRAM_LAYOUT_V1: &str = "kafkars.log-linear.v1";

/// Linear sub-buckets per power of two, as a bit count.
pub const SUB_BUCKET_BITS: u32 = 7;

/// Linear sub-buckets per power of two.
pub const SUB_BUCKET_COUNT: u64 = 1 << SUB_BUCKET_BITS;

/// The largest index this layout can produce, which is `bucket_index(u64::MAX)`.
///
/// Derived from the layout rather than written down as a number: the widest
/// value has a bit length of 64, so its scale is `64 - 1 - SUB_BUCKET_BITS`, its
/// bucket number is one more than that, and its sub-bucket is the last of
/// [`SUB_BUCKET_COUNT`]. A test pins this expression against `bucket_index`
/// itself, so the two cannot drift.
pub const MAX_BUCKET_INDEX: u32 =
    ((64 - SUB_BUCKET_BITS) << SUB_BUCKET_BITS) | ((1 << SUB_BUCKET_BITS) - 1);

/// Returns the bucket index for a value.
#[must_use]
pub fn bucket_index(value: u64) -> u32 {
    if value < SUB_BUCKET_COUNT {
        u32::try_from(value).unwrap_or(u32::MAX)
    } else {
        let bit_length = 64 - value.leading_zeros();
        let scale = bit_length - 1 - SUB_BUCKET_BITS;
        let sub = u32::try_from((value >> scale) - SUB_BUCKET_COUNT).unwrap_or(u32::MAX);
        ((scale + 1) << SUB_BUCKET_BITS) | sub
    }
}

/// Returns the smallest value a bucket contains.
///
/// Total over every `u32`. An index above [`MAX_BUCKET_INDEX`] names no bucket
/// this layout can produce, so it saturates to the top bucket rather than
/// shifting past the width of a `u64`. That is a deliberate choice about who
/// pays for a malformed document: [`EncodedHistogram::validate`] rejects such an
/// index with a message, and this function refuses to panic for anyone who
/// reached it without validating first.
#[must_use]
pub fn bucket_low(index: u32) -> u64 {
    let index = index.min(MAX_BUCKET_INDEX);
    if u64::from(index) < SUB_BUCKET_COUNT {
        u64::from(index)
    } else {
        let scale = (index >> SUB_BUCKET_BITS) - 1;
        (u64::from(index & (u32::try_from(SUB_BUCKET_COUNT).unwrap_or(u32::MAX) - 1))
            + SUB_BUCKET_COUNT)
            << scale
    }
}

/// Returns the largest value a bucket contains.
///
/// Total over every `u32`, on the same terms as [`bucket_low`].
#[must_use]
pub fn bucket_high(index: u32) -> u64 {
    let index = index.min(MAX_BUCKET_INDEX);
    if u64::from(index) < SUB_BUCKET_COUNT {
        u64::from(index)
    } else {
        let scale = (index >> SUB_BUCKET_BITS) - 1;
        bucket_low(index) + ((1u64 << scale) - 1)
    }
}

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

/// The wire form of a [`Histogram`]: field order here is the byte contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncodedHistogram {
    /// Layout discriminator; always [`HISTOGRAM_LAYOUT_V1`].
    pub layout: String,
    /// Unit of every recorded value; always `ns`.
    pub unit: String,
    /// Linear sub-buckets per power of two, as a bit count; always 7.
    pub sub_bucket_bits: u32,
    /// Number of recorded values.
    pub total: u64,
    /// Exact smallest recorded value; absent when nothing was recorded.
    pub min: Option<u64>,
    /// Exact largest recorded value; absent when nothing was recorded.
    pub max: Option<u64>,
    /// Saturating sum of recorded values.
    pub sum: u64,
    /// Sparse `[index, count]` pairs, strictly ascending, no zero counts.
    pub counts: Vec<(u32, u64)>,
}

impl EncodedHistogram {
    /// Checks the invariants the layout promises.
    ///
    /// # Errors
    ///
    /// Returns an error naming the first violated invariant.
    pub fn validate(&self) -> SchemaResult<()> {
        if self.layout != HISTOGRAM_LAYOUT_V1 {
            return Err(SchemaError::invalid_field(
                "histogram.layout",
                &format!("{:?} is not {HISTOGRAM_LAYOUT_V1:?}", self.layout),
            ));
        }
        if self.unit != "ns" {
            return Err(SchemaError::invalid_field(
                "histogram.unit",
                &format!("{:?} is not \"ns\"", self.unit),
            ));
        }
        if self.sub_bucket_bits != SUB_BUCKET_BITS {
            return Err(SchemaError::invalid_field(
                "histogram.sub_bucket_bits",
                &format!("{} is not {SUB_BUCKET_BITS}", self.sub_bucket_bits),
            ));
        }
        let mut previous: Option<u32> = None;
        let mut bucket_total = 0u64;
        for (index, count) in &self.counts {
            if *count == 0 {
                return Err(SchemaError::invalid_field(
                    "histogram.counts",
                    &format!("bucket {index} has a zero count"),
                ));
            }
            // No `u64` lands above the top bucket, so an index that does was not
            // recorded from a measurement. Decoding it would invent bounds — and
            // far enough out, a shift wider than the type — from a document
            // nothing in this repository could have written.
            if *index > MAX_BUCKET_INDEX {
                return Err(SchemaError::invalid_field(
                    "histogram.counts",
                    &format!(
                        "bucket {index} is above {MAX_BUCKET_INDEX}, the highest index any \
                         u64 can fall in"
                    ),
                ));
            }
            if previous.is_some_and(|p| p >= *index) {
                return Err(SchemaError::invalid_field(
                    "histogram.counts",
                    &format!("bucket {index} is not strictly ascending"),
                ));
            }
            previous = Some(*index);
            bucket_total = bucket_total.saturating_add(*count);
        }
        if bucket_total != self.total {
            return Err(SchemaError::invalid_field(
                "histogram.total",
                &format!(
                    "bucket counts sum to {bucket_total} but total is {}",
                    self.total
                ),
            ));
        }
        // Each extreme is checked against emptiness on its own. Folding them
        // together with an `or` let a document claim `total: 0` while carrying a
        // minimum, which is a recording that both did and did not happen.
        let empty = self.total == 0;
        for (field, extreme) in [("histogram.min", self.min), ("histogram.max", self.max)] {
            if extreme.is_none() != empty {
                return Err(SchemaError::invalid_field(
                    field,
                    if empty {
                        "an empty histogram has no extreme to report"
                    } else {
                        "a histogram with recordings must report this extreme"
                    },
                ));
            }
        }
        if let (Some(min), Some(max)) = (self.min, self.max)
            && min > max
        {
            return Err(SchemaError::invalid_field(
                "histogram.min",
                &format!("minimum {min} is above maximum {max}"),
            ));
        }
        Ok(())
    }
}
