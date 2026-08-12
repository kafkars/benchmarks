//! The wire side: the encoded form whose field order is the byte contract, and
//! the invariants a document handed to this crate has to satisfy.

use serde::{Deserialize, Serialize};

use crate::error::{SchemaError, SchemaResult};

use super::layout::{HISTOGRAM_LAYOUT_V1, MAX_BUCKET_INDEX, SUB_BUCKET_BITS};

/// The wire form of a [`Histogram`](crate::Histogram): field order here is the
/// byte contract.
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
