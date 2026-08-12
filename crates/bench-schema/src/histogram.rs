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
//!
//! # Layout
//!
//! - `layout` — the constants and the three total bucket functions.
//! - `recording` — the mergeable histogram a run accumulates into.
//! - `encoded` — the wire form and its invariants.

mod encoded;
mod layout;
mod recording;

pub use encoded::EncodedHistogram;
pub use layout::{
    HISTOGRAM_LAYOUT_V1, MAX_BUCKET_INDEX, SUB_BUCKET_BITS, SUB_BUCKET_COUNT, bucket_high,
    bucket_index, bucket_low,
};
pub use recording::Histogram;
