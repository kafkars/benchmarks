//! The bucket arithmetic of `kafkars.log-linear.v1`.
//!
//! This is the half of the layout the C implementation restates: three
//! constants and three total functions, with no state and no allocation. Every
//! other file in this module tree is bookkeeping on top of them.

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
/// pays for a malformed document:
/// [`EncodedHistogram::validate`](crate::EncodedHistogram::validate) rejects
/// such an index with a message, and this function refuses to panic for anyone
/// who reached it without validating first.
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
