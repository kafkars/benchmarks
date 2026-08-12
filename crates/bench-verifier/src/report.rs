//! The `kafkars.producer-verification.v1` document: one topic read back to the
//! end of every partition, counted, and judged.
//!
//! This is a mirror, not a definition. The producing implementation today is
//! `adapters/librdkafka-c/verifier.c`, which prints exactly this object on
//! stdout and exits non-zero when `valid` is false. The field set and the field
//! order below are taken from that `printf` so that a serialized
//! [`VerificationReport`] reproduces the bytes the C program emits.
//!
//! Deserialization is deliberately lenient about unknown keys. Sealed bundles
//! are immutable, and this crate has to keep reading bundles that were sealed
//! by older and newer writers alike; a reader that refuses a document because it
//! grew a key is a reader that retroactively invalidates evidence. Changes that
//! alter the *meaning* of an existing key are not additions and require a new
//! schema id.

use serde::{Deserialize, Serialize};

/// Schema id carried by every producer-verification document this crate reads.
pub const PRODUCER_VERIFICATION_SCHEMA_V1: &str = "kafkars.producer-verification.v1";

/// Independent read-back evidence for a single benchmark topic.
///
/// Every count is a whole number of records or partitions, so the document
/// carries no floating-point values and can participate in identity hashing
/// without canonicalization hazards.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationReport {
    /// Schema id of the document, expected to be
    /// [`PRODUCER_VERIFICATION_SCHEMA_V1`].
    pub schema: String,
    /// Topic that was read back.
    pub topic: String,
    /// Records the control plane told the verifier to expect.
    pub expected_records: u64,
    /// Distinct in-order records the verifier accepted.
    pub verified_records: u64,
    /// Records observed more than once.
    pub duplicates: u64,
    /// Expected records never observed.
    pub missing_records: u64,
    /// Records whose payload failed its self-check.
    pub corrupt: u64,
    /// Records on the wrong partition or out of per-partition sequence order.
    pub unexpected: u64,
    /// Partitions the verifier drove all the way to end-of-partition.
    pub eof_partitions: u64,
    /// The verifier's own verdict for this topic.
    pub valid: bool,
}

impl VerificationReport {
    /// Reports whether the document declares the schema this crate understands.
    ///
    /// A document that fails this check may still deserialize — the field is a
    /// plain string — so callers must consult it before trusting any count.
    pub fn has_expected_schema(&self) -> bool {
        self.schema == PRODUCER_VERIFICATION_SCHEMA_V1
    }

    /// Reports whether the topic is intact for `expected_records` records
    /// spread over `partitions` partitions.
    ///
    /// This mirrors `validateVerification` in the legacy control plane: the
    /// verifier's own `valid` flag is necessary but not sufficient, because it
    /// cannot know which topic or record count the control plane intended.
    pub fn satisfies_contract(&self, topic: &str, expected_records: u64, partitions: u64) -> bool {
        self.has_expected_schema()
            && self.topic == topic
            && self.expected_records == expected_records
            && self.verified_records == expected_records
            && self.duplicates == 0
            && self.missing_records == 0
            && self.corrupt == 0
            && self.unexpected == 0
            && self.eof_partitions == partitions
            && self.valid
    }
}
