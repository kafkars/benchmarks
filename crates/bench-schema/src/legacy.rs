//! Lenient read-only views of the documents the current adapters and tools
//! already produce.
//!
//! Everything else in this crate is strict, because everything else in this
//! crate is written by this repository. These types are the opposite: they read
//! documents written by an adapter that may be older, newer, or somebody
//! else's. A reader that refuses a sealed result because it grew a key is a
//! reader that retroactively invalidates evidence, so these types name only the
//! fields the control plane actually needs and let every other key pass by
//! untouched.
//!
//! The types deliberately derive `Deserialize` and not `Serialize`. An adapter
//! result is captured as bytes and sealed as those exact bytes; re-serializing
//! it through a partial view would drop the fields the view does not name and
//! would round-trip its floating-point numbers through a second formatter. The
//! views extract facts for gating decisions. They never rewrite evidence.
//!
//! The two shapes read here are `kafkars.producer-benchmark.v1` (closed loop,
//! flat latency percentiles) and `kafkars.producer-fixed-load.v1` (scheduled
//! fixed rate, latency split into uncorrected, corrected, and schedule-delay
//! surfaces). One view covers both, because the control plane's question — how
//! many records were acknowledged, how fast, at what tail — is the same
//! question either way.

use serde::Deserialize;

use crate::canon;
use crate::error::SchemaResult;
use crate::schema_id::{PRODUCER_BENCHMARK_V1, PRODUCER_FIXED_LOAD_V1, PRODUCER_VERIFICATION_V1};

/// One set of latency percentiles, in nanoseconds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct LegacyPercentiles {
    /// Median.
    pub p50: Option<u64>,
    /// 95th percentile.
    pub p95: Option<u64>,
    /// 99th percentile.
    pub p99: Option<u64>,
    /// 99.9th percentile.
    pub p999: Option<u64>,
    /// Largest observation.
    pub max: Option<u64>,
}

/// The `latency_ns` object of either producer result shape.
///
/// A closed-loop result fills the flat percentiles; a fixed-load result fills
/// the three named surfaces instead. Both are represented rather than modelled
/// as an enum, because a document that somehow carries both is evidence about a
/// broken adapter and should still parse.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct LegacyLatency {
    /// Closed-loop median.
    pub p50: Option<u64>,
    /// Closed-loop 95th percentile.
    pub p95: Option<u64>,
    /// Closed-loop 99th percentile.
    pub p99: Option<u64>,
    /// Closed-loop 99.9th percentile.
    pub p999: Option<u64>,
    /// Closed-loop maximum.
    pub max: Option<u64>,
    /// Fixed-load latency measured from admission, which hides coordinated
    /// omission.
    pub uncorrected: Option<LegacyPercentiles>,
    /// Fixed-load latency measured from the record's intended arrival time.
    pub corrected: Option<LegacyPercentiles>,
    /// Fixed-load gap between intended and actual admission.
    pub schedule_delay: Option<LegacyPercentiles>,
}

impl LegacyLatency {
    /// Returns the 99th percentile a comparison should use.
    ///
    /// Prefers the flat closed-loop value, then the corrected fixed-load
    /// surface, then the uncorrected one. Corrected outranks uncorrected
    /// because an open-loop run whose client fell behind its schedule has an
    /// uncorrected tail that flatters it.
    pub fn headline_p99_ns(&self) -> Option<u64> {
        self.p99
            .or_else(|| self.corrected.and_then(|surface| surface.p99))
            .or_else(|| self.uncorrected.and_then(|surface| surface.p99))
    }
}

/// The fields the control plane reads out of a producer result document.
///
/// Every field is optional: this view is applied to bytes it did not write, and
/// a missing field is a fact about the document, not a parse failure.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct KnownProducerResult {
    /// Declared schema id.
    pub schema: Option<String>,
    /// Adapter name the document claims.
    pub adapter: Option<String>,
    /// Adapter version the document claims.
    pub adapter_version: Option<String>,
    /// Run id the records were stamped with.
    pub run_id: Option<String>,
    /// Topic the measured phase produced to.
    pub topic: Option<String>,
    /// The adapter's own validity flag.
    pub valid: Option<bool>,
    /// Records offered to the client.
    pub offered_records: Option<u64>,
    /// Records the cluster acknowledged.
    pub acknowledged_records: Option<u64>,
    /// Records that failed permanently.
    pub failed_records: Option<u64>,
    /// Acknowledged records per second over the measured phase.
    pub acknowledged_records_per_second: Option<f64>,
    /// Latency percentiles, in whichever shape the adapter wrote.
    pub latency_ns: Option<LegacyLatency>,
}

impl KnownProducerResult {
    /// Reads the known fields out of a result document's bytes.
    pub fn from_slice(bytes: &[u8]) -> SchemaResult<Self> {
        canon::parse_json_slice(bytes)
    }

    /// Reports whether the document declares one of the two producer result
    /// schemas this view understands.
    pub fn has_known_schema(&self) -> bool {
        matches!(
            self.schema.as_deref(),
            Some(PRODUCER_BENCHMARK_V1 | PRODUCER_FIXED_LOAD_V1)
        )
    }

    /// Reports whether the adapter declared its own result valid.
    ///
    /// This is necessary and nowhere near sufficient: an adapter cannot know
    /// whether the records reached the broker intact, which is what read-back
    /// verification is for.
    pub fn adapter_declared_valid(&self) -> bool {
        self.valid == Some(true)
    }

    /// Returns the 99th percentile latency a comparison should use.
    pub fn headline_p99_ns(&self) -> Option<u64> {
        self.latency_ns
            .as_ref()
            .and_then(LegacyLatency::headline_p99_ns)
    }
}

/// The fields the control plane reads out of a read-back verification document.
///
/// The strict, writable mirror of this document lives in the verifier crate;
/// this view exists so the control plane can gate on a verification written by
/// a tool it does not own.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct VerifierReport {
    /// Declared schema id.
    pub schema: Option<String>,
    /// Topic that was read back.
    pub topic: Option<String>,
    /// Records the verifier was told to expect.
    pub expected_records: Option<u64>,
    /// Distinct in-order records accepted.
    pub verified_records: Option<u64>,
    /// Records observed more than once.
    pub duplicates: Option<u64>,
    /// Expected records never observed.
    pub missing_records: Option<u64>,
    /// Records whose payload failed its self-check.
    pub corrupt: Option<u64>,
    /// Records on the wrong partition or out of sequence.
    pub unexpected: Option<u64>,
    /// Partitions driven to end-of-partition.
    pub eof_partitions: Option<u64>,
    /// The verifier's own verdict.
    pub valid: Option<bool>,
}

impl VerifierReport {
    /// Reads the known fields out of a verification document's bytes.
    pub fn from_slice(bytes: &[u8]) -> SchemaResult<Self> {
        canon::parse_json_slice(bytes)
    }

    /// Reports whether the document declares the legacy verification schema.
    pub fn has_known_schema(&self) -> bool {
        self.schema.as_deref() == Some(PRODUCER_VERIFICATION_V1)
    }

    /// Reports whether the topic is intact for the run the control plane asked
    /// about.
    ///
    /// Mirrors `validateVerification` in the legacy control plane: the
    /// verifier's own flag is necessary but not sufficient, because the
    /// verifier cannot know which topic or record count was intended.
    pub fn satisfies_contract(&self, topic: &str, expected_records: u64, partitions: u64) -> bool {
        self.has_known_schema()
            && self.topic.as_deref() == Some(topic)
            && self.expected_records == Some(expected_records)
            && self.verified_records == Some(expected_records)
            && self.duplicates == Some(0)
            && self.missing_records == Some(0)
            && self.corrupt == Some(0)
            && self.unexpected == Some(0)
            && self.eof_partitions == Some(partitions)
            && self.valid == Some(true)
    }
}
