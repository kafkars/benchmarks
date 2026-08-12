//! `kafkars.producer-benchmark.v2`: the measurement document the adapter
//! protocol's `run` verb writes once the four-timestamp offer model exists.
//!
//! # What v2 fixes over v1
//!
//! Every offer owns one immutable identity and four timestamps — `intended`,
//! `call_start`, `accepted`, `terminal` — and none of them is ever reset
//! because a queue was full, so backpressure time is part of every latency
//! this document reports. Distributions are carried as bounded log-linear
//! [`EncodedHistogram`]s instead of run-sized arrays, so evidence memory no
//! longer scales with run length. What the adapter *did* in the measured path
//! — payload construction, ownership, completion, serialization — is declared
//! up front, so a reader can refuse to compare unlike work.
//!
//! The legacy stdout verbs keep emitting `kafkars.producer-benchmark.v1`
//! forever; v2 exists only on the protocol path. Readers derive percentiles
//! from the histograms; the writer never computes them.
//!
//! # Accounting invariants ([`ProducerBenchmarkV2::validate`])
//!
//! `offered >= accepted`;
//! `accepted == acknowledged + failed + timed_out + unknown`;
//! `call_start_to_accepted.total == accepted`;
//! `accepted_to_terminal.total == intended_to_terminal.total ==
//! acknowledged + failed + timed_out` (unknown offers have no terminal);
//! `intended_to_call_start` present exactly when the load mode is scheduled
//! open-loop, with `total == offered`.
//!
//! # Layout
//!
//! - this file — the document's shape, field by field.
//! - `validate` — the accounting invariants, and the order they are named in.

mod validate;

use serde::{Deserialize, Serialize};

use crate::error::{SchemaError, SchemaResult};
use crate::experiment::LoadMode;
use crate::histogram::EncodedHistogram;
use crate::schema_id::PRODUCER_BENCHMARK_V2;

/// What the adapter actually did in the measured path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclaredExecution {
    /// How payload bytes came to exist, e.g. `prebuilt-pool`.
    pub payload_construction: String,
    /// Who owned the bytes across the client boundary, e.g. `copy-in`.
    pub ownership: String,
    /// How completion was observed, e.g. `public-future`, `delivery-callback`.
    pub completion_mode: String,
    /// Whether serialization work was inside the measured interval; the
    /// harness contract is `excluded`.
    pub serialization: String,
}

/// Where every offer ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OfferOutcomes {
    /// Offers whose public-API attempt began.
    pub offered: u64,
    /// Offers the client took ownership of.
    pub accepted: u64,
    /// Accepted offers acknowledged by the broker.
    pub acknowledged: u64,
    /// Accepted offers that reached a failure terminal.
    pub failed: u64,
    /// Accepted offers that reached a timeout terminal.
    pub timed_out: u64,
    /// Accepted offers with no terminal by the drain deadline.
    pub unknown: u64,
}

/// The bounded latency distributions, all in nanoseconds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OfferTiming {
    /// The clock every duration was measured on; always `monotonic-ns`.
    pub clock: String,
    /// `terminal - intended` for every offer that reached a terminal.
    pub intended_to_terminal: EncodedHistogram,
    /// `terminal - accepted` for every offer that reached a terminal.
    pub accepted_to_terminal: EncodedHistogram,
    /// `accepted - call_start`: admission wait, including every retry of the
    /// same immutable offer.
    pub call_start_to_accepted: EncodedHistogram,
    /// `call_start - intended`: scheduler lateness; present exactly when a
    /// schedule existed (scheduled open-loop load).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intended_to_call_start: Option<EncodedHistogram>,
}

/// Goodput over the measured interval.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeasuredThroughput {
    /// Length of the measured interval.
    pub measured_duration_ns: u64,
    /// Acknowledged records per second over that interval.
    pub acknowledged_records_per_second: f64,
    /// Acknowledged payload bytes per second over that interval.
    pub acknowledged_payload_bytes_per_second: f64,
}

/// What the application-side queue did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueObservation {
    /// Largest outstanding-offer count observed.
    pub max_outstanding_observed: u64,
    /// Largest outstanding *payload byte* count observed, when the adapter
    /// tracked one.
    ///
    /// The count above says how many offers the client owned at the peak; this
    /// says how much memory that peak was. Two clients at the same record
    /// high-water can retain very different amounts of the application's bytes,
    /// and a record count alone cannot tell a reader which. Absent rather than
    /// zero for an adapter that does not track it, because "not measured" and
    /// "nothing retained" are opposite claims.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_outstanding_bytes_observed: Option<u64>,
    /// Outstanding offers when the run ended; complete drain is zero.
    pub final_outstanding: u64,
}

/// Self-reported process resources, when the platform exposes them safely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessResources {
    /// Peak resident set size in bytes.
    pub max_rss_bytes: u64,
    /// User CPU time in nanoseconds.
    pub user_cpu_ns: u64,
    /// System CPU time in nanoseconds.
    pub system_cpu_ns: u64,
}

/// The v2 producer measurement document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProducerBenchmarkV2 {
    /// Always [`PRODUCER_BENCHMARK_V2`].
    pub schema: String,
    /// Adapter name as declared by `describe`.
    pub adapter: String,
    /// Adapter version as declared by `describe`.
    pub adapter_version: String,
    /// The attempt's sixteen-hex run id.
    pub run_id: String,
    /// The load mode this measurement ran under.
    pub load_mode: LoadMode,
    /// What the adapter did in the measured path.
    pub declared: DeclaredExecution,
    /// Where every offer ended.
    pub outcomes: OfferOutcomes,
    /// The bounded latency distributions.
    pub timing: OfferTiming,
    /// Goodput over the measured interval.
    pub throughput: MeasuredThroughput,
    /// Application-side queue observations.
    pub queue: QueueObservation,
    /// Self-reported process resources, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ProcessResources>,
    /// Bundle-relative path of native client metrics, when the client emits
    /// them (librdkafka statistics JSONL).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_metrics_path: Option<String>,
    /// The adapter's own validity verdict for this measurement.
    pub valid: bool,
    /// Why the adapter declared the measurement invalid, when it did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invalid_reason: Option<String>,
}

impl ProducerBenchmarkV2 {
    /// The schema id of this document.
    pub const SCHEMA: &'static str = PRODUCER_BENCHMARK_V2;

    /// Parses and validates a v2 document from its bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the bytes are not JSON, the schema id is wrong,
    /// or an accounting invariant fails.
    pub fn from_slice(bytes: &[u8]) -> SchemaResult<Self> {
        let document: Self = serde_json::from_slice(bytes)
            .map_err(|error| SchemaError::parse(format!("producer-benchmark.v2: {error}")))?;
        document.validate()?;
        Ok(document)
    }

    /// Checks the accounting invariants documented on this type.
    ///
    /// # Errors
    ///
    /// Returns an error naming the first violated invariant.
    pub fn validate(&self) -> SchemaResult<()> {
        validate::validate(self)
    }
}
