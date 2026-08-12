//! The sub-documents of a resolved experiment: the application, the payload,
//! the producer, the budget, the cluster, and the objectives.
//!
//! Each is a section of the same hashed document, so every number here is an
//! integer and every field order is the byte contract. They are grouped in one
//! place because they share that property and nothing else: none of them has
//! behavior beyond the defaults the control plane also defaults to.

use serde::{Deserialize, Serialize};

/// How the application offers records: instance count, concurrency, and the
/// shape of the admission and completion surfaces it uses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationSpec {
    /// Number of producer handles the application creates.
    pub producer_instances: u32,
    /// Threads offering records per producer handle.
    pub callers_per_producer: u32,
    /// What the application does when admission is refused, as a vocabulary the
    /// adapter interprets — for example `block-within-original-offer`.
    pub backpressure: String,
    /// Bytes the client may hold in its own queues.
    pub queue_bytes: u64,
    /// Records the application allows to be outstanding at once.
    pub max_outstanding_records: u64,
    /// Which admission call the application uses — for example `public-batch`.
    pub admission_shape: String,
    /// Which completion signal the application waits on — for example
    /// `aggregate-batch-terminal`.
    pub completion_shape: String,
    /// Records per application-level batch offer.
    pub batch_records: u32,
}

/// What each record carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PayloadSpec {
    /// Payload size in bytes, identical for every record.
    pub bytes: u32,
    /// Generator vocabulary — for example `deterministic-ascii-envelope`.
    pub profile: String,
    /// Human description of the self-check envelope, when the profile has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
}

/// Client configuration under test.
///
/// These are the knobs whose values a comparison has to hold equal across
/// subjects; an adapter that cannot honour one of them declines the experiment
/// instead of running a different one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProducerSpec {
    /// Acknowledgement level, as the Kafka vocabulary spells it.
    pub acks: String,
    /// Whether the idempotent producer is enabled.
    pub idempotence: bool,
    /// Compression codec, or `none`.
    pub compression: String,
    /// Milliseconds the client may wait to fill a batch.
    pub linger_ms: u64,
    /// Records per broker-bound batch.
    pub batch_records: u32,
    /// Bytes per broker-bound batch.
    pub batch_bytes: u64,
    /// Bytes per produce request.
    pub request_bytes: u64,
    /// Milliseconds before a record's delivery is abandoned.
    pub delivery_timeout_ms: u64,
    /// Partition assignment vocabulary — for example `explicit-round-robin`.
    pub partitioning: String,
    /// Produce requests the client may leave in flight per broker.
    pub max_in_flight_requests_per_broker: u32,
    /// How many times a record may be re-enqueued after a retriable failure.
    pub retry_max_replacements: u32,
    /// Milliseconds between retries.
    pub retry_backoff_ms: u64,
    /// Partition assignment for the warmup phase, when it differs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warmup_partitioning: Option<String>,
    /// Records the warmup sends serially to prime every partition leader.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warmup_serialized_partition_primer_records: Option<u32>,
}

/// The ceilings an attempt declares for itself.
///
/// A budget is part of the intent rather than an operational detail: a run that
/// needed twice the wall clock to finish did not measure the same thing, so the
/// ceilings are hashed into the experiment id along with everything else.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetSpec {
    /// Seconds one subject may run before the control plane kills it.
    pub run_timeout_seconds: u64,
    /// Seconds a configured tool — topic creation, verification — may run.
    pub tool_timeout_seconds: u64,
    /// Seconds an adapter probe (`describe`, `validate`) may run.
    pub probe_timeout_seconds: u64,
    /// Bytes of a probe's output the control plane will read before giving up.
    pub max_captured_output_bytes: u64,
}

impl Default for BudgetSpec {
    /// Returns the default budget the control plane's flags also default to.
    fn default() -> Self {
        Self {
            run_timeout_seconds: 600,
            tool_timeout_seconds: 120,
            probe_timeout_seconds: 10,
            max_captured_output_bytes: 1_048_576,
        }
    }
}

/// The cluster shape the experiment requires.
///
/// This is intent, not observation: it says what the topics must look like, and
/// the environment document records what was actually there.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterSpec {
    /// Brokers the experiment expects to spread across.
    pub brokers: u32,
    /// Partitions per benchmark topic.
    pub partitions: u32,
    /// Replication factor for benchmark topics.
    pub replication_factor: u32,
    /// Minimum in-sync replicas for benchmark topics.
    pub min_in_sync_replicas: u32,
    /// Transport security vocabulary — for example `plaintext`.
    pub security: String,
    /// Whether unclean leader election is permitted during the run.
    pub unclean_leader_election: bool,
}

/// Service-level objectives a run is judged against.
///
/// Every field is optional because a diagnostic experiment declares none of
/// them; an empty specification serializes as an empty object rather than
/// disappearing, so a reader can tell "no objectives" from "objectives lost".
/// Durations are whole milliseconds and slopes are whole percent, because this
/// document is hashed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SloSpec {
    /// Ceiling on the coordinated-omission-corrected 99th percentile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub corrected_p99_ms: Option<u64>,
    /// Ceiling on the 99th percentile of schedule delay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_delay_p99_ms: Option<u64>,
    /// Ceiling on how long the drain after the last offer may take.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drain_tail_ms: Option<u64>,
    /// Ceiling on queue growth across the measurement window, in percent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_slope_percent: Option<u64>,
    /// Rate below which queue slope is not evidence of saturation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_slope_floor_records_per_second: Option<u64>,
    /// Fewest queue samples a slope judgement may rest on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum_queue_samples: Option<u64>,
    /// Native client retries permitted during the measurement window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_retries: Option<u64>,
    /// Native client timeouts permitted during the measurement window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_timeouts: Option<u64>,
}

impl SloSpec {
    /// Reports whether the experiment declared no objectives at all.
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}
