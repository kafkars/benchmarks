//! The scenario's `[section]` tables, one type each.
//!
//! They are grouped because they share their whole reason for existing: each
//! mirrors one table a person writes, denies unknown fields so a mistyped key
//! cannot pass for a setting, and carries no behavior of its own. The scenario
//! that assembles them is in [`super::scenario`].

use serde::{Deserialize, Serialize};

/// `[application]`: how many producers, how many callers, and what happens when
/// admission is refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceApplication {
    /// Producer handles the application creates.
    pub producer_instances: u32,
    /// Threads offering records per producer handle.
    pub callers_per_producer: u32,
    /// Backpressure vocabulary the adapter interprets.
    pub backpressure: String,
    /// Bytes the client may hold in its own queues.
    pub queue_bytes: u64,
    /// Records the application allows to be outstanding at once.
    pub max_outstanding_records: u64,
}

/// `[application_api]`: which client surfaces the application is required to
/// use.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceApplicationApi {
    /// Which admission call the application uses.
    pub admission_shape: String,
    /// Which completion signal the application waits on.
    pub completion_shape: String,
    /// Records per application-level batch offer.
    pub batch_records: u32,
}

/// `[payload]`: record size, generator, and the identity envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourcePayload {
    /// Payload size in bytes.
    pub bytes: u32,
    /// Generator vocabulary.
    pub profile: String,
    /// Seed for the payload generator; becomes the resolved experiment's seed
    /// unless the operator overrides it.
    pub seed: u64,
    /// Human description of the self-check envelope.
    #[serde(default)]
    pub identity: Option<String>,
}

/// `[producer]`: the client configuration under test.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceProducer {
    /// Acknowledgement level.
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
    /// Partition assignment vocabulary.
    pub partitioning: String,
    /// Produce requests the client may leave in flight per broker; the resolver
    /// supplies the default when a scenario leaves it out.
    #[serde(default)]
    pub max_in_flight_requests_per_broker: Option<u32>,
    /// How many times a record may be re-enqueued after a retriable failure.
    pub retry_max_replacements: u32,
    /// Milliseconds between retries.
    pub retry_backoff_ms: u64,
    /// Partition assignment for the warmup phase, when it differs.
    #[serde(default)]
    pub warmup_partitioning: Option<String>,
    /// Records the warmup sends serially to prime every partition leader.
    #[serde(default)]
    pub warmup_serialized_partition_primer_records: Option<u32>,
}

/// `[cluster]`: the cluster shape the scenario requires.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceCluster {
    /// Brokers the experiment expects to spread across.
    pub brokers: u32,
    /// Partitions per benchmark topic.
    pub partitions: u32,
    /// Replication factor for benchmark topics.
    pub replication_factor: u32,
    /// Minimum in-sync replicas for benchmark topics.
    pub min_in_sync_replicas: u32,
    /// Transport security vocabulary.
    pub security: String,
    /// Whether unclean leader election is permitted; defaults to false.
    #[serde(default)]
    pub unclean_leader_election: Option<bool>,
}

/// `[slo]`: the objectives a run is judged against.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSlo {
    /// Ceiling on the corrected 99th percentile, in milliseconds.
    #[serde(default)]
    pub corrected_p99_ms: Option<u64>,
    /// Ceiling on the 99th percentile of schedule delay, in milliseconds.
    #[serde(default)]
    pub schedule_delay_p99_ms: Option<u64>,
    /// Ceiling on the drain after the last offer, in milliseconds.
    #[serde(default)]
    pub drain_tail_ms: Option<u64>,
    /// Ceiling on queue growth across the window, in percent.
    #[serde(default)]
    pub queue_slope_percent: Option<u64>,
    /// Rate below which queue slope is not evidence of saturation.
    #[serde(default)]
    pub queue_slope_floor_records_per_second: Option<u64>,
    /// Fewest queue samples a slope judgement may rest on.
    #[serde(default)]
    pub minimum_queue_samples: Option<u64>,
    /// Native client retries permitted during measurement.
    #[serde(default)]
    pub native_retries: Option<u64>,
    /// Native client timeouts permitted during measurement.
    #[serde(default)]
    pub native_timeouts: Option<u64>,
}

/// `[search]`: bounds for a capacity search.
///
/// Read but not executed in this milestone; kept so that the reference-capacity
/// scenario parses rather than being quietly excluded from the vocabulary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSearch {
    /// First rate the search offers.
    pub initial_records_per_second: u64,
    /// Lowest rate the search will consider.
    pub minimum_records_per_second: u64,
    /// Highest rate the search will consider.
    pub maximum_records_per_second: u64,
    /// Multiplier applied while the search is still growing.
    pub growth_factor: u64,
    /// Width of the final bracket, in percent.
    pub resolution_percent: u64,
    /// Percentages of the found capacity that derived fixed-load runs use.
    pub derived_fixed_load_percentages: Vec<u64>,
}

/// `[validity]`: the checks a scenario declares mandatory.
///
/// The legacy harness treats every one of these as required, so they are read
/// for completeness rather than for configurability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "this mirrors a scenario section that is a list of independent required checks"
)]
pub struct SourceValidity {
    /// Every offered record must be drained before the run ends.
    pub require_complete_drain: bool,
    /// Every delivery must reach a terminal state.
    pub require_every_delivery_terminal: bool,
    /// Records must be identifiable when read back from the broker.
    pub require_broker_visible_identity: bool,
    /// No record may be missing on read-back.
    pub require_no_missing_records: bool,
    /// No record may be duplicated on read-back.
    pub require_no_duplicate_records: bool,
    /// Per-partition order must hold on read-back.
    pub require_partition_order: bool,
    /// Payload bytes must match exactly on read-back.
    pub require_exact_payload: bool,
}

/// `[native_request_concurrency]`: how the scenario matched request concurrency
/// across clients, recorded as prose because the setting has a different name
/// in each client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceNativeRequestConcurrency {
    /// Whether the settings were matched, and how.
    pub status: String,
    /// The kafkars setting and its value.
    pub kafkars: String,
    /// The librdkafka setting and its value.
    pub librdkafka_c: String,
}
