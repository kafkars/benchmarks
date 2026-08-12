//! The human-authored TOML inputs: a scenario, a cluster profile, and a subject
//! list.
//!
//! These three files are what a person writes and reviews; everything else in
//! this crate is machine-written. They resolve into a
//! [`ResolvedExperiment`](crate::ResolvedExperiment), which is where defaults
//! stop being implicit.
//!
//! Parsing is fail-closed: every type denies unknown fields. A scenario is a
//! declaration of intent, and a mistyped key in a declaration of intent is not
//! a harmless extra — it is a setting the author believes is in effect and that
//! nothing is reading. The cost is that adding a scenario key means adding it
//! here too, which is the intended amount of friction.
//!
//! The scenario types model the migrated legacy scenarios under
//! `scenarios/producer/` exactly, and tests parse those files from disk rather
//! than from a copy, so a drift between the two is a test failure. Note that
//! `scenarios/producer/producer-baseline.toml` is deliberately *not* one of
//! them: it is the design document's future parameter matrix, not an executable
//! scenario, and it has never been read by any harness.
//!
//! The scenario vocabulary for `load_mode` is not the resolved vocabulary. The
//! legacy scenarios say `closed-loop-capacity-point`, and the resolved document
//! says `closed-loop`; [`SourceLoadMode::resolved`] is the only place that
//! mapping is written down, and it is also where the capacity search mode is
//! refused, because this milestone has no implementation of it.

use serde::{Deserialize, Serialize};

use crate::error::{SchemaError, SchemaResult};
use crate::experiment::{LoadMode, SUBJECT_ROLES, is_subject_role};

/// How a scenario says load is offered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceLoadMode {
    /// One closed-loop capacity point: the client sets its own rate.
    #[serde(rename = "closed-loop-capacity-point")]
    ClosedLoopCapacityPoint,
    /// A scheduled open-loop run at one fixed offered rate.
    #[serde(rename = "scheduled-open-loop-fixed-rate")]
    ScheduledOpenLoopFixedRate,
    /// A search for the highest rate that still meets the objectives.
    #[serde(rename = "scheduled-open-loop-capacity-search")]
    ScheduledOpenLoopCapacitySearch,
}

impl SourceLoadMode {
    /// Returns the wire string a scenario writes.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ClosedLoopCapacityPoint => "closed-loop-capacity-point",
            Self::ScheduledOpenLoopFixedRate => "scheduled-open-loop-fixed-rate",
            Self::ScheduledOpenLoopCapacitySearch => "scheduled-open-loop-capacity-search",
        }
    }

    /// Maps a scenario load mode onto the resolved vocabulary.
    ///
    /// Capacity search is refused rather than approximated: running one fixed
    /// rate and calling it a capacity search would produce evidence that claims
    /// something nobody measured.
    pub fn resolved(self) -> SchemaResult<LoadMode> {
        match self {
            Self::ClosedLoopCapacityPoint => Ok(LoadMode::ClosedLoop),
            Self::ScheduledOpenLoopFixedRate => Ok(LoadMode::ScheduledOpenLoopFixedRate),
            Self::ScheduledOpenLoopCapacitySearch => Err(SchemaError::invalid_field(
                "load_mode",
                "capacity search is not implemented in this milestone",
            )),
        }
    }
}

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

/// A scenario file: the human statement of what to measure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceExperiment {
    /// Scenario name; carried into the resolved experiment.
    pub name: String,
    /// Rigor profile — `diagnostic`, `reference-capacity`, and so on.
    pub status: String,
    /// Whether the author believes the scenario could support a claim. Always
    /// false in this milestone.
    pub claim_eligible: bool,
    /// How load is offered.
    pub load_mode: SourceLoadMode,
    /// Records the measured phase produces; absent for a capacity search, which
    /// measures for a window instead.
    #[serde(default)]
    pub records: Option<u64>,
    /// Records the warmup phase produces.
    #[serde(default)]
    pub warmup_records: Option<u64>,
    /// Offered rate for a fixed-rate scenario.
    #[serde(default)]
    pub offered_records_per_second: Option<u64>,
    /// Measurement window for a capacity search, in seconds.
    #[serde(default)]
    pub window_seconds: Option<u64>,
    /// Warmup window for a capacity search, in seconds.
    #[serde(default)]
    pub warmup_seconds: Option<u64>,
    /// Repetitions per candidate rate in a capacity search.
    #[serde(default)]
    pub repetitions_per_rate: Option<u32>,
    /// How the application offers records.
    pub application: SourceApplication,
    /// Which client surfaces the application must use.
    pub application_api: SourceApplicationApi,
    /// What each record carries.
    pub payload: SourcePayload,
    /// Client configuration under test.
    pub producer: SourceProducer,
    /// Cluster shape the scenario requires.
    pub cluster: SourceCluster,
    /// Capacity search bounds, when the scenario searches.
    #[serde(default)]
    pub search: Option<SourceSearch>,
    /// Objectives the run is judged against.
    #[serde(default)]
    pub slo: Option<SourceSlo>,
    /// Validity checks the scenario declares mandatory.
    #[serde(default)]
    pub validity: Option<SourceValidity>,
    /// Prose record of how request concurrency was matched across clients.
    #[serde(default)]
    pub native_request_concurrency: Option<SourceNativeRequestConcurrency>,
}

impl SourceExperiment {
    /// Parses a scenario from TOML text.
    pub fn from_toml_str(text: &str) -> SchemaResult<Self> {
        parse_toml(text, "scenario")
    }
}

/// One subject the operator wants measured.
///
/// `role` is the operator's statement of what this subject is for, and it is
/// carried through resolution into
/// [`SubjectSpec::role`](crate::SubjectSpec::role), where it participates in the
/// experiment id. Leaving it out is legal and means unlabeled, so every subject
/// list written before roles existed still parses and still resolves to the
/// same identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectEntry {
    /// Name used in topic names, evidence paths, and comparisons.
    pub name: String,
    /// Argument vector that runs the adapter, program first.
    pub command: Vec<String>,
    /// One of [`SUBJECT_ROLES`](crate::SUBJECT_ROLES), or absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
}

/// The subject list: which adapters to run, and how to invoke them.
///
/// Subjects are a separate file from the scenario because they change for a
/// different reason. The scenario says what to measure and is reviewed; the
/// subject list says which binaries are on this machine today.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectsFile {
    /// Subjects in declaration order.
    #[serde(default)]
    pub subjects: Vec<SubjectEntry>,
}

impl SubjectsFile {
    /// Parses a subject list from TOML text.
    pub fn from_toml_str(text: &str) -> SchemaResult<Self> {
        parse_toml(text, "subjects")
    }

    /// Checks that every declared role is one the vocabulary knows.
    ///
    /// A role is a claim about what the run is comparing, and a misspelled one
    /// would quietly demote a subject to unlabeled — the same failure mode
    /// `deny_unknown_fields` exists to prevent, one level down.
    ///
    /// # Errors
    ///
    /// Returns an error naming the first entry whose role is not one of
    /// [`SUBJECT_ROLES`](crate::SUBJECT_ROLES).
    pub fn validate(&self) -> SchemaResult<()> {
        for (index, entry) in self.subjects.iter().enumerate() {
            if let Some(role) = &entry.role
                && !is_subject_role(role)
            {
                return Err(SchemaError::invalid_field(
                    &format!("subjects[{index}].role"),
                    &format!("{role:?} is not one of {SUBJECT_ROLES:?}"),
                ));
            }
        }
        Ok(())
    }
}

/// Argument vectors for the tools the control plane calls out to.
///
/// Topic creation and read-back verification are deliberately *not* part of the
/// adapter protocol: an adapter must never be the thing that decides whether
/// its own output was correct. The tools are named by configuration so that no
/// adapter name is hard-coded into the control plane, and each vector is a
/// prefix — the control plane appends the positional arguments the legacy tools
/// already expect.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterTools {
    /// Prefix for topic creation, called as
    /// `<prefix...> <bootstrap> <partitions> <replication-factor> <topic...>`.
    #[serde(default)]
    pub topic_create: Vec<String>,
    /// Prefix for topic deletion, called as `<prefix...> <bootstrap> <topic...>`.
    #[serde(default)]
    pub topic_delete: Vec<String>,
    /// Prefix for read-back verification, called as
    /// `<prefix...> <bootstrap> <topic> <run-id> <records> <payload-bytes> <partitions>`.
    #[serde(default)]
    pub verify: Vec<String>,
}

impl ClusterTools {
    /// Reports whether no tool at all is configured.
    ///
    /// A profile with no tools can still run an experiment; the control plane
    /// records the skipped phases as deferred checks rather than pretending
    /// they passed.
    pub fn is_empty(&self) -> bool {
        self.topic_create.is_empty() && self.topic_delete.is_empty() && self.verify.is_empty()
    }
}

/// A cluster profile: the facts about a cluster that a scenario must not
/// contain.
///
/// Keeping bootstrap servers out of the scenario is what lets the same
/// experiment id describe a run on a laptop and a run in CI.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterProfile {
    /// Profile name, used in logs and evidence.
    pub name: String,
    /// Bootstrap servers, `host:port[,host:port...]`.
    pub bootstrap: String,
    /// Broker version, when the operator knows it.
    #[serde(default)]
    pub broker_version: Option<String>,
    /// Transport security vocabulary, when it differs from the scenario's.
    #[serde(default)]
    pub security: Option<String>,
    /// Brokers actually in this cluster.
    #[serde(default)]
    pub brokers: Option<u32>,
    /// Who owns the cluster's lifecycle, recorded in the environment document.
    #[serde(default)]
    pub lifecycle: Option<String>,
    /// Tools the control plane calls out to.
    #[serde(default)]
    pub tools: ClusterTools,
}

impl ClusterProfile {
    /// Parses a cluster profile from TOML text.
    pub fn from_toml_str(text: &str) -> SchemaResult<Self> {
        parse_toml(text, "cluster profile")
    }
}

fn parse_toml<T: serde::de::DeserializeOwned>(text: &str, document: &str) -> SchemaResult<T> {
    toml::from_str(text).map_err(|error| SchemaError::parse(format!("{document}: {error}")))
}
