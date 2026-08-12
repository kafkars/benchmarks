//! `kafkars.experiment.v1`: the resolved statement of what a benchmark attempt
//! intends to measure.
//!
//! This document is the input to everything and the source of the experiment
//! id. It is produced by resolving a human-authored scenario against a cluster
//! profile, a subject list, and a seed, so by the time it exists every default
//! has been made explicit. Nothing downstream may re-derive a value that is
//! stated here.
//!
//! Two rules keep the document hashable.
//!
//! - Every number that participates in the identity is an integer. Rates,
//!   byte counts, and record counts are exact; ratios and latencies are
//!   measurement, and measurement belongs in the result documents.
//! - The runtime binding — bootstrap servers, run id, topic names, execution
//!   order — is carried in an optional [`RuntimeBinding`] that the identity
//!   ignores, so the same intent keeps the same id when it is run against a
//!   different cluster tomorrow.
//!
//! [`ResolvedExperiment::validate`] holds the cross-field rules that no type
//! can express: a fixed-rate experiment needs an offered rate and an arrival
//! model, a producer experiment needs a producer section, subject names have to
//! survive being turned into topic names, and `claim_eligible` is false for
//! every experiment this milestone can produce.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::{SchemaError, SchemaResult};
use crate::schema_id::{self, EXPERIMENT_V1};

/// Longest topic name Kafka accepts.
pub const MAX_TOPIC_NAME_LENGTH: usize = 249;

/// Longest subject name this repository accepts.
///
/// Subject names become part of topic names, so they are bounded well below the
/// Kafka limit to leave room for the prefix, the run id, and the `-warmup`
/// suffix.
pub const MAX_SUBJECT_NAME_LENGTH: usize = 48;

/// Reports whether `name` uses only characters Kafka accepts in a topic name.
///
/// The legal set is ASCII alphanumerics plus `.`, `_`, and `-`. The two names
/// `.` and `..` are excluded because they are legal path components and a topic
/// name ends up in file paths inside an evidence bundle.
pub fn is_topic_charset_safe(name: &str) -> bool {
    if name.is_empty() || name == "." || name == ".." {
        return false;
    }
    name.chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-'))
}

/// What kind of client behavior an experiment measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExperimentKind {
    /// Records produced to a topic and acknowledged by the cluster.
    Producer,
}

/// How load is offered to the client under test.
///
/// The wire strings are pinned by tests. `closed-loop` means the application
/// offers the next record as soon as the previous one is admitted, so the
/// client's own capacity sets the rate; `scheduled-open-loop-fixed-rate` means
/// records have intended arrival times computed from a rate, so a slow client
/// falls behind its schedule instead of slowing the offer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LoadMode {
    /// Offer rate is whatever the client can absorb.
    ClosedLoop,
    /// Offer times come from a fixed-rate schedule the client cannot slow.
    ScheduledOpenLoopFixedRate,
}

impl LoadMode {
    /// Returns the wire string for this mode.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ClosedLoop => "closed-loop",
            Self::ScheduledOpenLoopFixedRate => "scheduled-open-loop-fixed-rate",
        }
    }
}

/// The arrival process a scheduled load mode draws its intended offsets from.
///
/// Only [`ArrivalModel::Deterministic`] has an implementation in this
/// milestone; an adapter that is handed anything else declines it in its
/// validate report rather than silently substituting a schedule it does
/// implement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ArrivalModel {
    /// Evenly spaced arrivals, one every `1 / rate` seconds.
    Deterministic,
    /// Exponentially distributed inter-arrival times with the given mean rate.
    Poisson,
}

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

/// One thing being measured: a name, the adapter behind it, and how to run it.
///
/// `command` is excluded from the experiment id. The same subject built into a
/// different directory, or invoked through an absolute rather than a relative
/// path, is the same subject; what it actually was at run time is recorded in
/// the subjects lock instead.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectSpec {
    /// Name used in topic names, evidence paths, and comparisons.
    pub name: String,
    /// Adapter identity as reported by `describe`.
    pub adapter_name: String,
    /// Adapter version as reported by `describe`.
    pub adapter_version: String,
    /// Argument vector that runs the adapter, program first.
    pub command: Vec<String>,
}

/// The measured and warmup topics one subject owns for one attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicPair {
    /// Topic the measured phase produces to.
    pub measured: String,
    /// Topic the warmup phase produces to.
    pub warmup: String,
}

/// Everything about an attempt that the experiment id deliberately ignores.
///
/// Present once the control plane has bound the experiment to a cluster and an
/// attempt; absent in the output of a bare `resolve`, which describes intent
/// that has not been run yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeBinding {
    /// Bootstrap servers the subjects connect to.
    pub bootstrap: String,
    /// Sixteen lowercase hex characters identifying this attempt's records.
    pub run_id: String,
    /// Prefix every topic name for this attempt starts with.
    pub topic_prefix: String,
    /// Topics per subject name.
    pub topics: BTreeMap<String, TopicPair>,
    /// Subject names in the order they were executed.
    pub execution_order: Vec<String>,
}

/// `kafkars.experiment.v1`: resolved intent, ready to run and ready to hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedExperiment {
    /// Schema id, always [`ResolvedExperiment::SCHEMA`].
    pub schema: String,
    /// Scenario name this experiment was resolved from.
    pub name: String,
    /// What kind of client behavior is measured.
    pub kind: ExperimentKind,
    /// Rigor profile the scenario declares — for example `diagnostic`.
    pub profile: String,
    /// Whether the run may support a published claim. False everywhere in this
    /// milestone; [`ResolvedExperiment::validate`] rejects true.
    pub claim_eligible: bool,
    /// How load is offered.
    pub load_mode: LoadMode,
    /// Records the measured phase produces.
    pub records: u64,
    /// Records the warmup phase produces before measurement starts.
    pub warmup_records: u64,
    /// Offered rate; required by, and only by, a fixed-rate load mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offered_records_per_second: Option<u64>,
    /// Arrival process; required by, and only by, a fixed-rate load mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arrival: Option<ArrivalModel>,
    /// Seed for every deterministic choice the run makes.
    pub seed: u64,
    /// How the application offers records.
    pub application: ApplicationSpec,
    /// What each record carries.
    pub payload: PayloadSpec,
    /// Producer configuration; required by, and only by, a producer experiment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer: Option<ProducerSpec>,
    /// Ceilings the attempt declares for itself.
    pub budget: BudgetSpec,
    /// Cluster shape the experiment requires.
    pub cluster: ClusterSpec,
    /// Objectives the run is judged against; may be empty.
    pub slo: SloSpec,
    /// Subjects to measure, in declaration order.
    pub subjects: Vec<SubjectSpec>,
    /// Attempt-specific binding, excluded from the experiment id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<RuntimeBinding>,
}

impl ResolvedExperiment {
    /// Schema id this document declares.
    pub const SCHEMA: &'static str = EXPERIMENT_V1;

    /// Reports whether the document declares the expected schema id.
    pub fn has_expected_schema(&self) -> bool {
        self.schema == Self::SCHEMA
    }

    /// Returns the subject with the given name.
    pub fn subject(&self, name: &str) -> Option<&SubjectSpec> {
        self.subjects.iter().find(|subject| subject.name == name)
    }

    /// Checks every cross-field rule the type system cannot express.
    ///
    /// The rules are deliberately strict and stated in one place: an experiment
    /// that reaches an adapter has already been judged coherent, so an adapter
    /// declining it means the adapter cannot do it, not that the request was
    /// malformed.
    pub fn validate(&self) -> SchemaResult<()> {
        schema_id::require_schema(&self.schema, Self::SCHEMA)?;
        if self.name.trim().is_empty() {
            return Err(SchemaError::invalid_field("name", "must not be empty"));
        }
        if self.profile.trim().is_empty() {
            return Err(SchemaError::invalid_field("profile", "must not be empty"));
        }
        if self.claim_eligible {
            return Err(SchemaError::invalid_field(
                "claim_eligible",
                "no experiment in this milestone may support a published claim",
            ));
        }
        self.validate_load()?;
        self.validate_shape()?;
        self.validate_subjects()?;
        self.validate_runtime()
    }

    fn validate_load(&self) -> SchemaResult<()> {
        if self.records == 0 {
            return Err(SchemaError::invalid_field(
                "records",
                "a measured phase of zero records measures nothing",
            ));
        }
        match self.load_mode {
            LoadMode::ScheduledOpenLoopFixedRate => {
                match self.offered_records_per_second {
                    None => {
                        return Err(SchemaError::invalid_field(
                            "offered_records_per_second",
                            "a fixed-rate experiment must state the rate it offers",
                        ));
                    }
                    Some(0) => {
                        return Err(SchemaError::invalid_field(
                            "offered_records_per_second",
                            "must be greater than zero",
                        ));
                    }
                    Some(_) => {}
                }
                if self.arrival.is_none() {
                    return Err(SchemaError::invalid_field(
                        "arrival",
                        "a fixed-rate experiment must state its arrival process",
                    ));
                }
            }
            LoadMode::ClosedLoop => {
                if self.offered_records_per_second.is_some() {
                    return Err(SchemaError::invalid_field(
                        "offered_records_per_second",
                        "a closed-loop experiment does not offer a chosen rate",
                    ));
                }
                if self.arrival.is_some() {
                    return Err(SchemaError::invalid_field(
                        "arrival",
                        "a closed-loop experiment has no arrival schedule",
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_shape(&self) -> SchemaResult<()> {
        match self.kind {
            ExperimentKind::Producer => {
                if self.producer.is_none() {
                    return Err(SchemaError::invalid_field(
                        "producer",
                        "a producer experiment must carry a producer section",
                    ));
                }
            }
        }
        if self.payload.bytes == 0 {
            return Err(SchemaError::invalid_field(
                "payload.bytes",
                "must be greater than zero",
            ));
        }
        if self.application.producer_instances == 0 {
            return Err(SchemaError::invalid_field(
                "application.producer_instances",
                "must be greater than zero",
            ));
        }
        if self.application.callers_per_producer == 0 {
            return Err(SchemaError::invalid_field(
                "application.callers_per_producer",
                "must be greater than zero",
            ));
        }
        if self.cluster.brokers == 0 {
            return Err(SchemaError::invalid_field(
                "cluster.brokers",
                "must be greater than zero",
            ));
        }
        if self.cluster.partitions == 0 {
            return Err(SchemaError::invalid_field(
                "cluster.partitions",
                "must be greater than zero",
            ));
        }
        if self.cluster.replication_factor == 0 {
            return Err(SchemaError::invalid_field(
                "cluster.replication_factor",
                "must be greater than zero",
            ));
        }
        if self.cluster.min_in_sync_replicas == 0
            || self.cluster.min_in_sync_replicas > self.cluster.replication_factor
        {
            return Err(SchemaError::invalid_field(
                "cluster.min_in_sync_replicas",
                "must be between one and the replication factor",
            ));
        }
        Ok(())
    }

    fn validate_subjects(&self) -> SchemaResult<()> {
        if self.subjects.is_empty() {
            return Err(SchemaError::invalid_field(
                "subjects",
                "an experiment with no subjects measures nothing",
            ));
        }
        for (index, subject) in self.subjects.iter().enumerate() {
            let field = format!("subjects[{index}].name");
            if !is_topic_charset_safe(&subject.name) {
                return Err(SchemaError::invalid_field(
                    &field,
                    "must use only ASCII alphanumerics, '.', '_', and '-', \
                     because it becomes part of a topic name",
                ));
            }
            if subject.name.len() > MAX_SUBJECT_NAME_LENGTH {
                return Err(SchemaError::invalid_field(
                    &field,
                    "is longer than a subject name may be",
                ));
            }
            if self
                .subjects
                .iter()
                .filter(|other| other.name == subject.name)
                .count()
                > 1
            {
                return Err(SchemaError::invalid_field(
                    &field,
                    "appears more than once, so its evidence would overwrite itself",
                ));
            }
            if subject.adapter_name.trim().is_empty() {
                return Err(SchemaError::invalid_field(
                    &format!("subjects[{index}].adapter_name"),
                    "must not be empty",
                ));
            }
            if subject.command.is_empty() {
                return Err(SchemaError::invalid_field(
                    &format!("subjects[{index}].command"),
                    "must name the program to run",
                ));
            }
        }
        Ok(())
    }

    fn validate_runtime(&self) -> SchemaResult<()> {
        let Some(runtime) = &self.runtime else {
            return Ok(());
        };
        if runtime.bootstrap.trim().is_empty() {
            return Err(SchemaError::invalid_field(
                "runtime.bootstrap",
                "must name at least one broker",
            ));
        }
        if runtime.run_id.len() != 16
            || !runtime
                .run_id
                .chars()
                .all(|character| character.is_ascii_hexdigit() && !character.is_ascii_uppercase())
        {
            return Err(SchemaError::invalid_field(
                "runtime.run_id",
                "must be sixteen lowercase hexadecimal characters",
            ));
        }
        if !is_topic_charset_safe(&runtime.topic_prefix) {
            return Err(SchemaError::invalid_field(
                "runtime.topic_prefix",
                "must use only characters legal in a topic name",
            ));
        }
        for subject in &self.subjects {
            let Some(topics) = runtime.topics.get(&subject.name) else {
                return Err(SchemaError::invalid_field(
                    &format!("runtime.topics.{}", subject.name),
                    "every subject needs its own topics",
                ));
            };
            validate_topic_name(
                &format!("runtime.topics.{}.measured", subject.name),
                &topics.measured,
            )?;
            validate_topic_name(
                &format!("runtime.topics.{}.warmup", subject.name),
                &topics.warmup,
            )?;
            if topics.measured == topics.warmup {
                return Err(SchemaError::invalid_field(
                    &format!("runtime.topics.{}", subject.name),
                    "the warmup and measured topics must differ",
                ));
            }
        }
        if runtime.topics.len() != self.subjects.len() {
            return Err(SchemaError::invalid_field(
                "runtime.topics",
                "names topics for a subject the experiment does not have",
            ));
        }
        if runtime.execution_order.len() != self.subjects.len()
            || !runtime
                .execution_order
                .iter()
                .all(|name| self.subject(name).is_some())
            || (1..runtime.execution_order.len()).any(|index| {
                runtime.execution_order[index..].contains(&runtime.execution_order[index - 1])
            })
        {
            return Err(SchemaError::invalid_field(
                "runtime.execution_order",
                "must list every subject exactly once",
            ));
        }
        Ok(())
    }
}

fn validate_topic_name(field: &str, topic: &str) -> SchemaResult<()> {
    if !is_topic_charset_safe(topic) {
        return Err(SchemaError::invalid_field(
            field,
            "must use only ASCII alphanumerics, '.', '_', and '-'",
        ));
    }
    if topic.len() > MAX_TOPIC_NAME_LENGTH {
        return Err(SchemaError::invalid_field(
            field,
            "is longer than Kafka accepts",
        ));
    }
    Ok(())
}
