//! The evidence vocabulary: every versioned document a benchmark attempt reads
//! or writes, the canonical bytes those documents hash to, and the two
//! identities derived from them.
//!
//! # Why a schema crate exists at all
//!
//! A benchmark result is only worth as much as the reader's ability to say what
//! it measured. That makes the document set — not the measuring code — the
//! stable surface of this repository. Schemas are versioned and append-only: a
//! field may be added, a new schema id may be minted, and an existing field's
//! meaning may never be changed under the same id. Sealed bundles are
//! immutable, so a mutation is not an edit, it is a retroactive lie about runs
//! that already happened.
//!
//! This crate is pure. It never spawns a process, opens a socket, reads a
//! clock, or touches a broker; the control plane does all of that and hands the
//! bytes here.
//!
//! # The two identities
//!
//! [`experiment_id`] hashes *intent*: the resolved experiment with the runtime
//! binding and the subject commands removed, so that the same experiment run
//! tomorrow, on another cluster, from another checkout, keeps its id.
//! [`BundleManifest::bundle_digest`] hashes *evidence*: the bytes of
//! `checksums.txt`, which in turn covers every other file in the sealed bundle.
//! One aggregates repetitions; the other detects tampering. Neither can do the
//! other's job.
//!
//! # Strict where we write, lenient where we read
//!
//! Documents this repository authors deny unknown fields, because a key nobody
//! reads is a setting somebody believes is in effect. Documents it merely reads
//! — [`KnownProducerResult`] and [`VerifierReport`] over adapter output — name
//! only the fields the control plane needs and ignore everything else, because
//! refusing a sealed result for growing a key would invalidate evidence after
//! the fact. Those views cannot serialize, so they cannot rewrite what they
//! read.
//!
//! # Layout
//!
//! - `error` — the single failure type, a kind plus context.
//! - `schema_id` — the 26 schema ids and [`require_schema`].
//! - `canon` — canonical and pretty JSON bytes; float rejection.
//! - `identity` — [`ExperimentId`], the two-key exclusion, sha-256 helpers.
//! - `experiment` — `kafkars.experiment.v1` and its cross-field rules.
//! - `source` — the human-authored TOML that resolves into it.
//! - `adapter` — the adapter protocol's three documents.
//! - `status` — how an attempt ended, and in what order it ran.
//! - `bundle` — the checksum manifest and the bundle digest.
//! - `lock` — what each subject actually was at probe time.
//! - `environment` — the machine, repository-agnostic, identity-bearing.
//! - `classify` — validity, claim eligibility, and comparison ratios.
//! - `legacy` — lenient views of the documents today's adapters write.
#![forbid(unsafe_code)]

mod adapter;
mod bundle;
mod canon;
mod classify;
mod environment;
mod error;
mod experiment;
mod histogram;
mod identity;
mod legacy;
mod lock;
mod result_v2;
mod schema_id;
mod source;
mod status;

pub use adapter::{
    AdapterCapabilities, AdapterDescription, AdapterFailure, AdapterOutcome, AdapterStatus,
    ValidateReport,
};
pub use bundle::{
    BundleManifest, CHECKSUM_SEPARATOR, ChecksumEntry, parse_checksums, render_checksums,
};
pub use canon::{
    canonical_bytes, canonical_bytes_of_value, parse_json_slice, parse_json_str, pretty_bytes,
    pretty_bytes_of_value, reject_floats, to_canonical_value,
};
pub use classify::{Classification, Comparison, ComparisonPair, SubjectValidity};
pub use environment::{BrokerFacts, EnvironmentDocument, HostFacts, RepositoryState, UNAVAILABLE};
pub use error::{SchemaError, SchemaErrorKind, SchemaResult};
pub use experiment::{
    ApplicationSpec, ArrivalModel, BudgetSpec, ClusterSpec, ExperimentKind, LoadMode,
    MAX_SUBJECT_NAME_LENGTH, MAX_TOPIC_NAME_LENGTH, PayloadSpec, ProducerSpec, ResolvedExperiment,
    RuntimeBinding, SloSpec, SubjectSpec, TopicPair, is_topic_charset_safe,
};
pub use histogram::{
    EncodedHistogram, HISTOGRAM_LAYOUT_V1, Histogram, SUB_BUCKET_BITS, SUB_BUCKET_COUNT,
    bucket_high, bucket_index, bucket_low,
};
pub use identity::{
    DIGEST_HEX_LENGTH, ExperimentId, SHORT_ID_LENGTH, experiment_id, identity_bytes,
    identity_document, is_digest_hex, sha256_hex,
};
pub use legacy::{KnownProducerResult, LegacyLatency, LegacyPercentiles, VerifierReport};
pub use lock::{SubjectLockEntry, SubjectsLock};
pub use result_v2::{
    DeclaredExecution, MeasuredThroughput, OfferOutcomes, OfferTiming, PRODUCER_BENCHMARK_V2,
    ProcessResources, ProducerBenchmarkV2, QueueObservation,
};
pub use schema_id::{
    ADAPTER_STATUS_V1, ADAPTER_V1, ADAPTER_VALIDATE_V1, BENCHMARK_ADAPTER_CONFIG_V1,
    BENCHMARK_ENVIRONMENT_V1, BENCHMARK_ENVIRONMENT_V2, BUNDLE_V1, CLASSIFICATION_V1,
    COMPARISON_V1, ENGINE_SCHEMA_IDS, EXECUTION_ORDER_V1, EXPERIMENT_V1, LEGACY_SCHEMA_IDS,
    LIBRDKAFKA_CAPACITY_CURVE_V2, LIBRDKAFKA_CAPACITY_PROBE_V2, LIBRDKAFKA_NATIVE_METRICS_V1,
    LIBRDKAFKA_STATISTICS_V1, PROCESS_RESOURCES_V2, PRODUCER_BENCHMARK_V1,
    PRODUCER_COMPARISON_SUITE_V1, PRODUCER_COMPARISON_V1, PRODUCER_FIXED_COMPARISON_SUITE_V2,
    PRODUCER_FIXED_COMPARISON_V2, PRODUCER_FIXED_LOAD_V1, PRODUCER_FIXED_MATRIX_V2,
    PRODUCER_VERIFICATION_V1, RUN_STATUS_V1, SCHEMA_FILE_SUFFIX, SUBJECTS_LOCK_V1, is_registered,
    registered_schema_ids, require_schema, schema_file_name, schema_id_from_file_name,
};
pub use source::{
    ClusterProfile, ClusterTools, SourceApplication, SourceApplicationApi, SourceCluster,
    SourceExperiment, SourceLoadMode, SourceNativeRequestConcurrency, SourcePayload,
    SourceProducer, SourceSearch, SourceSlo, SourceValidity, SubjectEntry, SubjectsFile,
};
pub use status::{
    ExecutionOrder, ExecutionStatus, PhaseOutcome, PhaseRecord, ProcessExit, RunStatus,
    SubjectExecution, SubjectVerification, VerificationOutcome,
};

#[cfg(test)]
mod adapter_test;
#[cfg(test)]
mod bundle_test;
#[cfg(test)]
mod canon_test;
#[cfg(test)]
mod classify_test;
#[cfg(test)]
mod environment_test;
#[cfg(test)]
mod error_test;
#[cfg(test)]
mod experiment_test;
#[cfg(test)]
mod histogram_test;
#[cfg(test)]
mod identity_test;
#[cfg(test)]
mod legacy_test;
#[cfg(test)]
mod lock_test;
#[cfg(test)]
mod result_v2_test;
#[cfg(test)]
mod schema_id_test;
#[cfg(test)]
mod source_test;
#[cfg(test)]
mod status_test;
