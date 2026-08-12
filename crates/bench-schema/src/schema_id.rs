//! The schema registry: every document identifier this repository may write or
//! read, and the check that a document is the one a caller asked for.
//!
//! Schema ids are the stable surface of the whole benchmark lab. A sealed
//! bundle outlives the code that produced it, so the id is what tells a future
//! reader which contract the bytes were written under. Ids are versioned and
//! append-only: a field may be added under the same id, but changing what an
//! existing field *means* requires a new id, because sealed evidence cannot be
//! re-interpreted after the fact.
//!
//! The registry is written out by hand. Deriving it from the `schemas/`
//! directory listing would make the completeness check tautological — the point
//! is that minting, renaming, or retiring an id is a decision someone states in
//! a diff, not a side effect of touching a directory.
//!
//! Two families live here. The **engine** ids are minted by the Rust control
//! plane described in this repository's harness design. The **legacy** ids are
//! inherited from the Node harness under `legacy/benchctl/`, which still
//! produces them today; every legacy string below was read out of the `.mjs`
//! file that writes it, because the producing code — not the plan — is the
//! authority on what an id actually says.

use crate::error::{SchemaError, SchemaResult};

/// Resolved experiment intent, the document the experiment id is derived from.
pub const EXPERIMENT_V1: &str = "kafkars.experiment.v1";
/// Adapter capability document printed by `<adapter> describe --json`.
pub const ADAPTER_V1: &str = "kafkars.adapter.v1";
/// Terminal outcome an adapter writes for its own run.
pub const ADAPTER_STATUS_V1: &str = "kafkars.adapter-status.v1";
/// Adapter verdict on whether it can honour a resolved experiment.
pub const ADAPTER_VALIDATE_V1: &str = "kafkars.adapter-validate.v1";
/// Control-plane record of how one attempt ended.
pub const RUN_STATUS_V1: &str = "kafkars.run-status.v1";
/// Sealed-bundle manifest carrying the bundle digest.
pub const BUNDLE_V1: &str = "kafkars.bundle.v1";
/// What each subject actually was at probe time: command, binary, capabilities.
pub const SUBJECTS_LOCK_V1: &str = "kafkars.subjects-lock.v1";
/// Repository-agnostic environment capture.
pub const BENCHMARK_ENVIRONMENT_V2: &str = "kafkars.benchmark-environment.v2";
/// Validity and claim eligibility for one attempt.
pub const CLASSIFICATION_V1: &str = "kafkars.classification.v1";
/// Pairwise comparison between subjects of one attempt.
pub const COMPARISON_V1: &str = "kafkars.comparison.v1";
/// Order the subjects were executed in, and what decided it.
pub const EXECUTION_ORDER_V1: &str = "kafkars.execution-order.v1";

/// Legacy environment capture written by `legacy/benchctl/environment.mjs`.
pub const BENCHMARK_ENVIRONMENT_V1: &str = "kafkars.benchmark-environment.v1";
/// Legacy normalized adapter settings written by `legacy/benchctl/seal.mjs`.
pub const BENCHMARK_ADAPTER_CONFIG_V1: &str = "kafkars.benchmark-adapter-config.v1";
/// Legacy closed-loop producer result printed by both producer adapters.
pub const PRODUCER_BENCHMARK_V1: &str = "kafkars.producer-benchmark.v1";
/// Legacy scheduled fixed-rate producer result printed by both adapters.
pub const PRODUCER_FIXED_LOAD_V1: &str = "kafkars.producer-fixed-load.v1";
/// Legacy read-back verification printed by the C verifier.
pub const PRODUCER_VERIFICATION_V1: &str = "kafkars.producer-verification.v1";
/// Legacy closed-loop paired comparison written by `legacy/benchctl/seal.mjs`.
pub const PRODUCER_COMPARISON_V1: &str = "kafkars.producer-comparison.v1";
/// Legacy closed-loop suite summary written by `legacy/benchctl/suite.mjs`.
pub const PRODUCER_COMPARISON_SUITE_V1: &str = "kafkars.producer-comparison-suite.v1";
/// Legacy fixed-rate paired comparison written by
/// `legacy/benchctl/fixed-seal.mjs`.
pub const PRODUCER_FIXED_COMPARISON_V2: &str = "kafkars.producer-fixed-comparison.v2";
/// Legacy fixed-rate suite summary written by `legacy/benchctl/fixed-suite.mjs`.
pub const PRODUCER_FIXED_COMPARISON_SUITE_V2: &str = "kafkars.producer-fixed-comparison-suite.v2";
/// Legacy single capacity probe written by
/// `legacy/benchctl/reference-probe-seal.mjs`.
pub const LIBRDKAFKA_CAPACITY_PROBE_V2: &str = "kafkars.librdkafka-capacity-probe.v2";
/// Legacy capacity curve written by
/// `legacy/benchctl/reference-capacity-runner.mjs`.
pub const LIBRDKAFKA_CAPACITY_CURVE_V2: &str = "kafkars.librdkafka-capacity-curve.v2";
/// Legacy fixed-load matrix written by `legacy/benchctl/fixed-matrix-runner.mjs`.
pub const PRODUCER_FIXED_MATRIX_V2: &str = "kafkars.producer-fixed-matrix.v2";
/// Legacy process CPU and peak memory written by
/// `legacy/benchctl/process-resources.mjs`.
pub const PROCESS_RESOURCES_V2: &str = "kafkars.process-resources.v2";
/// Legacy librdkafka statistics snapshot validated by
/// `legacy/benchctl/librdkafka-statistics/schema.mjs`.
pub const LIBRDKAFKA_STATISTICS_V1: &str = "kafkars.librdkafka-statistics.v1";
/// Legacy librdkafka native metric summary written by
/// `legacy/benchctl/librdkafka-statistics.mjs`.
pub const LIBRDKAFKA_NATIVE_METRICS_V1: &str = "kafkars.librdkafka-native-metrics.v1";

/// Every schema id minted by the Rust engine.
pub const ENGINE_SCHEMA_IDS: [&str; 11] = [
    EXPERIMENT_V1,
    ADAPTER_V1,
    ADAPTER_STATUS_V1,
    ADAPTER_VALIDATE_V1,
    RUN_STATUS_V1,
    BUNDLE_V1,
    SUBJECTS_LOCK_V1,
    BENCHMARK_ENVIRONMENT_V2,
    CLASSIFICATION_V1,
    COMPARISON_V1,
    EXECUTION_ORDER_V1,
];

/// Every schema id inherited from the legacy Node harness.
///
/// These are still produced today by `legacy/benchctl/` and by the migrated
/// adapters, so they are part of the vocabulary whether or not the Rust engine
/// writes them.
pub const LEGACY_SCHEMA_IDS: [&str; 15] = [
    BENCHMARK_ENVIRONMENT_V1,
    BENCHMARK_ADAPTER_CONFIG_V1,
    PRODUCER_BENCHMARK_V1,
    PRODUCER_FIXED_LOAD_V1,
    PRODUCER_VERIFICATION_V1,
    PRODUCER_COMPARISON_V1,
    PRODUCER_COMPARISON_SUITE_V1,
    PRODUCER_FIXED_COMPARISON_V2,
    PRODUCER_FIXED_COMPARISON_SUITE_V2,
    LIBRDKAFKA_CAPACITY_PROBE_V2,
    LIBRDKAFKA_CAPACITY_CURVE_V2,
    PRODUCER_FIXED_MATRIX_V2,
    PROCESS_RESOURCES_V2,
    LIBRDKAFKA_STATISTICS_V1,
    LIBRDKAFKA_NATIVE_METRICS_V1,
];

/// Suffix every schema document file in `schemas/` carries.
pub const SCHEMA_FILE_SUFFIX: &str = ".schema.json";

/// Returns every registered schema id, engine ids first, then legacy ids.
///
/// The order is the declaration order of the two arrays, which is grouped by
/// family rather than sorted; sort the result when comparing against a
/// directory listing.
pub fn registered_schema_ids() -> Vec<&'static str> {
    ENGINE_SCHEMA_IDS
        .into_iter()
        .chain(LEGACY_SCHEMA_IDS)
        .collect()
}

/// Returns the `schemas/` file name that documents `id`.
pub fn schema_file_name(id: &str) -> String {
    format!("{id}{SCHEMA_FILE_SUFFIX}")
}

/// Returns the schema id a `schemas/` file name documents, if it is one.
pub fn schema_id_from_file_name(file_name: &str) -> Option<&str> {
    file_name.strip_suffix(SCHEMA_FILE_SUFFIX)
}

/// Reports whether `id` is a registered schema id.
pub fn is_registered(id: &str) -> bool {
    ENGINE_SCHEMA_IDS.contains(&id) || LEGACY_SCHEMA_IDS.contains(&id)
}

/// Fails unless a document's declared schema id is exactly the expected one.
///
/// This is the fail-closed gate every reader in this crate goes through. A
/// document that declares a different id is not a document with an unexpected
/// field — it is a different contract, and guessing at it is how evidence
/// starts lying.
pub fn require_schema(actual: &str, expected: &str) -> SchemaResult<()> {
    if actual == expected {
        return Ok(());
    }
    Err(SchemaError::wrong_schema(format!(
        "expected schema {expected}, found {actual}"
    )))
}
