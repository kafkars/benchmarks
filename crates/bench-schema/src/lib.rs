//! The evidence vocabulary: every versioned document a benchmark attempt reads
//! or writes, the canonical bytes those documents hash to, and the two
//! identities derived from them.
//!
//! **This crate is a stub.** It carries the contract below so that the layout,
//! the workspace graph, and the lint gate are real from the first commit; the
//! implementation lands in the following wave. Nothing depends on it yet.
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
//! # What this crate will own
//!
//! - `error` — `SchemaError { kind, context }` over the `Parse`, `WrongSchema`,
//!   `InvalidField`, `NonCanonicalNumber`, and `Identity` failure kinds.
//! - `schema_id` — the schema-id constants and `require_schema(actual,
//!   expected)`. The registry covers all 26 ids: the 11 new engine ids
//!   (`kafkars.experiment.v1`, `kafkars.adapter.v1`,
//!   `kafkars.adapter-status.v1`, `kafkars.adapter-validate.v1`,
//!   `kafkars.run-status.v1`, `kafkars.bundle.v1`,
//!   `kafkars.subjects-lock.v1`, `kafkars.benchmark-environment.v2`,
//!   `kafkars.classification.v1`, `kafkars.comparison.v1`,
//!   `kafkars.execution-order.v1`) plus the 15 legacy ids the migrated harness
//!   still produces. A test asserts the registry and the `schemas/` directory
//!   listing agree in both directions.
//! - `canon` — canonical JSON. Keys are byte-sorted because the map type is
//!   ordered, never because a serializer preserved declaration order; compact
//!   form is what gets hashed, and the pretty form is two-space indented with a
//!   trailing newline so it stays byte-compatible with the legacy Node writer.
//!   Floating-point numbers are rejected outright in identity documents.
//! - `identity` — `ExperimentId` and the sha-256 helpers. The experiment id
//!   hashes the canonical resolved experiment minus two keys, `runtime` and
//!   each `SubjectSpec.command`, so that the same intent run against a
//!   different bootstrap, or with a subject binary at a different path, keeps
//!   the same identity. The exclusion is golden-tested precisely because it is
//!   the kind of rule that quietly drifts.
//! - `experiment` — `ResolvedExperiment` (`kafkars.experiment.v1`) with its
//!   cross-field `validate()`, plus the application, payload, producer, budget,
//!   cluster, and service-level specs, the subject list, and the optional
//!   runtime binding. Identity-relevant numbers are integers only.
//! - `source` — the human-authored TOML types that resolve into the above.
//! - `adapter` — `AdapterDescription` (`kafkars.adapter.v1`), `AdapterStatus`,
//!   and `ValidateReport`: what an adapter says it can do, how an attempt of it
//!   ended, and whether it accepts a given experiment.
//! - `status` — `RunStatus` (`kafkars.run-status.v1`) and `ExecutionOrder`. The
//!   execution-status wire strings are pinned by tests.
//! - `bundle` — `BundleManifest` plus the checksum line format: a 64-character
//!   hex digest, two spaces, a slash-separated relative path, byte-order
//!   sorted, newline terminated. The format is byte-compatible with the legacy
//!   sealer and verifiable with `shasum -a 256 -c`.
//! - `lock` — `SubjectsLock`: per subject, the command, the binary digest, the
//!   capability document, and the validation report actually observed.
//! - `environment` — `EnvironmentDocument`
//!   (`kafkars.benchmark-environment.v2`), repository-agnostic, whose identity
//!   ignores the capture timestamp.
//! - `classify` — `Classification` and `Comparison`. Validity and claim
//!   eligibility are a separate axis from execution status; ratios live here,
//!   which is the one place floating-point numbers are welcome, because a
//!   comparison is evidence and never an identity.
//! - `legacy` — deliberately lenient read-only views of the current adapter
//!   output. Everything the views do not name stays opaque bytes and is never
//!   re-serialized.
#![forbid(unsafe_code)]
