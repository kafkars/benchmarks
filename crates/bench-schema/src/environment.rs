//! `kafkars.benchmark-environment.v2`: the machine, the toolchain, the source
//! revisions, and the broker — captured once per attempt.
//!
//! Version 2 differs from the legacy `kafkars.benchmark-environment.v1` in one
//! way that matters: it is repository-agnostic. Version 1 hard-coded three
//! Kafka repositories and a librdkafka artifact pin, which was honest for a
//! harness that lived inside one client's repository and could only ever
//! measure that client. A generic benchmark lab cannot name its subjects in
//! advance, so `source` here is a map from a caller-chosen repository name to
//! its commit and dirty flag, and adapter provenance moved to the subjects
//! lock, which is where the binary digest already lives.
//!
//! The identity rule is inherited unchanged from v1's
//! `benchmarkEnvironmentIdentity`: the digest covers the whole document with
//! `captured_at` removed. Two attempts on the same unchanged machine share an
//! environment identity, which is what lets a reader say that a difference
//! between them is not the environment. The one deliberate difference from v1
//! is that the bytes are canonical — byte-sorted keys — where Node hashed
//! `JSON.stringify` in declaration order, so the two digests are not comparable
//! across the harness boundary and were never meant to be.
//!
//! Fields are strings with an `unavailable` fallback rather than options,
//! mirroring the legacy capture: a benchmark that cannot read its own CPU model
//! should say so in the evidence, not omit the key and let a reader assume
//! nothing was there to read.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::canon;
use crate::error::SchemaResult;
use crate::identity::sha256_hex;
use crate::schema_id::BENCHMARK_ENVIRONMENT_V2;

/// Value recorded when a fact could not be read.
pub const UNAVAILABLE: &str = "unavailable";

/// A source repository's state at capture time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryState {
    /// Commit the repository was checked out at, or [`UNAVAILABLE`].
    pub commit: String,
    /// Whether the working tree had uncommitted changes.
    ///
    /// A dirty tree does not invalidate a diagnostic run, but it does mean the
    /// run can never support a published claim, so the flag is captured rather
    /// than assumed false.
    pub dirty: bool,
}

/// The host the attempt ran on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostFacts {
    /// Operating system name.
    pub platform: String,
    /// Kernel or OS release string.
    pub release: String,
    /// CPU architecture.
    pub architecture: String,
    /// CPU model, or [`UNAVAILABLE`].
    pub cpu: String,
    /// Logical CPUs visible to the process; zero when unreadable.
    pub logical_cpus: u32,
    /// Physical memory in bytes; zero when unreadable.
    pub memory_bytes: u64,
    /// Full `uname -a` output, or [`UNAVAILABLE`].
    pub uname: String,
}

/// The cluster the attempt produced to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerFacts {
    /// Broker version as reported by the operator or the cluster.
    pub version: String,
    /// Bootstrap servers the subjects connected to.
    pub bootstrap: String,
    /// Who owns the cluster's lifecycle — for this repository, always the
    /// caller, because a harness that starts its own broker is measuring its
    /// own startup.
    pub lifecycle: String,
}

/// `kafkars.benchmark-environment.v2`: everything about the run that is not the
/// experiment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentDocument {
    /// Schema id, always [`EnvironmentDocument::SCHEMA`].
    pub schema: String,
    /// UTC RFC 3339 timestamp of the capture. Excluded from the identity.
    pub captured_at: String,
    /// Source repositories by caller-chosen name.
    pub source: BTreeMap<String, RepositoryState>,
    /// Toolchain versions by tool name — `rustc`, `cargo`, `cc`, and so on.
    pub toolchain: BTreeMap<String, String>,
    /// The host the attempt ran on.
    pub host: HostFacts,
    /// The cluster the attempt produced to.
    pub broker: BrokerFacts,
}

impl EnvironmentDocument {
    /// Schema id this document declares.
    pub const SCHEMA: &'static str = BENCHMARK_ENVIRONMENT_V2;

    /// Reports whether the document declares the expected schema id.
    pub fn has_expected_schema(&self) -> bool {
        self.schema == Self::SCHEMA
    }

    /// Returns the canonical bytes the environment identity is computed over:
    /// the whole document with `captured_at` removed.
    pub fn identity_bytes(&self) -> SchemaResult<Vec<u8>> {
        let mut document = canon::to_canonical_value(self)?;
        if let Some(object) = document.as_object_mut() {
            object.remove("captured_at");
        }
        canon::canonical_bytes_of_value(&document)
    }

    /// Returns the environment identity: sha-256 of [`Self::identity_bytes`].
    ///
    /// Two attempts that share this digest ran in the same environment, which
    /// is the precondition for comparing them to each other.
    pub fn identity(&self) -> SchemaResult<String> {
        Ok(sha256_hex(&self.identity_bytes()?))
    }
}
