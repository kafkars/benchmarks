//! Strict deserialization of the hand-authored `guardrails.toml`.
//!
//! `deny_unknown_fields` throughout, and a checked `schema` number. A policy
//! file is a reviewed document: a key nobody recognises is far more likely to
//! be a rule somebody thought they were writing than a harmless extra, and
//! silently ignoring it would let a reviewer believe a check exists that never
//! runs.

use std::fmt;
use std::fs;
use std::path::Path;

use serde::Deserialize;

/// Policy schema this crate understands.
const SUPPORTED_SCHEMA: u32 = 1;

/// The complete checked-in guardrail policy.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    /// Policy schema number; must equal the one this crate supports.
    pub schema: u32,
    /// Source roots subject to the policy.
    pub paths: Paths,
    /// Per-category line budgets and their reviewed exceptions.
    pub budgets: Budgets,
    /// Crates banned from the harness's own dependency graph.
    pub forbidden_transitive_dependencies: ForbiddenDependencies,
}

/// Source roots subject to the policy, repository-relative.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Paths {
    /// Directories walked for `.rs` files.
    pub rust_roots: Vec<String>,
}

/// Per-category line budgets plus the justified exceptions to the gate.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Budgets {
    /// Budget for `lib.rs` and `mod.rs`.
    pub facade: Budget,
    /// Budget for `main.rs` and ordinary modules.
    pub implementation: Budget,
    /// Budget for `*_test.rs` siblings and `tests/**`.
    pub test: Budget,
    /// Budget for binaries, shared test helpers, and `cfg(test)` fixtures.
    pub auxiliary: Budget,
    /// Per-file exceptions to the soft gate.
    #[serde(default)]
    pub baseline: Vec<Baseline>,
}

/// A design target and the gate that actually fails a build.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Budget {
    /// Design goal; exceeding it prints an advisory and never fails.
    pub target: usize,
    /// Hard gate; exceeding it fails unless a baseline entry justifies it.
    pub soft: usize,
}

/// One reviewed file frozen at an exact length above its category's gate.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Baseline {
    /// Repository-relative path, forward slashes, no `.` or `..` components.
    pub path: String,
    /// The file's exact measured length when the exception was reviewed.
    pub lines: usize,
    /// Why this file is allowed to be this long. Must not be blank.
    pub reason: String,
}

/// Crates that may not appear anywhere in the harness's own lock file.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForbiddenDependencies {
    /// Package names banned from the whole transitive graph.
    pub crates: Vec<String>,
}

/// Why a policy file could not be turned into a [`Policy`].
#[derive(Debug)]
pub enum PolicyError {
    /// The file could not be read.
    Unreadable(String),
    /// The file is not valid TOML, or does not match the policy shape.
    Malformed(String),
    /// The file declares a schema number this crate does not implement.
    UnsupportedSchema(u32),
}

impl fmt::Display for PolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unreadable(detail) | Self::Malformed(detail) => formatter.write_str(detail),
            Self::UnsupportedSchema(schema) => {
                write!(
                    formatter,
                    "guardrails.toml declares unsupported schema {schema}"
                )
            }
        }
    }
}

/// Read and validate the policy that governs `repository_root`.
pub fn load_policy(repository_root: &Path) -> Result<Policy, PolicyError> {
    let path = repository_root.join("guardrails.toml");
    let source = fs::read_to_string(&path)
        .map_err(|error| PolicyError::Unreadable(format!("read {}: {error}", path.display())))?;
    parse_policy(&source)
}

/// Validate an in-memory policy document, so tests need no file on disk.
pub fn parse_policy(source: &str) -> Result<Policy, PolicyError> {
    let policy: Policy = toml::from_str(source)
        .map_err(|error| PolicyError::Malformed(format!("parse guardrails.toml: {error}")))?;
    if policy.schema == SUPPORTED_SCHEMA {
        Ok(policy)
    } else {
        Err(PolicyError::UnsupportedSchema(policy.schema))
    }
}

impl Budgets {
    /// The budget governing one category.
    #[must_use]
    pub const fn for_category(&self, category: crate::Category) -> Budget {
        match category {
            crate::Category::Facade => self.facade,
            crate::Category::Implementation => self.implementation,
            crate::Category::Test => self.test,
            crate::Category::Auxiliary => self.auxiliary,
        }
    }
}
