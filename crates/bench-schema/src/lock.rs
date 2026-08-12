//! `kafkars.subjects-lock.v1`: what each subject actually was, at the moment it
//! was asked.
//!
//! The resolved experiment says which subjects to measure and deliberately
//! excludes their commands from the identity. This document is where the
//! excluded facts land: the argument vector as invoked, the digest of the
//! binary behind it, the capabilities it claimed, and the verdict it gave on
//! this exact experiment.
//!
//! It exists because "kafkars was 3% faster" is not evidence unless somebody
//! can say which build of kafkars that was. Recording the describe and validate
//! documents verbatim also means a later reader can tell an adapter that
//! *declined* part of the experiment from one that never saw it.

use serde::{Deserialize, Serialize};

use crate::adapter::{AdapterDescription, ValidateReport};
use crate::schema_id::SUBJECTS_LOCK_V1;

/// One subject as observed at probe time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectLockEntry {
    /// Subject name, matching the resolved experiment.
    pub name: String,
    /// Argument vector as invoked, program first.
    pub command: Vec<String>,
    /// Sha-256 of the program file, when it could be read.
    ///
    /// Absent for a subject invoked through a wrapper, a shell builtin, or
    /// anything else that is not a readable file — which is recorded honestly
    /// rather than filled in with a placeholder.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binary_sha256: Option<String>,
    /// The capability document the adapter printed.
    pub describe: AdapterDescription,
    /// The adapter's verdict on this experiment.
    pub validate: ValidateReport,
}

/// `kafkars.subjects-lock.v1`: every subject, as probed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectsLock {
    /// Schema id, always [`SubjectsLock::SCHEMA`].
    pub schema: String,
    /// Subjects in the order the experiment declares them.
    pub subjects: Vec<SubjectLockEntry>,
}

impl SubjectsLock {
    /// Schema id this document declares.
    pub const SCHEMA: &'static str = SUBJECTS_LOCK_V1;

    /// Creates a lock document over the given entries.
    pub fn new(subjects: Vec<SubjectLockEntry>) -> Self {
        Self {
            schema: Self::SCHEMA.to_owned(),
            subjects,
        }
    }

    /// Returns the entry for a subject name.
    pub fn subject(&self, name: &str) -> Option<&SubjectLockEntry> {
        self.subjects.iter().find(|subject| subject.name == name)
    }

    /// Reports whether the document declares the expected schema id.
    pub fn has_expected_schema(&self) -> bool {
        self.schema == Self::SCHEMA
    }
}
