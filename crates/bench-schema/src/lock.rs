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
//!
//! # Why the arguments are digested too
//!
//! `binary_sha256` covers `command[0]`, which is the right answer only when
//! `command[0]` is the thing being measured. It often is not. A subject invoked
//! as `["shim", "--binary", "<some C producer>"]` is measuring the C producer,
//! and hashing the shim identifies the wrong program entirely — the digest moves
//! when the shim is rebuilt and stays put when the client under test is
//! replaced, which is precisely backwards.
//!
//! [`SubjectLockEntry::argument_binary_sha256s`] closes that by digesting every
//! *argument* that named a readable regular file at probe time. The rule is
//! mechanical and stated rather than clever: no attempt is made to understand
//! which flag means "the real binary", because guessing an adapter's argument
//! grammar would be a second thing to get wrong.

use std::collections::BTreeMap;

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
    /// Sha-256 of each further readable file named in the argument vector,
    /// keyed by the argument exactly as it was written.
    ///
    /// Keyed rather than positional so an entry stays readable when the file it
    /// covered has since moved, and so nothing has to re-apply the selection
    /// rule to work out which argument a digest belongs to. A `BTreeMap`
    /// because the canonical byte form sorts object keys anyway; making the
    /// order a property of the type keeps the document and its digest agreeing.
    ///
    /// Empty and omitted for the ordinary case where the program is the only
    /// file in the command, which is why this field could be appended to a
    /// sealed schema without changing a single existing document's bytes.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub argument_binary_sha256s: BTreeMap<String, String>,
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
