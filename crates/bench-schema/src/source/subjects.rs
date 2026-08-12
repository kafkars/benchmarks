//! The subject list: which adapters to run, and how to invoke them.

use serde::{Deserialize, Serialize};

use crate::error::{SchemaError, SchemaResult};
use crate::experiment::{SUBJECT_ROLES, is_subject_role};

use super::parse::parse_toml;

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
