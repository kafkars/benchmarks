//! The subjects an experiment measures, the topics they were bound to, and the
//! rules both have to satisfy.
//!
//! The types and their cross-field rules are one concern: every rule here
//! exists because a subject name travels into a topic name, an evidence path,
//! and a comparison column, and a binding that names the wrong topics would
//! measure the wrong subject rather than fail.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::{SchemaError, SchemaResult};

use super::{
    naming::{
        MAX_SUBJECT_NAME_LENGTH, MAX_TOPIC_NAME_LENGTH, SUBJECT_ROLES, is_subject_role,
        is_topic_charset_safe,
    },
    resolved::ResolvedExperiment,
};

/// One thing being measured: a name, the adapter behind it, and how to run it.
///
/// `command` is excluded from the experiment id. The same subject built into a
/// different directory, or invoked through an absolute rather than a relative
/// path, is the same subject; what it actually was at run time is recorded in
/// the subjects lock instead.
///
/// `role` is *not* excluded. It says what the subject is for — which side of a
/// comparison it is, or that it is the anchor held fixed across attempts — and
/// two experiments that disagree about that are asking different questions even
/// when every other field matches. It is absent by default and omitted from the
/// bytes when absent, so an unlabeled subject list keeps the id it always had.
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
    /// One of [`SUBJECT_ROLES`], or absent for an unlabeled subject.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
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

impl ResolvedExperiment {
    pub(super) fn validate_subjects(&self) -> SchemaResult<()> {
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
            if let Some(role) = &subject.role
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

    pub(super) fn validate_runtime(&self) -> SchemaResult<()> {
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
