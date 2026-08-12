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
//!
//! # Layout
//!
//! - `naming` — the name limits, the role vocabulary, and the topic charset.
//! - `mode` — what is measured, how load is offered, how arrivals are drawn.
//! - `spec` — the document's sections: application, payload, producer, budget,
//!   cluster, and objectives.
//! - `subject` — the subjects, the topics they were bound to, and their rules.
//! - `resolved` — the document itself, plus the load and shape rules.

mod mode;
mod naming;
mod resolved;
mod spec;
mod subject;

pub use mode::{ArrivalModel, ExperimentKind, LoadMode};
pub use naming::{
    MAX_SUBJECT_NAME_LENGTH, MAX_TOPIC_NAME_LENGTH, SUBJECT_ROLE_ANCHOR, SUBJECT_ROLE_BASE,
    SUBJECT_ROLE_HEAD, SUBJECT_ROLES, is_subject_role, is_topic_charset_safe,
};
pub use resolved::ResolvedExperiment;
pub use spec::{ApplicationSpec, BudgetSpec, ClusterSpec, PayloadSpec, ProducerSpec, SloSpec};
pub use subject::{RuntimeBinding, SubjectSpec, TopicPair};

#[cfg(test)]
mod mode_test;
#[cfg(test)]
mod naming_test;
#[cfg(test)]
mod spec_test;
#[cfg(test)]
mod subject_test;
