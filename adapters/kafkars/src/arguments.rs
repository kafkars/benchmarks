//! Strict command parsing for reproducible benchmark adapter processes.
//!
//! Two command surfaces live here and neither may drift from the other. The
//! legacy positional commands are what the migrated Node control plane drives,
//! and their stdout bytes are evidence that has already been sealed, so they
//! are preserved exactly. The three adapter-protocol verbs — `describe`,
//! `validate`, `run` — take a resolved experiment document instead, and reach
//! the same phase code through `protocol`.
//!
//! # Layout
//!
//! - `dispatch` — the verb table and the process entry point.
//! - `produce` — the two produce verbs' argument shapes and parsers.
//! - `topic` — the topic verbs' argument shapes and parsers.
//! - `experiment` — flag parsing for the three protocol verbs.
//! - `emit` — the verbs that print a deterministic artefact.
//! - `parse` — the usage string, the validators, the numeric parsers.

mod dispatch;
mod emit;
mod experiment;
mod parse;
mod produce;
mod topic;

pub use dispatch::run;

pub(crate) use parse::MIN_PAYLOAD_BYTES;
pub(crate) use produce::{FixedProduceArgs, ProduceArgs};
pub(crate) use topic::{TopicArgs, TopicDeletionArgs};
