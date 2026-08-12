//! Experiment resolution: source TOML plus cluster profile plus subject
//! describe/validate results, deterministically merged into one canonical
//! `kafkars.experiment.v1` document and its subjects lock. The pure core is
//! separated from process-spawning so goldens can pin its bytes.
//!
//! # Purity
//!
//! Nothing here spawns a process, reads a clock, or touches the filesystem.
//! Every fact the resolution needs — what each adapter said about itself, what
//! attempt this is, which bootstrap to bind to — arrives as an argument. That
//! is what makes [`resolve_pure`] golden-testable: the same inputs produce the
//! same bytes forever, and a change to those bytes is a change to what "the
//! same experiment" means.
//!
//! # Layout
//!
//! - `inputs` — [`ResolveInputs`], [`SubjectProbe`], [`RuntimeInputs`]: every
//!   fact the resolution is given.
//! - `experiment` — the two passes, [`resolve_experiment`] and [`resolve_pure`].
//! - `document` — the field mapping from scenario to resolved document.
//! - `subjects` — probed subjects into subject specifications, and the identity
//!   tokens that may be hashed into an experiment id.
//! - `binding` — the run id, the topic names, and the execution order.

mod binding;
#[cfg(test)]
mod binding_test;
mod document;
#[cfg(test)]
mod document_test;
mod experiment;
#[cfg(test)]
mod experiment_test;
mod inputs;
mod subjects;
#[cfg(test)]
mod subjects_test;

pub use self::binding::{RUN_ID_LENGTH, TOPIC_PREFIX, WARMUP_TOPIC_SUFFIX, derive_run_id};
pub use self::document::{DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_BROKER, slo_spec};
pub use self::experiment::{resolve_experiment, resolve_pure};
pub use self::inputs::{ResolveInputs, RuntimeInputs, SubjectProbe};
