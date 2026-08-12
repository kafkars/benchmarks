//! The adapter protocol: `describe`, `validate`, and `run` over the resolved
//! experiment document.
//!
//! # Two surfaces, one measurement
//!
//! This adapter keeps its legacy positional commands (`produce`,
//! `produce-fixed`, `payload`, `schedule`, `topics-*`) exactly as they were,
//! because the migrated Node control plane still drives them and its evidence
//! has to stay comparable. The protocol verbs parse a `kafkars.experiment.v1`
//! document and map it onto the very same argument structs the legacy commands
//! build — the workload, the client configuration, and the phase shapes are
//! one thing, resolved in one place.
//!
//! What the protocol verbs no longer share is the *instrumentation*. `run`
//! drives `producer::v2`, whose admission clock spans queue-full retries and
//! whose evidence is bounded histograms rather than per-record arrays. That is
//! a change in what is measured, and applying it to the legacy verbs would
//! redefine measurements already sealed under their name. The two paths
//! therefore differ deliberately, and say so with two schema ids.
//!
//! The result documents deliberately differ. The legacy arm prints
//! `kafkars.producer-benchmark.v1` on stdout, exactly as it always has,
//! because a control plane's sealed evidence cannot be redefined after the
//! fact. This module writes `kafkars.producer-benchmark.v2`: the same phase
//! shapes, admitted through the same client configuration, but measured with
//! an admission clock that survives queue-full retries and stored in bounded
//! histograms rather than per-record arrays. Two schema ids is how that
//! difference is stated honestly — the file layout is unchanged, one JSON line
//! and the newline the legacy shell redirect added.
//!
//! # Declining is a first-class answer
//!
//! [`validate`] compares the experiment against what this adapter actually
//! configures. Everything in `producer.rs` above the argument surface is a
//! constant — the queue budget, the batching, the retry policy, the in-flight
//! ceiling — so an experiment asking for anything else would be run as a
//! different experiment. Saying so in a validate report is the difference
//! between a missing subject somebody can explain and a comparison that quietly
//! answers the wrong question.
//!
//! # Which subject am I?
//!
//! The control plane's output directory is `adapters/<subject>/`, so its last
//! component names the subject whose topics this process must use. When that
//! name is not a subject — `validate` is called without an output directory at
//! all — the fallback is the unique subject whose adapter is this one, and an
//! ambiguous experiment is refused rather than guessed at.
//!
//! # Layout
//!
//! - `describe` — the adapter's name, its schema ids, and what it claims.
//! - `verdict` — `validate`, the report behind it, and which subject this is.
//! - `settings` — the fixed client settings an experiment is judged against.
//! - `run` — the measured path and the status it writes on every exit.
//! - `translate` — a resolved experiment as the legacy argument structs.
//! - `documents` — the files this adapter reads and writes.
//! - `timestamp` — wall-clock strings for the status document.

mod describe;
mod documents;
mod run;
mod settings;
mod timestamp;
mod translate;
mod verdict;

pub(crate) use describe::{
    ADAPTER_NAME, LEGACY_CLOSED_LOOP_RESULT_SCHEMA, LEGACY_FIXED_RATE_RESULT_SCHEMA, describe,
};
pub(crate) use run::run;
pub(crate) use verdict::validate;

// The rest of the surface is reached only from `protocol_test`, which tests the
// pieces the three verbs are assembled from rather than only the verbs
// themselves. The gate keeps the facade honest: an item re-exported here
// without a caller would otherwise read as crate-wide surface that nothing
// uses.
#[cfg(test)]
pub(crate) use describe::{RESULT_SCHEMA, description};
#[cfg(test)]
pub(crate) use timestamp::utc_rfc3339_millis;
#[cfg(test)]
pub(crate) use translate::{fixed_arguments, produce_arguments};
#[cfg(test)]
pub(crate) use verdict::{report, subject_of};
