//! Always-seal: every terminal path of an attempt — success, subject failure,
//! tool failure, timeout, interrupt, panic — converges on exactly one sealing
//! pass that writes status, classification, comparison, execution order,
//! checksums, and the bundle manifest, in that order.
//!
//! # Three layers, because one is not enough
//!
//! 1. **One funnel.** Nothing in [`run_attempt`] returns early on failure.
//!    Every phase records what happened and the attempt continues to the single
//!    `seal` call at the end. A subject that failed is evidence about that
//!    subject, not a reason to discard the evidence about the other one.
//! 2. **A caught panic.** The whole supervision body runs inside
//!    [`catch_unwind`](std::panic::catch_unwind), so a bug in the control plane
//!    becomes a `crashed` bundle carrying everything recorded up to the panic,
//!    rather than a stack trace and an empty directory.
//! 3. **A drop guard.** A last-resort guard is armed on entry and disarmed only
//!    by a completed seal. If the process unwinds past the funnel for any
//!    reason, its `Drop` writes a minimal `status.json` and a checksum manifest,
//!    best effort, errors swallowed.
//!
//! # What ends an attempt, and how it is named
//!
//! Execution status takes the worst of every phase: `timed_out` > `crashed` >
//! `partial` > `complete`. A deadline expiry is a timeout; a child dying on a
//! signal nobody sent it, or a panic in this crate, is a crash; a non-zero
//! adapter exit, a verifier that could not run, an interrupt, or a phase that
//! was skipped is partial.
//!
//! Phase records answer a different question from execution status. A verifier
//! that ran cleanly and found missing records is a *failed phase* — the check
//! did not pass — but not a broken machine, so the execution status stays
//! `complete` and the verdict lands in `classification.json`. Confusing the two
//! is how "the run finished" turns into "the numbers are good".
//!
//! # Layout
//!
//! - `request` — [`AttemptRequest`], and the two facts every path reads off it.
//! - `driver` — [`run_attempt`]: the funnel, and the panic boundary around the
//!   verdict derivation.
//! - `phases` — sealed inputs, the topic lifecycle, and the loop over subjects.
//! - `subject` — running one subject and recording how its process ended.
//! - `verification` — the configured verifier, per subject and phase.
//! - `state` — what the attempt has learned, and the plan it turns into.
//! - `write` — the single funnel that writes a plan to disk, in order.
//! - `failure` — [`seal_failure`], for the phases that happen before an attempt.
//! - `guard` — the last-resort seal.

mod driver;
#[cfg(test)]
mod driver_test;
mod failure;
mod guard;
mod phases;
mod request;
mod state;
mod subject;
mod verification;
mod write;

pub use self::driver::run_attempt;
pub use self::failure::seal_failure;
pub use self::request::AttemptRequest;
