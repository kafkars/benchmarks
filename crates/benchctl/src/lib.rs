//! The control plane: resolves experiments, probes and supervises adapter
//! processes, verifies results against the broker, and always seals an
//! evidence bundle — for crashes and timeouts as much as for successes.
//!
//! # Shape
//!
//! Everything here is std-only orchestration: `std::process` children, files,
//! monotonic deadlines, and the [`bench_schema`] vocabulary. No async runtime,
//! by repository contract. The binary in `main.rs` is a thin shell over the
//! `cli` module; integration tests drive the same library seams the binary
//! uses.
//!
//! # Layout
//!
//! - `error` — [`CtlError`] and the exit-code table.
//! - `time` — hand-rolled UTC formatting for names and document fields.
//! - `attempt` — [`AttemptId`] and [`AttemptPaths`], the bundle layout.
//! - `cli` — argv parsing and the verb dispatch (resolver workstream).
//! - `resolve` — source TOML → canonical resolved experiment (resolver).
//! - `probe` — adapter `describe`/`validate` spawning (resolver).
//! - `environment` — machine capture, `benchmark-environment.v2` (resolver).
//! - `supervise` — child lifecycle, deadlines, exit capture (supervisor).
//! - `topics` — configured topic tool lifecycle (supervisor).
//! - `verify` — configured broker-visible verifier runs (supervisor).
//! - `results` — reading subject evidence for classification (supervisor).
//! - `checksum` — deterministic bundle checksums (supervisor).
//! - `seal` — the single always-seal funnel (supervisor).
//! - `interrupt` — SIGINT/SIGTERM latch (supervisor).
#![forbid(unsafe_code)]

mod attempt;
mod checksum;
mod cli;
mod environment;
mod error;
mod interrupt;
mod probe;
mod resolve;
mod results;
mod seal;
mod supervise;
mod time;
mod topics;
mod verify;

pub use attempt::{AttemptId, AttemptPaths, PENDING_DIR};
pub use error::{
    CtlError, CtlErrorKind, CtlResult, EXIT_ATTEMPT_EXISTS, EXIT_INTERNAL, EXIT_INVALID,
    EXIT_SEAL_WRITE, EXIT_SEALED_COMPLETE, EXIT_SEALED_CRASHED, EXIT_SEALED_PARTIAL,
    EXIT_SEALED_TIMED_OUT, EXIT_USAGE, exit_code_for_status,
};
pub use time::{utc_compact_seconds, utc_rfc3339_millis};

#[cfg(test)]
mod attempt_test;
#[cfg(test)]
mod error_test;
#[cfg(test)]
mod time_test;
