//! Command-line surface: hand-rolled argv parsing for every verb, the flag set
//! fixed by the design plan, and the dispatch into the pipeline, the suite, the
//! capacity search, and the reporting verbs.
//!
//! The seven verbs and what each one is for are stated in [`USAGE`] and
//! explained in `parse`; the flags that differ from the design plan, and why,
//! are documented there too.
//!
//! # Exit discipline
//!
//! Before an attempt workspace exists there is nowhere to seal, so failures
//! exit with the error's own code: 64 for a malformed command line, 65 for
//! unreadable or incoherent inputs, 73 for an attempt directory already
//! claimed. Once the workspace exists, *every* ending goes through the sealer:
//! a control-plane error seals a partial bundle and exits 20, a panic is caught
//! and seals a crashed bundle and exits 70, and a sealed attempt exits with the
//! code for its execution status. Only a failure to seal escapes as 74.
//!
//! The looping verbs keep the same two ranges. A suite exits 0 only when every
//! attempt sealed `complete` and at least two classified valid, and 20
//! otherwise; a capacity search exits 0 when it converged and 20 when it did
//! not; a pack exits 0 only when every entry exited 0. An unconverged search is
//! a sealed outcome, not a fault.
//!
//! # Layout
//!
//! - `usage` — [`USAGE`] and [`DEFAULT_RESULTS_ROOT`].
//! - `command` — [`Command`] and the two command structs this module owns.
//! - `parse` — [`parse`], one function per verb.
//! - `flags` — splitting argv into named values, and reading them back.
//! - `dispatch` — [`run`]: verb to implementation, and the exit code.

mod command;
mod dispatch;
mod flags;
mod parse;
mod usage;

pub use self::command::{Command, ResolveCommand, RunCommand};
pub use self::dispatch::run;
pub use self::parse::parse;
pub use self::usage::{DEFAULT_RESULTS_ROOT, USAGE};
