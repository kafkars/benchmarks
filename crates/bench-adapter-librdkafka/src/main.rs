//! Adapter-protocol shim in front of the unmodified librdkafka C benchmark
//! binary.
//!
//! # Why a shim instead of a change to the C program
//!
//! The reference client is the thing being compared against, so the reference
//! adapter has to stay boring. Zero lines of C change here: the C program keeps
//! its existing positional argument surface and its existing stdout document,
//! and this shim is the only thing that learns the adapter protocol. That keeps
//! the comparison honest — nothing about the reference's measured path was
//! rewritten to suit this harness — and it keeps the C build reproducible
//! against the pinned librdkafka anchor.
//!
//! # Argument surface
//!
//! ```text
//! bench-adapter-librdkafka --binary <path> describe --json
//! bench-adapter-librdkafka --binary <path> validate --experiment <resolved.json>
//! bench-adapter-librdkafka --binary <path> run --experiment <resolved.json> --output <dir>
//! ```
//!
//! `--binary` comes first because it is configuration, not a verb argument: the
//! subject list names the shim *and* the C binary it drives, and the control
//! plane appends the verb and its flags to that prefix unchanged.
//!
//! # Which subject am I?
//!
//! A resolved experiment describes every subject, including topics per subject,
//! so a subject that does not know its own name cannot pick its topics. The
//! control plane's output directory is `adapters/<subject>/`, so the directory's
//! last component is the answer; when it is not a subject name — a `validate`
//! call has no output directory at all — the shim falls back to the unique
//! subject whose `adapter_name` is [`describe::ADAPTER_NAME`], and refuses when
//! that is ambiguous rather than guessing.
//!
//! # Layout
//!
//! - `arguments` — the argv surface and its parse table.
//! - `describe` — the static capability document.
//! - `validate` — what the C argument surface can and cannot express.
//! - `translate` — resolved experiment → the exact legacy positional argv.
//! - `run` — spawning the child, capturing its result, sealing its status.
//! - `time` — UTC timestamps for the status document.
#![forbid(unsafe_code)]

mod arguments;
mod describe;
mod run;
mod time;
mod translate;
mod validate;

#[cfg(test)]
mod arguments_test;
#[cfg(test)]
mod describe_test;
#[cfg(test)]
mod fixture;
#[cfg(test)]
mod run_test;
#[cfg(test)]
mod time_test;
#[cfg(test)]
mod translate_test;
#[cfg(test)]
mod validate_test;

fn main() {
    std::process::exit(arguments::run(&std::env::args().collect::<Vec<_>>()));
}
