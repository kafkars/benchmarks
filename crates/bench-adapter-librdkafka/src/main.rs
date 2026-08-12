//! Adapter-protocol shim in front of the unmodified librdkafka C benchmark
//! binary.
//!
//! **This binary is a stub.** It carries the contract below so that the
//! workspace graph and the lint gate are real from the first commit; the
//! translation lands in a following wave. Every invocation is reported as a
//! usage error with exit code `64`.
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
//! # What this binary will own
//!
//! - A `--binary <path>` prefix naming the C executable to drive, followed by
//!   one of the three protocol verbs.
//! - `describe --json` — a static capability document for the pinned
//!   librdkafka version. The version actually linked at run time is reported
//!   separately inside the result document rather than being trusted here.
//! - `validate --experiment <resolved.json>` — checking a resolved experiment
//!   against what the C argument surface can express, and reporting the reasons
//!   when it cannot.
//! - `run --experiment <resolved.json> --output <dir>` — translating the
//!   resolved experiment into the exact legacy positional argument vector,
//!   capturing the child's stdout as `result.json`, and writing `status.json`
//!   from the child's exit, including when the child fails.
//!
//! Validity is not decided here. The verifier and the topic tool stay outside
//! the adapter protocol as separately configured tools, because an adapter must
//! never be in a position to vouch for its own run.
#![forbid(unsafe_code)]

fn main() {
    eprintln!(
        "usage: bench-adapter-librdkafka --binary <path> <describe|validate|run> [options] (not implemented yet)"
    );
    std::process::exit(64);
}
