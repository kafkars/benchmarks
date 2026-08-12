//! The control plane: it resolves an experiment, spawns adapter processes,
//! supervises them against deadlines, and seals an evidence bundle whatever
//! happens.
//!
//! **This crate is a stub.** It carries the contract below so that the layout,
//! the workspace graph, and the lint gate are real from the first commit; the
//! implementation lands in the following waves.
//!
//! # Why the control plane spawns processes
//!
//! Every subject under measurement is a separate operating-system process
//! started by this crate, never a library linked into it. That is the whole
//! design. Two Kafka clients cannot share an address space without sharing
//! allocator behavior, thread scheduling, and page cache pressure, and a
//! harness that links its subjects is measuring the harness. Process
//! boundaries also mean the control plane can measure a subject written in a
//! language it does not host, and can survive a subject that crashes.
//!
//! The control plane owns everything a subject must not: identity, topic
//! naming, execution order, deadlines, verification, and sealing. An adapter
//! never decides whether its own run was valid.
//!
//! # Always seal
//!
//! The invariant that shapes this crate is that an attempt which started must
//! leave a sealed bundle behind. A crash, a timeout, an interrupt, or a bug in
//! this crate produces a bundle recording exactly that, because the runs worth
//! studying most are the ones that went wrong. Sealing is therefore layered:
//! every phase result funnels into exactly one seal call, a panic is caught and
//! sealed as a crash, and a last-resort drop guard makes a best-effort seal if
//! both of those are somehow bypassed.
//!
//! Execution status and validity are different questions on different axes. A
//! run can complete cleanly and still be invalid; a partial run can still carry
//! useful evidence. Worst status wins: timed out, then crashed, then partial,
//! then complete.
//!
//! # What this crate will own
//!
//! - `cli` — argument parsing by hand, `benchctl resolve` and `benchctl run`.
//!   There is no argument-parsing dependency; the surface is small enough to
//!   own and the parse table is unit-tested.
//! - `error` — the exit-code table: `0` sealed complete, `20` partial, `21`
//!   crashed, `22` timed out, `64` usage, `65` invalid before the attempt, `70`
//!   panic, `73` attempt directory already exists, `74` seal write failure.
//! - `resolve` — the pure resolution function, separated from any spawning so
//!   that resolution is golden-testable, plus topic-name derivation.
//! - `probe` — spawning `describe` and `validate` with bounded stdout capture
//!   and a probe timeout, because a misbehaving adapter must not be able to
//!   exhaust memory before it has run anything.
//! - `attempt` — attempt identifiers and the bundle path layout. An attempt
//!   materializes under a pending directory first and is renamed once its
//!   experiment identity exists, so a failure before resolution still seals.
//! - `time` — hand-rolled UTC formatting, vector-tested. Wall-clock time names
//!   things; deadlines use a monotonic instant.
//! - `supervise` — spawning with output redirected to files, polling for exit
//!   against a deadline and an interrupt flag, and, on expiry, killing and then
//!   blocking to reap the child before any output file is touched.
//! - `topics` and `verify` — invoking configured external tools by argument
//!   vector. The tools come from configuration; no adapter name is ever
//!   hard-coded into a policy decision.
//! - `environment` — capturing the host, toolchain, and repository state, with
//!   explicit unavailability rather than invented values.
//! - `results` — reading adapter status and result documents through lenient
//!   views and gating on them.
//! - `checksum` — a deterministic recursive walk and streaming digest.
//! - `seal` — the single sealing path described above.
//! - `interrupt` — signal handling into an atomic flag.
//! - `src/bin/fake-adapter.rs` — a test fixture speaking the full adapter
//!   protocol, so the whole pipeline including every failure mode is tested
//!   offline with no broker in sight.
#![forbid(unsafe_code)]
