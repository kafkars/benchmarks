//! Child-process supervision without an async runtime: spawn with stdio
//! redirected to files, poll `try_wait` against monotonic deadlines, kill and
//! reap on expiry or interrupt, and capture exit codes and signals exactly.
//!
//! Stub: implemented by the supervisor workstream (agent C).
