//! Interrupt handling: SIGINT/SIGTERM latched into an atomic flag via
//! signal-hook so a run can stop between polls and still seal partial
//! evidence.
//!
//! Stub: implemented by the supervisor workstream (agent C).
