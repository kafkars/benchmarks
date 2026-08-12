//! Always-seal: every terminal path of an attempt — success, subject failure,
//! tool failure, timeout, interrupt, panic — converges on exactly one sealing
//! pass that writes status, classification, comparison, execution order,
//! checksums, and the bundle manifest, in that order.
//!
//! Stub: implemented by the supervisor workstream (agent C).
