//! The deterministic analysis packet: everything an interpreter is allowed to
//! reason from, numbered so that it can be cited instead of restated.
//!
//! # Why numbering matters
//!
//! Prose about measurements drifts. A sentence that says "roughly eighteen
//! percent faster" survives a re-run that moved the number, and nobody notices.
//! A sentence that says "M016" cannot: the value lives in one place, the
//! citation resolves or it does not, and
//! [`validate_llm_summary`] rejects a summary that cites a metric this packet
//! never defined.
//!
//! # Layout
//!
//! - `metrics` — the metric table, and the id layout every citation depends on.
//! - `findings` — the statements the deterministic layer makes, and the verdict
//!   rule nothing downstream may disagree with.
//! - `assemble` — the pass that puts one packet together.
//! - `validate` — the one check a model-written summary has to pass.

mod assemble;
mod findings;
mod metrics;
mod validate;

pub use assemble::build_packet;
pub use validate::validate_llm_summary;
