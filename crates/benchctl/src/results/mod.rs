//! Reading subject evidence after execution, and turning it into the two
//! verdict documents a sealed bundle carries.
//!
//! This module answers the second of the two questions a sealed bundle must
//! answer. `status.json` says whether the machinery did what it was told;
//! `classification.json` says whether the numbers mean anything, and that is
//! decided here. The two are kept apart deliberately: a run can complete every
//! phase and be invalid, and a partial run can still hold one subject's perfectly
//! good evidence.
//!
//! Nothing here rewrites what an adapter wrote. A result document is captured as
//! bytes and sealed as those exact bytes; the parse extracts the facts the gate
//! needs and the sealed file keeps every byte the adapter chose. Re-serializing
//! would round-trip the adapter's floating-point numbers through a second
//! formatter, which is a way of altering evidence while believing you are reading
//! it.
//!
//! # Layout
//!
//! - `evidence` — [`SubjectEvidence`] and [`read_subject`]: what is on disk.
//! - `outcome` — [`SubjectOutcome`] and [`VerificationVerdict`]: everything the
//!   verdict needs to know about one subject.
//! - `reasons` — why a subject's evidence may not be believed.
//! - `verdict` — [`classify`] and [`compare`], the two sealed documents.

mod evidence;
#[cfg(test)]
mod evidence_test;
mod outcome;
mod reasons;
#[cfg(test)]
mod reasons_test;
mod verdict;
#[cfg(test)]
mod verdict_test;

pub use self::evidence::{HEADLINE_QUANTILE, SubjectEvidence, read_subject};
pub use self::outcome::{SubjectOutcome, VerificationVerdict};
pub use self::reasons::max_failed_records;
pub use self::verdict::{DEFERRED_CHECKS, classify, compare};
