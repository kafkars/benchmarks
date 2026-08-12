//! `kafkars.suite-summary.v1`: what a set of repetitions of one experiment says
//! once every attempt is in.
//!
//! A single attempt is an anecdote. This document is the first place in the
//! evidence chain where a number is allowed to be called a result, and the shape
//! of it is deliberate: the per-attempt observations stay in the document
//! alongside the medians derived from them, so a reader can recompute every
//! summary statistic from the same bytes that state it. A summary a reader
//! cannot audit is a summary a reader has to trust.
//!
//! # What is summarized, and how
//!
//! - **Medians, not means.** One stalled attempt should not move the reported
//!   number, and a median of an odd repetition count is a value that actually
//!   occurred.
//! - **Ratios of medians, with an interval.** A paired ratio names its numerator
//!   and its denominator, because a ratio whose denominator is implied is
//!   decoration. `resamples` records how many bootstrap resamples produced
//!   `ci_low` and `ci_high`, and `seed` records what made that resampling
//!   reproducible.
//! - **Dispersion beside every central value.** A coefficient of variation is
//!   absent rather than zero when the underlying values could not produce one,
//!   because "we did not measure spread" and "there was no spread" are opposite
//!   claims.
//! - **Gates that name themselves.** Each [`GateOutcome`] carries the sentence it
//!   is enforcing, so a failing gate is readable without the code that ran it.
//! - **Attribution beside comparison.** The declared execution surface, the
//!   scheduler lateness, and the client-internal portion of the latency travel
//!   with every observation. They say *where* a difference is; only the
//!   offer-to-terminal percentiles and the goodput may say *that* there is one.
//!
//! `practical_threshold` is the difference the suite was set up to care about,
//! recorded next to the intervals so that "the interval excludes zero" is never
//! confused with "the difference matters".
//!
//! # Invariants ([`SuiteSummary::validate`])
//!
//! `claim_eligible` is false, as it is for every document this milestone can
//! produce; every declared subject role is one the vocabulary knows;
//! `ci_low <= ci_high` for every pair; and `practical_threshold` is a finite,
//! non-negative fraction. Everything else is measurement, and this crate does
//! not second-guess measurement.
//!
//! Floating-point numbers are welcome here. A suite summary is evidence, never
//! identity: it is not hashed into an experiment id, and
//! [`canonical_bytes`](crate::canonical_bytes) refuses it if anyone tries.
//!
//! # Layout
//!
//! - `observation` — one subject's numbers, per attempt and as a median.
//! - `summary` — the document, its ratios, its gates, and its invariants.

mod observation;
mod summary;

pub use observation::{SubjectMedians, SuiteAttempt, SuiteSubjectObservation};
pub use summary::{GateOutcome, PairedRatio, SubjectDispersion, SuiteSummary};
