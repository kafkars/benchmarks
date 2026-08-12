//! `benchctl capacity`: the highest offered rate at which a subject still meets
//! its objectives, found by probing.
//!
//! # Every probe is an ordinary attempt
//!
//! A probe is a full `benchctl run` at one offered rate: fresh attempt id, fresh
//! topics, its own sealed bundle, its own classification. The search never
//! invents a measurement and never reuses one; the only thing it does that `run`
//! does not is decide which rate to ask for next. That means a capacity search
//! leaves behind exactly the evidence a reader would have collected by hand, and
//! [`CapacitySearch`](bench_schema::CapacitySearch) records the digest of every
//! bundle so the ladder can be re-walked.
//!
//! # What the search requires
//!
//! A scenario whose load mode resolves to `scheduled-open-loop-fixed-rate`,
//! carrying a `[search]` section and a non-empty `[slo]`. The load mode matters
//! because a closed-loop scenario has no offered rate to bisect on. The
//! objectives matter more: with none declared, every rate satisfies, and the
//! search would climb to its ceiling and report a capacity nobody measured.
//! Both are refused before any topic is created.
//!
//! # Exit codes
//!
//! `0` for a converged search, `20` for an unconverged one. An unconverged
//! search is a legitimate sealed outcome, not a crash: the probes are on disk and
//! the document says where the ladder stopped.
//!
//! # Layout
//!
//! - `command` — [`CapacityCommand`] and the whole search, end to end.
//! - `bounds` — [`SearchBounds`]: where the ladder starts, stops, and how
//!   finely it converges.
//! - `ladder` — bracket, bisect, confirm, and [`MAX_PROBES`].
//! - `evaluate` — the two gates one sealed probe has to pass.
//! - `rewrite` — [`with_offered_rate`], the scenario patch each probe seals.
//! - `report` — the search document and the small report beside it.

mod bounds;
mod command;
mod evaluate;
mod ladder;
mod report;
mod rewrite;
#[cfg(test)]
mod rewrite_test;

pub use self::bounds::{
    DEFAULT_CONFIRMATIONS, DEFAULT_GROWTH_FACTOR, DEFAULT_RESOLUTION_PERCENT, SearchBounds,
};
pub use self::command::{CapacityCommand, execute};
pub use self::ladder::MAX_PROBES;
pub use self::report::CAPACITY_REPORT_SUFFIX;
pub use self::rewrite::with_offered_rate;
