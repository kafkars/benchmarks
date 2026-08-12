//! The dispatch rule: which verb one entry runs.
//!
//! Each entry names a scenario and how many attempts of it the pack asks for.
//! Three cases, decided by [`EntryPlan::decide`] and nothing else:
//!
//! ```text
//! repetitions >= 2                      -> benchctl suite    --repetitions N
//! repetitions == 1 and [search] present -> benchctl capacity
//! repetitions == 1                      -> benchctl run
//! ```
//!
//! The middle case is the only one that reads the scenario, and it reads it for
//! one fact: a `[search]` section is what distinguishes "run this once" from
//! "walk a rate ladder". A capacity search is already repeated internally —
//! `repetitions_per_rate` confirmations at the surviving rate — so asking a
//! pack to repeat one would be asking for a suite over documents that are not
//! attempts of the same experiment. That is why a search entry states one
//! repetition and gets a ladder rather than a loop.

use std::path::Path;

use bench_schema::SourceExperiment;

use crate::error::CtlResult;
use crate::pipeline;
use crate::suite::MINIMUM_REPETITIONS;

/// Which verb one entry dispatches to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryPlan {
    /// One attempt: `benchctl run`.
    Run,
    /// N attempts of one experiment: `benchctl suite`.
    Suite,
    /// A rate ladder: `benchctl capacity`.
    Capacity,
}

impl EntryPlan {
    /// Decides what an entry runs, from its repetition count and whether its
    /// scenario carries a `[search]` section.
    ///
    /// A pure function of two facts, so the plan for a whole pack can be read
    /// off before a single attempt runs.
    #[must_use]
    pub const fn decide(repetitions: u32, has_search: bool) -> Self {
        if repetitions >= MINIMUM_REPETITIONS {
            Self::Suite
        } else if has_search {
            Self::Capacity
        } else {
            Self::Run
        }
    }

    /// The verb name this plan prints as.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Suite => "suite",
            Self::Capacity => "capacity",
        }
    }
}

/// Reads the scenario far enough to decide which verb the entry dispatches to.
///
/// # Errors
///
/// Returns an invalid-experiment error when the scenario cannot be read or does
/// not parse. Only a `repetitions == 1` entry needs the file at all, but it is
/// read for every entry: a suite would fail on the same unreadable scenario a
/// moment later, and failing here means the pack says which entry was broken
/// before it creates a workspace for it.
pub(super) fn plan_for(scenario: &str, repetitions: u32) -> CtlResult<EntryPlan> {
    let source = SourceExperiment::from_toml_str(&pipeline::read(Path::new(scenario))?)?;
    Ok(EntryPlan::decide(repetitions, source.search.is_some()))
}
