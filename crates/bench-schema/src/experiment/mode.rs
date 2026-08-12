//! The three closed vocabularies an experiment chooses from: what is measured,
//! how load is offered, and where scheduled arrivals come from.
//!
//! Every wire string here is pinned by a test, because these are the words a
//! reader of a sealed bundle uses to say what the run was.

use serde::{Deserialize, Serialize};

/// What kind of client behavior an experiment measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExperimentKind {
    /// Records produced to a topic and acknowledged by the cluster.
    Producer,
}

/// How load is offered to the client under test.
///
/// The wire strings are pinned by tests. `closed-loop` means the application
/// offers the next record as soon as the previous one is admitted, so the
/// client's own capacity sets the rate; `scheduled-open-loop-fixed-rate` means
/// records have intended arrival times computed from a rate, so a slow client
/// falls behind its schedule instead of slowing the offer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LoadMode {
    /// Offer rate is whatever the client can absorb.
    ClosedLoop,
    /// Offer times come from a fixed-rate schedule the client cannot slow.
    ScheduledOpenLoopFixedRate,
}

impl LoadMode {
    /// Returns the wire string for this mode.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ClosedLoop => "closed-loop",
            Self::ScheduledOpenLoopFixedRate => "scheduled-open-loop-fixed-rate",
        }
    }
}

/// The arrival process a scheduled load mode draws its intended offsets from.
///
/// Only [`ArrivalModel::Deterministic`] has an implementation in this
/// milestone; an adapter that is handed anything else declines it in its
/// validate report rather than silently substituting a schedule it does
/// implement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ArrivalModel {
    /// Evenly spaced arrivals, one every `1 / rate` seconds.
    Deterministic,
    /// Exponentially distributed inter-arrival times with the given mean rate.
    Poisson,
}
