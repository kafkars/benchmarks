//! The scenario's load-mode vocabulary, and the only place it is mapped onto
//! the resolved one.

use serde::{Deserialize, Serialize};

use crate::error::{SchemaError, SchemaResult};
use crate::experiment::LoadMode;

/// How a scenario says load is offered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceLoadMode {
    /// One closed-loop capacity point: the client sets its own rate.
    #[serde(rename = "closed-loop-capacity-point")]
    ClosedLoopCapacityPoint,
    /// A scheduled open-loop run at one fixed offered rate.
    #[serde(rename = "scheduled-open-loop-fixed-rate")]
    ScheduledOpenLoopFixedRate,
    /// A search for the highest rate that still meets the objectives.
    #[serde(rename = "scheduled-open-loop-capacity-search")]
    ScheduledOpenLoopCapacitySearch,
}

impl SourceLoadMode {
    /// Returns the wire string a scenario writes.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ClosedLoopCapacityPoint => "closed-loop-capacity-point",
            Self::ScheduledOpenLoopFixedRate => "scheduled-open-loop-fixed-rate",
            Self::ScheduledOpenLoopCapacitySearch => "scheduled-open-loop-capacity-search",
        }
    }

    /// Maps a scenario load mode onto the resolved vocabulary.
    ///
    /// Capacity search is refused rather than approximated: running one fixed
    /// rate and calling it a capacity search would produce evidence that claims
    /// something nobody measured.
    pub fn resolved(self) -> SchemaResult<LoadMode> {
        match self {
            Self::ClosedLoopCapacityPoint => Ok(LoadMode::ClosedLoop),
            Self::ScheduledOpenLoopFixedRate => Ok(LoadMode::ScheduledOpenLoopFixedRate),
            Self::ScheduledOpenLoopCapacitySearch => Err(SchemaError::invalid_field(
                "load_mode",
                "capacity search is not implemented in this milestone",
            )),
        }
    }
}
