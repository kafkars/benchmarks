//! `kafkars.capacity-search.v1`: the rate ladder a capacity search walked, and
//! where it stopped.
//!
//! Capacity is the highest offered rate at which a subject still meets its
//! objectives. That number is only meaningful next to the objectives it was
//! measured against and the probes that were actually run, so this document
//! carries all three: the [`SloSpec`] the search judged by, every probe in the
//! order it was offered, and the bracket the search narrowed to.
//!
//! # Why every probe stays in the document
//!
//! A search that reported only its answer would be asking a reader to trust the
//! search. Keeping the failed probes — each with the reasons it failed and the
//! digest of the bundle it came from — means a reader can re-derive the verdict,
//! and can see the shape of the failure: a subject that misses by one percent
//! over three rates is a different story from one that collapses.
//!
//! Confirmation is separate from the search. The probes in `confirmation` are
//! repetitions at the rate the search settled on, run after the bracket closed,
//! and they are what turns "the search stopped here" into "this rate holds".
//!
//! # Invariants ([`CapacitySearch::validate`])
//!
//! `subject` is named; `resolution` is greater than zero, because a search that
//! narrows to nothing never terminates; a bracket, when both ends are present,
//! has `bracket_low <= bracket_high`; and `confirmed_rate` is present exactly
//! when the status is [`CapacityStatus::Converged`]. An unconverged or invalid
//! search that carried a rate would be stating a capacity nobody established.

use serde::{Deserialize, Serialize};

use crate::error::{SchemaError, SchemaResult};
use crate::experiment::SloSpec;
use crate::schema_id::{CAPACITY_SEARCH_V1, require_schema};

/// How a capacity search ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapacityStatus {
    /// The bracket closed to within the resolution and the rate was confirmed.
    Converged,
    /// The search ran out of bounds, budget, or probes before closing.
    Unconverged,
    /// The probes cannot support any conclusion — for example every one of them
    /// was an invalid run.
    Invalid,
}

impl CapacityStatus {
    /// Returns the wire string for this status.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Converged => "converged",
            Self::Unconverged => "unconverged",
            Self::Invalid => "invalid",
        }
    }
}

/// One offered rate, and whether the subject met its objectives at that rate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityProbe {
    /// The rate this probe offered, in records per second.
    pub offered_records_per_second: u64,
    /// Attempt id of the run that offered it.
    pub attempt_id: String,
    /// Digest of the sealed bundle that run produced.
    pub bundle_digest: String,
    /// Whether every objective held at this rate.
    pub satisfied: bool,
    /// One reason per objective that did not hold. Empty when satisfied.
    pub reasons: Vec<String>,
}

/// `kafkars.capacity-search.v1`: the ladder, the bracket, and the answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacitySearch {
    /// Schema id, always [`CapacitySearch::SCHEMA`].
    pub schema: String,
    /// The subject whose capacity was searched for.
    pub subject: String,
    /// The objectives every probe was judged against.
    pub slo: SloSpec,
    /// Every probe the search offered, in the order it offered them.
    pub probes: Vec<CapacityProbe>,
    /// Highest rate known to satisfy the objectives, when one is known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bracket_low: Option<u64>,
    /// Lowest rate known to violate them, when one is known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bracket_high: Option<u64>,
    /// Bracket width, in records per second, the search narrows to.
    pub resolution: u64,
    /// The confirmed capacity; present exactly when the search converged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirmed_rate: Option<u64>,
    /// Repetitions at `confirmed_rate`, run after the bracket closed.
    pub confirmation: Vec<CapacityProbe>,
    /// How the search ended.
    pub status: CapacityStatus,
}

impl CapacitySearch {
    /// Schema id this document declares.
    pub const SCHEMA: &'static str = CAPACITY_SEARCH_V1;

    /// Reports whether the document declares the expected schema id.
    pub fn has_expected_schema(&self) -> bool {
        self.schema == Self::SCHEMA
    }

    /// Parses and validates a capacity search from its bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the bytes are not JSON, the schema id is wrong, or
    /// an invariant documented on this type fails.
    pub fn from_slice(bytes: &[u8]) -> SchemaResult<Self> {
        let document: Self = serde_json::from_slice(bytes)
            .map_err(|error| SchemaError::parse(format!("capacity-search.v1: {error}")))?;
        document.validate()?;
        Ok(document)
    }

    /// Checks the invariants documented on this type.
    ///
    /// # Errors
    ///
    /// Returns an error naming the first violated invariant.
    pub fn validate(&self) -> SchemaResult<()> {
        require_schema(&self.schema, Self::SCHEMA)?;
        if self.subject.trim().is_empty() {
            return Err(SchemaError::invalid_field(
                "subject",
                "a capacity is always a capacity of something",
            ));
        }
        if self.resolution == 0 {
            return Err(SchemaError::invalid_field(
                "resolution",
                "a bracket that narrows to zero never closes",
            ));
        }
        if let (Some(low), Some(high)) = (self.bracket_low, self.bracket_high)
            && low > high
        {
            return Err(SchemaError::invalid_field(
                "bracket_low",
                &format!("{low} is above bracket_high {high}"),
            ));
        }
        match (self.status, self.confirmed_rate) {
            (CapacityStatus::Converged, None) => Err(SchemaError::invalid_field(
                "confirmed_rate",
                "a converged search must state the rate it converged on",
            )),
            (CapacityStatus::Unconverged | CapacityStatus::Invalid, Some(rate)) => {
                Err(SchemaError::invalid_field(
                    "confirmed_rate",
                    &format!(
                        "a {} search must not claim the confirmed rate {rate}",
                        self.status.as_str()
                    ),
                ))
            }
            _ => Ok(()),
        }
    }
}
