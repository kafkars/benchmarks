//! The search bounds, with every degenerate value replaced by a documented
//! default.

use bench_schema::{SourceExperiment, SourceSearch};

use crate::error::{CtlError, CtlResult};

/// Growth factor used when the scenario states one below two.
///
/// A factor of one never leaves the initial rate, and a factor of zero is not a
/// search at all; doubling is what the migrated legacy scenarios use.
pub const DEFAULT_GROWTH_FACTOR: u64 = 2;

/// Bracket width, as a percentage of the bracket's upper end, used when the
/// scenario states zero.
pub const DEFAULT_RESOLUTION_PERCENT: u64 = 5;

/// Confirmation repetitions used when the scenario states none.
///
/// One: the search has already probed this rate once during the bisection, and a
/// scenario that wants more says so in `repetitions_per_rate`.
pub const DEFAULT_CONFIRMATIONS: u32 = 1;

/// The search bounds, with every degenerate value replaced by a documented
/// default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchBounds {
    /// First rate the search offers.
    pub initial: u64,
    /// Lowest rate the search will consider.
    pub minimum: u64,
    /// Highest rate the search will consider.
    pub maximum: u64,
    /// Multiplier applied while the search is still bracketing.
    pub growth_factor: u64,
    /// Bracket width, as a percentage of the bracket's upper end.
    pub resolution_percent: u64,
    /// Repetitions at the surviving rate, after the bracket closes.
    pub confirmations: u32,
}

impl SearchBounds {
    /// Reads the bounds a scenario declares, substituting defaults.
    ///
    /// # Errors
    ///
    /// Returns an invalid-experiment error when the bounds cannot describe a
    /// search: no `[search]` section, or a minimum above the maximum.
    pub fn from_source(source: &SourceExperiment) -> CtlResult<Self> {
        let search: &SourceSearch = source.search.as_ref().ok_or_else(|| {
            CtlError::invalid(
                "a capacity search needs a [search] section stating where to start and stop",
            )
        })?;
        if search.minimum_records_per_second > search.maximum_records_per_second {
            return Err(CtlError::invalid(format!(
                "the search floor {} is above its ceiling {}",
                search.minimum_records_per_second, search.maximum_records_per_second
            )));
        }
        let bounds = Self {
            initial: search.initial_records_per_second.clamp(
                search.minimum_records_per_second.max(1),
                search.maximum_records_per_second.max(1),
            ),
            minimum: search.minimum_records_per_second.max(1),
            maximum: search.maximum_records_per_second.max(1),
            growth_factor: if search.growth_factor < 2 {
                DEFAULT_GROWTH_FACTOR
            } else {
                search.growth_factor
            },
            resolution_percent: if search.resolution_percent == 0 {
                DEFAULT_RESOLUTION_PERCENT
            } else {
                search.resolution_percent
            },
            confirmations: source.repetitions_per_rate.unwrap_or(DEFAULT_CONFIRMATIONS),
        };
        Ok(bounds)
    }

    /// The absolute bracket width the search narrows to, given the upper end it
    /// found.
    ///
    /// The document carries one number, so the percentage is resolved against
    /// the bracket's upper end the moment one exists, and against the initial
    /// rate when the search never bracketed. Never zero: a bracket that narrows
    /// to nothing never closes.
    #[must_use]
    pub fn resolution(&self, upper: Option<u64>) -> u64 {
        let scale = upper.unwrap_or(self.initial);
        (scale.saturating_mul(self.resolution_percent) / 100).max(1)
    }
}
