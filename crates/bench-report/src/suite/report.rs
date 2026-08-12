//! How a suite is to be summarized, and what comes back beside the sealed
//! document.

use bench_schema::SuiteSummary;

use crate::bootstrap::DEFAULT_BOOTSTRAP_RESAMPLES;
use crate::economics::RequestEconomics;

/// The default practical threshold: a five percent relative difference.
///
/// Chosen to match
/// [`COEFFICIENT_OF_VARIATION_BUDGET`](crate::COEFFICIENT_OF_VARIATION_BUDGET),
/// because a difference smaller than the run-to-run noise of the machine is not
/// a difference this harness can speak about.
pub const DEFAULT_PRACTICAL_THRESHOLD: f64 = 0.05;

/// How a suite is to be summarized.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SuiteOptions {
    /// Seed behind every deterministic choice, including the resampling.
    pub seed: u64,
    /// Bootstrap resamples behind every interval.
    pub resamples: u32,
    /// The relative difference the suite was set up to care about.
    pub practical_threshold: f64,
}

impl Default for SuiteOptions {
    fn default() -> Self {
        Self {
            seed: 0x6B61_666B,
            resamples: DEFAULT_BOOTSTRAP_RESAMPLES,
            practical_threshold: DEFAULT_PRACTICAL_THRESHOLD,
        }
    }
}

/// One subject's native request economics, totalled over the valid attempts.
///
/// Separate from [`SuiteSummary`] because `kafkars.suite-summary.v1` has no
/// field for it: the schema carries latency, goodput, and resources, and
/// request economics are only available for subjects whose client emits native
/// statistics. Carrying them beside the summary keeps the sealed document
/// honest about what it contains, and keeps the asymmetry visible instead of
/// hiding it in a half-empty column.
#[derive(Debug, Clone, PartialEq)]
pub struct SubjectEconomics {
    /// Subject name, matching the resolved experiment.
    pub subject: String,
    /// The subject's role in this comparison, when it declared one.
    pub role: Option<String>,
    /// Valid attempts that contributed a native statistics stream.
    pub attempts_reported: usize,
    /// The totals, and the normalizations derived from them.
    pub totals: RequestEconomics,
}

/// A suite summary together with everything the schema has no field for.
///
/// [`summarize_suite`](super::summarize_suite) returns the sealed document
/// alone; this is the same analysis with the request economics still attached,
/// for the renderers and the analysis packet.
#[derive(Debug, Clone, PartialEq)]
pub struct SuiteReport {
    /// The sealed suite summary.
    pub summary: SuiteSummary,
    /// Per-subject request economics, in subject declaration order. Subjects
    /// whose client emits no native statistics are absent from this list
    /// entirely rather than present with zeroes.
    pub economics: Vec<SubjectEconomics>,
}
