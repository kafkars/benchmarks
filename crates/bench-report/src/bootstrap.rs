//! Paired-block percentile bootstrap: the confidence interval that decides
//! whether a difference between two subjects survived the noise.
//!
//! # What a paired block is
//!
//! One block is one attempt: the same experiment, run on every subject, close
//! together in time on the same machine. Within a block the subjects share
//! whatever the machine was doing that minute, so the *ratio* of two subjects'
//! numbers inside one block is far quieter than either number across blocks.
//! The bootstrap therefore resamples whole blocks, never individual subjects'
//! numbers: breaking a block apart would throw away the pairing that makes the
//! comparison worth anything and would produce an interval that is far too
//! narrow.
//!
//! # The method, and what is inherited
//!
//! Ported from `summarizeRatios` in the legacy control plane's
//! `statistics.mjs`, which this repository treats as the behavioral reference:
//!
//! - the statistic is the **geometric mean** of the per-block ratios, so that
//!   the interval is symmetric under inverting the comparison;
//! - `resamples` replicates are drawn, each one `n` blocks sampled *with
//!   replacement* from the `n` observed blocks;
//! - the interval is the plain **percentile** interval — the 2.5th and 97.5th
//!   percentiles of the sorted replicate distribution, indexed as
//!   `floor(p * (m - 1))`, which is the legacy's index rule restated exactly;
//! - the legacy resample count, [`DEFAULT_BOOTSTRAP_RESAMPLES`], is 50,000.
//!
//! The generator is not inherited; see [`rng`](crate::rng) for why. Given the
//! same `(seed, resamples)` and the same blocks this function returns the same
//! interval on every platform and every run, which is the property that lets an
//! interval be sealed into evidence and recomputed later by a sceptic.
//!
//! # What the interval does not mean
//!
//! A percentile bootstrap over five blocks is descriptive. It reports where the
//! geometric mean of *these* blocks would have landed had the same machine
//! produced a different draw of the same blocks; it says nothing about a
//! different machine, a different broker, or a different day. A single block
//! yields a degenerate interval of zero width, which is retained as description
//! and is never evidence of precision.

use crate::error::{ReportError, ReportResult};
use crate::rng::Rng;
use crate::stats::geometric_mean;

/// Resamples the legacy control plane drew, and the default here.
pub const DEFAULT_BOOTSTRAP_RESAMPLES: u32 = 50_000;

/// The method name sealed alongside every interval this module produces.
pub const BOOTSTRAP_METHOD: &str = "paired-block-percentile-bootstrap";

/// Lower tail probability of the reported interval.
pub const LOWER_QUANTILE: f64 = 0.025;

/// Upper tail probability of the reported interval.
pub const UPPER_QUANTILE: f64 = 0.975;

/// One repetition of one metric, measured on both sides of a comparison.
///
/// `baseline` is the denominator and `candidate` the numerator, so a ratio
/// above one means the candidate produced the larger number — which is a win
/// for goodput and a loss for latency. Direction is the caller's business; this
/// module only divides.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PairedObservation {
    /// The value the ratio divides by.
    pub baseline: f64,
    /// The value the ratio is for.
    pub candidate: f64,
}

/// How a bootstrap is to be drawn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BootstrapOptions {
    /// Seed for the generator; the interval is a function of this value.
    pub seed: u64,
    /// Replicates to draw.
    pub resamples: u32,
}

impl Default for BootstrapOptions {
    /// The legacy control plane's settings: its seed was the ASCII `kafk`.
    fn default() -> Self {
        Self {
            seed: 0x6B61_666B,
            resamples: DEFAULT_BOOTSTRAP_RESAMPLES,
        }
    }
}

/// A paired ratio with the interval that says how much of it to believe.
#[derive(Debug, Clone, PartialEq)]
pub struct RatioInterval {
    /// Method name, always [`BOOTSTRAP_METHOD`].
    pub method: String,
    /// Paired blocks the interval rests on.
    pub blocks: usize,
    /// Replicates drawn.
    pub resamples: u32,
    /// Seed the replicates were drawn from.
    pub seed: u64,
    /// Geometric mean of the observed per-block ratios: the point estimate.
    pub point_ratio: f64,
    /// 2.5th percentile of the replicate distribution.
    pub lower: f64,
    /// 97.5th percentile of the replicate distribution.
    pub upper: f64,
}

impl RatioInterval {
    /// Reports whether the whole interval lies above `threshold`.
    ///
    /// This is the shape a gate wants for a metric where larger is better: not
    /// "the point estimate improved", but "even the pessimistic end of the
    /// interval improved".
    #[must_use]
    pub fn entirely_above(&self, threshold: f64) -> bool {
        self.lower > threshold
    }

    /// Reports whether the whole interval lies below `threshold`.
    #[must_use]
    pub fn entirely_below(&self, threshold: f64) -> bool {
        self.upper < threshold
    }

    /// Reports whether the interval contains `value`.
    #[must_use]
    pub fn contains(&self, value: f64) -> bool {
        self.lower <= value && value <= self.upper
    }

    /// Returns the midpoint of the interval on the log scale, which is the
    /// scale the ratios were averaged on.
    ///
    /// Used only to order findings by size; it is never reported as an
    /// estimate in its own right.
    #[must_use]
    pub fn log_midpoint(&self) -> f64 {
        f64::midpoint(self.lower.ln(), self.upper.ln()).exp()
    }
}

/// Computes the paired ratio and its percentile bootstrap interval.
///
/// # Errors
///
/// Returns a [`ReportErrorKind::Sample`](crate::ReportErrorKind::Sample) error
/// when there are no blocks, when a block carries a value that is not finite
/// and strictly positive, or when no replicates were asked for.
pub fn paired_ratio_interval(
    blocks: &[PairedObservation],
    options: &BootstrapOptions,
) -> ReportResult<RatioInterval> {
    let ratios = paired_ratios(blocks)?;
    if options.resamples == 0 {
        return Err(ReportError::sample(
            "a bootstrap of zero resamples produces no interval",
        ));
    }
    let point_ratio = geometric_mean(&ratios)
        .ok_or_else(|| ReportError::sample("paired ratios are not summarizable"))?;
    let (lower, upper) = percentile_interval(&ratios, options);
    Ok(RatioInterval {
        method: BOOTSTRAP_METHOD.to_owned(),
        blocks: ratios.len(),
        resamples: options.resamples,
        seed: options.seed,
        point_ratio,
        lower,
        upper,
    })
}

/// Returns the per-block ratios, rejecting any block that cannot produce one.
///
/// # Errors
///
/// Returns a sample error when the slice is empty or a value is not finite and
/// strictly positive.
pub fn paired_ratios(blocks: &[PairedObservation]) -> ReportResult<Vec<f64>> {
    if blocks.is_empty() {
        return Err(ReportError::sample(
            "at least one paired block is required for a ratio",
        ));
    }
    blocks
        .iter()
        .map(|block| {
            if !block.baseline.is_finite()
                || block.baseline <= 0.0
                || !block.candidate.is_finite()
                || block.candidate <= 0.0
            {
                return Err(ReportError::sample(
                    "paired observations must be finite positive numbers",
                ));
            }
            Ok(block.candidate / block.baseline)
        })
        .collect()
}

/// Draws the replicate distribution and returns its 2.5th and 97.5th
/// percentiles.
#[expect(
    clippy::indexing_slicing,
    reason = "every index is derived from the length of the vector being indexed"
)]
fn percentile_interval(ratios: &[f64], options: &BootstrapOptions) -> (f64, f64) {
    let mut rng = Rng::from_seed(options.seed);
    let count = ratios.len();
    let mut distribution = Vec::with_capacity(options.resamples as usize);
    for _ in 0..options.resamples {
        let mut log_total = 0.0f64;
        for _ in 0..count {
            log_total += ratios[rng.index_below(count)].ln();
        }
        #[expect(
            clippy::cast_precision_loss,
            reason = "block counts are small integers that f64 represents exactly"
        )]
        distribution.push((log_total / count as f64).exp());
    }
    distribution.sort_by(f64::total_cmp);
    (
        distribution[quantile_index(distribution.len(), LOWER_QUANTILE)],
        distribution[quantile_index(distribution.len(), UPPER_QUANTILE)],
    )
}

/// The legacy index rule: `floor(p * (m - 1))` over the sorted distribution.
#[expect(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "the product is a non-negative index below a length that came from a usize"
)]
fn quantile_index(length: usize, probability: f64) -> usize {
    if length == 0 {
        return 0;
    }
    let index = (probability * (length - 1) as f64).floor() as usize;
    index.min(length - 1)
}
