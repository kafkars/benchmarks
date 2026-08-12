//! Descriptive summary of repeated positive measurements, plus the two
//! thresholds that decide whether such a summary is credible.
//!
//! Semantics mirror `summarizePositiveValues` in the legacy control plane's
//! `statistics.mjs`, which is the behavioral reference until the Node harness
//! is retired:
//!
//! - an empty sample is an error, not an empty result;
//! - every value must be finite and strictly positive, because these samples are
//!   throughputs, latencies, and ratios, and a zero or a NaN in that position
//!   means the measurement failed rather than measured zero;
//! - the coefficient of variation uses the *sample* standard deviation, with
//!   `n - 1` in the denominator, and is absent rather than zero for a single
//!   repetition.
//!
//! The geometric mean, median, extrema, and percentile bootstrap that
//! `statistics.mjs` also computes are intentionally not duplicated here yet.

use serde::{Deserialize, Serialize};

/// Fewest paired repetitions a comparison needs before its summary may support
/// a claim.
///
/// Mirrors `MINIMUM_PAIRED_REPETITIONS` in `statistics.mjs`.
pub const MINIMUM_PAIRED_REPETITIONS: usize = 5;

/// Largest coefficient of variation a sample may show and still be treated as
/// inside the measurement noise budget.
///
/// Mirrors `COEFFICIENT_OF_VARIATION_BUDGET` in `statistics.mjs`.
pub const COEFFICIENT_OF_VARIATION_BUDGET: f64 = 0.05;

/// Why a sample could not be summarized.
///
/// Both variants describe an input that is structurally unusable, so they are
/// programming or harness errors rather than measurement outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SummaryError {
    /// The sample contained no values at all.
    Empty,
    /// The sample contained a value that was not finite and strictly positive.
    NotFinitePositive,
}

impl core::fmt::Display for SummaryError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let message = match self {
            Self::Empty => "at least one finite positive value is required",
            Self::NotFinitePositive => "values must be finite positive numbers",
        };
        formatter.write_str(message)
    }
}

impl core::error::Error for SummaryError {}

/// Central tendency and dispersion for one sample of positive measurements.
///
/// Field names match the JSON keys the legacy control plane seals, so this
/// struct serializes into the shape existing evidence readers already accept.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositiveValueSummary {
    /// Number of values in the sample.
    pub repetitions: usize,
    /// Arithmetic mean of the sample.
    pub arithmetic_mean: f64,
    /// Sample standard deviation divided by the arithmetic mean, or `None` when
    /// a single repetition leaves dispersion undefined.
    pub coefficient_of_variation: Option<f64>,
}

impl PositiveValueSummary {
    /// Reports whether the sample is quiet enough to be trusted.
    ///
    /// A sample with undefined dispersion is never inside the budget: one
    /// repetition proves nothing about run-to-run noise. Mirrors
    /// `insideNoiseBudget` in `statistics.mjs`.
    pub fn inside_noise_budget(&self) -> bool {
        self.coefficient_of_variation
            .is_some_and(|variation| variation <= COEFFICIENT_OF_VARIATION_BUDGET)
    }

    /// Reports whether the sample has at least [`MINIMUM_PAIRED_REPETITIONS`]
    /// repetitions.
    pub fn enough_repetitions(&self) -> bool {
        self.repetitions >= MINIMUM_PAIRED_REPETITIONS
    }
}

/// Summarizes a sample of finite, strictly positive measurements.
///
/// Rejects an empty sample and rejects any value that is not finite and
/// positive, rather than silently producing a summary of nonsense.
#[expect(
    clippy::cast_precision_loss,
    reason = "repetition counts are small integers that f64 represents exactly"
)]
pub fn summarize_positive_values(values: &[f64]) -> Result<PositiveValueSummary, SummaryError> {
    if values.is_empty() {
        return Err(SummaryError::Empty);
    }
    if !values.iter().all(|value| value.is_finite() && *value > 0.0) {
        return Err(SummaryError::NotFinitePositive);
    }

    let repetitions = values.len();
    let total: f64 = values.iter().sum();
    let arithmetic_mean = total / repetitions as f64;
    let coefficient_of_variation = if repetitions < 2 {
        None
    } else {
        let squared_difference: f64 = values
            .iter()
            .map(|value| (value - arithmetic_mean).powi(2))
            .sum();
        let standard_deviation = (squared_difference / (repetitions - 1) as f64).sqrt();
        Some(standard_deviation / arithmetic_mean)
    };

    Ok(PositiveValueSummary {
        repetitions,
        arithmetic_mean,
        coefficient_of_variation,
    })
}
