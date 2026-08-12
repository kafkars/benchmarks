//! Paired ratios, and the two questions asked of each one's interval.
//!
//! Ratios are always *numerator over denominator of the medians*, and the
//! interval beside them is the paired-block bootstrap over the per-attempt
//! pairs — the same attempts, in the same order, for both subjects.
//!
//! An interval that straddles the threshold is not a small effect, it is an
//! unresolved one. [`pair_passes`] and [`pair_regresses`] are therefore not
//! each other's negation: both are false for an unresolved pair, and a caller
//! that wants to say something about it has to say "unresolved".

use bench_schema::{PairedRatio, SubjectMedians, SuiteAttempt};

use crate::bootstrap::{BootstrapOptions, PairedObservation, paired_ratio_interval};
use crate::error::ReportResult;

use super::comparison::Comparison;
use super::metric::SuiteMetric;
use super::report::SuiteOptions;

/// Every paired ratio, with the bootstrap interval over the paired attempts.
pub(super) fn paired_ratios(
    comparisons: &[Comparison],
    medians: &[SubjectMedians],
    valid: &[&SuiteAttempt],
    options: &SuiteOptions,
) -> ReportResult<Vec<PairedRatio>> {
    let bootstrap = BootstrapOptions {
        seed: options.seed,
        resamples: options.resamples,
    };
    let mut ratios = Vec::new();
    for comparison in comparisons {
        for metric in SuiteMetric::ALL {
            let (Some(numerator), Some(denominator)) = (
                medians
                    .iter()
                    .find(|entry| entry.name == comparison.numerator)
                    .and_then(|entry| metric.median_of(entry)),
                medians
                    .iter()
                    .find(|entry| entry.name == comparison.denominator)
                    .and_then(|entry| metric.median_of(entry)),
            ) else {
                continue;
            };
            if denominator <= 0.0 || numerator <= 0.0 {
                continue;
            }
            let blocks: Vec<PairedObservation> = valid
                .iter()
                .filter_map(|attempt| {
                    let find = |name: &str| {
                        attempt
                            .subjects
                            .iter()
                            .find(|entry| entry.name == name)
                            .and_then(|entry| metric.observed(entry))
                    };
                    match (find(&comparison.numerator), find(&comparison.denominator)) {
                        (Some(candidate), Some(baseline)) => Some(PairedObservation {
                            baseline,
                            candidate,
                        }),
                        _ => None,
                    }
                })
                .collect();
            let Ok(interval) = paired_ratio_interval(&blocks, &bootstrap) else {
                continue;
            };
            ratios.push(PairedRatio {
                numerator_subject: comparison.numerator.clone(),
                denominator_subject: comparison.denominator.clone(),
                metric: metric.field().to_owned(),
                ratio_of_medians: numerator / denominator,
                ci_low: interval.lower,
                ci_high: interval.upper,
            });
        }
    }
    Ok(ratios)
}

/// Reports whether a pair cleared the practical threshold on the favorable
/// side, with the whole interval.
#[must_use]
pub fn pair_passes(pair: &PairedRatio, metric: SuiteMetric, threshold: f64) -> bool {
    if metric.higher_is_better() {
        pair.ci_low > 1.0 + threshold
    } else {
        pair.ci_high < 1.0 - threshold
    }
}

/// Reports whether a pair is a regression: the whole interval cleared the
/// threshold on the *unfavorable* side.
#[must_use]
pub fn pair_regresses(pair: &PairedRatio, metric: SuiteMetric, threshold: f64) -> bool {
    if metric.higher_is_better() {
        pair.ci_high < 1.0 - threshold
    } else {
        pair.ci_low > 1.0 + threshold
    }
}
