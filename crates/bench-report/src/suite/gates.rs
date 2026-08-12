//! Every gate the suite checked, in the order it checked them.
//!
//! A gate is not "the point estimate moved". A metric gate passes only when the
//! **entire** confidence interval clears the practical threshold on the
//! favorable side:
//!
//! - goodput, where larger is better, passes when `ci_low > 1 + threshold`;
//! - latency, CPU, and memory, where smaller is better, pass when
//!   `ci_high < 1 - threshold`.
//!
//! An interval that straddles the threshold is not a small effect, it is an
//! unresolved one, and the gate says so rather than rounding it to a verdict.
//! Three gates are not about any pair: at least one attempt has to be valid, a
//! comparison needs [`MINIMUM_PAIRED_REPETITIONS`] valid attempts, and every
//! compared pair's goodput and p99 *ratio* has to vary by no more than
//! [`COEFFICIENT_OF_VARIATION_BUDGET`].

use bench_schema::{GateOutcome, PairedRatio, SubjectDispersion, SuiteAttempt};

use crate::summary::{COEFFICIENT_OF_VARIATION_BUDGET, MINIMUM_PAIRED_REPETITIONS};

use super::comparison::Comparison;
use super::metric::{SuiteMetric, metric_of_field};
use super::pairs::pair_passes;
use super::report::SuiteOptions;

/// Every gate the suite checked, in the order it checked them.
pub(super) fn gates(
    comparisons: &[Comparison],
    pairs: &[PairedRatio],
    pair_dispersion: &[SubjectDispersion],
    valid: &[&SuiteAttempt],
    options: &SuiteOptions,
) -> Vec<GateOutcome> {
    let mut gates = vec![
        GateOutcome {
            name: "attempts-valid".to_owned(),
            description: "at least one attempt has to be usable evidence".to_owned(),
            passed: !valid.is_empty(),
            detail: format!("{} valid attempts", valid.len()),
        },
        GateOutcome {
            name: "paired-repetitions".to_owned(),
            description: format!(
                "a comparison needs at least {MINIMUM_PAIRED_REPETITIONS} valid paired attempts"
            ),
            passed: valid.len() >= MINIMUM_PAIRED_REPETITIONS,
            detail: format!("{} of {MINIMUM_PAIRED_REPETITIONS} required", valid.len()),
        },
    ];

    // Goodput and p99 for every comparison: the series that must exist before
    // the budget can say anything.
    let expected_series = 2 * comparisons.len();
    let gated: Vec<&SubjectDispersion> = pair_dispersion
        .iter()
        .filter(|entry| {
            matches!(
                metric_of_field(&entry.metric),
                Some(SuiteMetric::Goodput | SuiteMetric::P99Latency)
            )
        })
        .collect();
    let noisy: Vec<String> = gated
        .iter()
        .filter(|entry| {
            !entry
                .coefficient_of_variation
                .is_some_and(|value| value <= COEFFICIENT_OF_VARIATION_BUDGET)
        })
        .map(|entry| {
            entry.coefficient_of_variation.map_or_else(
                || format!("{} {} (undefined)", entry.name, entry.metric),
                |value| format!("{} {} ({value:.4})", entry.name, entry.metric),
            )
        })
        .collect();
    gates.push(GateOutcome {
        name: "dispersion-within-budget".to_owned(),
        description: format!(
            "every compared pair's goodput and p99 ratio must vary by no more than \
             {COEFFICIENT_OF_VARIATION_BUDGET:.2} of its mean, over the same per-attempt \
             ratios the interval is drawn from"
        ),
        // A suite with no ratio series to judge does not pass this gate by
        // having nothing to fail. One subject, or no attempt where both subjects
        // reported, means the dispersion the budget is about was never measured,
        // and "not measured" is not "inside the budget".
        passed: expected_series > 0 && gated.len() == expected_series && noisy.is_empty(),
        detail: if expected_series == 0 {
            "there is no compared pair, so the dispersion this budget is about was never \
             measured"
                .to_owned()
        } else if gated.len() != expected_series {
            format!(
                "{} of the {expected_series} gated ratio series could be formed at all",
                gated.len()
            )
        } else if noisy.is_empty() {
            "every gated ratio dispersion is inside the budget".to_owned()
        } else {
            format!("outside the budget or undefined: {}", noisy.join(", "))
        },
    });

    for comparison in comparisons {
        for metric in SuiteMetric::ALL {
            let Some(pair) = pairs.iter().find(|pair| {
                pair.numerator_subject == comparison.numerator
                    && pair.denominator_subject == comparison.denominator
                    && pair.metric == metric.field()
            }) else {
                continue;
            };
            gates.push(GateOutcome {
                name: format!(
                    "{}-over-{}:{}",
                    comparison.numerator,
                    comparison.denominator,
                    metric.field()
                ),
                description: metric.gate_description(options.practical_threshold),
                passed: pair_passes(pair, metric, options.practical_threshold),
                detail: format!(
                    "ratio of medians {:.4}, interval [{:.4}, {:.4}]",
                    pair.ratio_of_medians, pair.ci_low, pair.ci_high
                ),
            });
        }
    }
    gates
}
