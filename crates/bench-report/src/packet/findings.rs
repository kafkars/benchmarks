//! The statements the deterministic layer is willing to make, and the verdict
//! nothing downstream may disagree with.
//!
//! # The verdict rule, exactly
//!
//! Each comparable pair is *favorable* when its whole interval clears the
//! practical threshold on the good side, *unfavorable* when its whole interval
//! clears it on the bad side, and *unresolved* otherwise. With `F` favorable
//! and `U` unfavorable pairs:
//!
//! | condition | verdict |
//! | --- | --- |
//! | no valid attempt | `invalid` |
//! | fewer than [`MINIMUM_PAIRED_REPETITIONS`] valid attempts | `inconclusive` |
//! | no comparable pair, or `F == 0 && U == 0` | `inconclusive` |
//! | `F > 0 && U == 0` | `improved` |
//! | `U > 0 && F == 0` | `regressed` |
//! | `F > 0 && U > 0` | `mixed` |
//!
//! Only a [claimable](crate::SuiteMetric::claimable) metric's pair is counted.
//! Scheduler lateness and the accepted-to-terminal portion are attribution, and
//! a verdict drawn from either would be a claim the evidence cannot carry.
//!
//! # Too few repetitions is inconclusive, and says which way it leaned
//!
//! A run below the repetition minimum has an interval, and that interval can
//! sit entirely past the threshold — but a bootstrap over two paired blocks is
//! an interval over two numbers, and calling it `improved` states more than the
//! evidence supports. The verdict is therefore `inconclusive`, and the
//! directional read is not discarded: it moves into a deterministic finding
//! that names the direction *and* the repetition count in the same sentence, so
//! a reader gets the signal without the document asserting it.
//!
//! Unresolved pairs never decide a verdict, and they are never silent either:
//! each one is listed in `anomalies`, so `improved` alongside four unresolved
//! metrics reads as what it is.
//!
//! Nothing downstream may disagree with this verdict. That is the entire point
//! of computing it here.

use bench_schema::{PacketFinding, SuiteSummary, Verdict};

use crate::suite::{
    MATCHED_EXECUTION_SURFACE, SubjectEconomics, SuiteMetric, metric_of_field, pair_passes,
    pair_regresses,
};
use crate::summary::{COEFFICIENT_OF_VARIATION_BUDGET, MINIMUM_PAIRED_REPETITIONS};

use super::costs::economics_findings;
use super::metrics::{EconomicsIds, PairIds, extreme};

/// The statements the deterministic layer is willing to make.
pub(super) fn findings(
    summary: &SuiteSummary,
    pair_ids: &[PairIds],
    economics_ids: &[EconomicsIds],
    economics: &[SubjectEconomics],
    runs_valid: u32,
    runs_total: u32,
) -> Vec<PacketFinding> {
    let mut findings = vec![PacketFinding {
        text: format!(
            "{runs_valid} of {runs_total} attempts were valid evidence; every median, ratio, \
             and interval in this packet is over those {runs_valid}."
        ),
        metric_refs: Vec::new(),
    }];
    findings.extend(unmatched_surface_finding(summary));
    let favorable: Vec<&PairIds> = pair_ids
        .iter()
        .filter(|entry| pair_passes(&entry.pair, entry.metric, summary.practical_threshold))
        .collect();
    let unfavorable: Vec<&PairIds> = pair_ids
        .iter()
        .filter(|entry| pair_regresses(&entry.pair, entry.metric, summary.practical_threshold))
        .collect();
    if let Some(best) = extreme(&favorable) {
        findings.push(best.finding("clears", summary.practical_threshold));
    }
    if let Some(worst) = extreme(&unfavorable) {
        findings.push(worst.finding("breaks", summary.practical_threshold));
    }
    findings.extend(under_repeated_finding(
        pair_ids,
        summary.practical_threshold,
        runs_valid as usize,
    ));
    findings.extend(economics_findings(economics_ids, economics));
    findings
}

/// The finding a packet carries when the compared subjects declared unlike work.
///
/// It is stated as a finding rather than only as an anomaly because it changes
/// what every other finding in the packet means: a ratio between unlike
/// measurements is not a weak comparison, it is not a comparison.
fn unmatched_surface_finding(summary: &SuiteSummary) -> Option<PacketFinding> {
    let gate = summary
        .gates
        .iter()
        .find(|gate| gate.name == MATCHED_EXECUTION_SURFACE && !gate.passed)?;
    Some(PacketFinding {
        text: format!(
            "The compared subjects did not declare the same measured work, so every ratio in \
             this packet is between unlike measurements: {}.",
            gate.detail
        ),
        metric_refs: Vec::new(),
    })
}

/// The directional read a run below the repetition minimum is not allowed to
/// state as a verdict.
fn under_repeated_finding(
    pairs: &[PairIds],
    threshold: f64,
    valid_attempts: usize,
) -> Option<PacketFinding> {
    if valid_attempts == 0 || valid_attempts >= MINIMUM_PAIRED_REPETITIONS {
        return None;
    }
    let direction = match directional(pairs, threshold) {
        Verdict::Improved => "improved",
        Verdict::Regressed => "regressed",
        Verdict::Mixed => "mixed",
        Verdict::Inconclusive | Verdict::Invalid => return None,
    };
    Some(PacketFinding {
        text: format!(
            "Directionally {direction} on n={valid_attempts}; below the \
             {MINIMUM_PAIRED_REPETITIONS} paired repetitions a comparison needs, so the verdict \
             is inconclusive rather than {direction}."
        ),
        metric_refs: Vec::new(),
    })
}

/// Everything a reader has to weigh before believing the verdict.
pub(super) fn anomalies(
    summary: &SuiteSummary,
    pair_ids: &[PairIds],
    valid_attempts: usize,
) -> Vec<String> {
    let mut anomalies = Vec::new();
    for entry in pair_ids {
        if !pair_passes(&entry.pair, entry.metric, summary.practical_threshold)
            && !pair_regresses(&entry.pair, entry.metric, summary.practical_threshold)
        {
            anomalies.push(format!(
                "{} over {} {} is unresolved: the interval [{:.4}, {:.4}] straddles the \
                 practical threshold of {:.4}",
                entry.pair.numerator_subject,
                entry.pair.denominator_subject,
                entry.pair.metric,
                entry.pair.ci_low,
                entry.pair.ci_high,
                summary.practical_threshold
            ));
        }
    }
    anomalies.extend(dispersion_anomalies(summary));
    if valid_attempts < MINIMUM_PAIRED_REPETITIONS {
        anomalies.push(format!(
            "{valid_attempts} valid attempts is below the {MINIMUM_PAIRED_REPETITIONS} paired \
             repetitions a comparison needs"
        ));
    }
    for gate in summary.gates.iter().filter(|gate| !gate.passed) {
        anomalies.push(format!("gate {} did not pass: {}", gate.name, gate.detail));
    }
    anomalies.extend(summary.notes.iter().cloned());
    anomalies
}

/// Dispersion that is outside the budget, or absent where it was needed.
fn dispersion_anomalies(summary: &SuiteSummary) -> Vec<String> {
    summary
        .dispersion
        .iter()
        .filter(|entry| {
            matches!(
                metric_of_field(&entry.metric),
                Some(SuiteMetric::Goodput | SuiteMetric::P99Latency)
            )
        })
        .filter_map(|entry| match entry.coefficient_of_variation {
            Some(value) if value > COEFFICIENT_OF_VARIATION_BUDGET => Some(format!(
                "{} {} varies by {value:.4} of its mean, above the budget of \
                 {COEFFICIENT_OF_VARIATION_BUDGET:.2}",
                entry.name, entry.metric
            )),
            None => Some(format!(
                "{} {} has no measurable dispersion, so its stability is unknown rather than \
                 good",
                entry.name, entry.metric
            )),
            Some(_) => None,
        })
        .collect()
}

/// The packet's verdict, by the rule documented on this module.
pub(super) fn verdict(valid_attempts: usize, pairs: &[PairIds], threshold: f64) -> Verdict {
    if valid_attempts == 0 {
        return Verdict::Invalid;
    }
    if valid_attempts < MINIMUM_PAIRED_REPETITIONS {
        // The direction is not lost — `under_repeated_finding` states it, with
        // the repetition count attached so it cannot be quoted without one.
        return Verdict::Inconclusive;
    }
    directional(pairs, threshold)
}

/// Which way the intervals lean, before the repetition minimum is applied.
fn directional(pairs: &[PairIds], threshold: f64) -> Verdict {
    let favorable = pairs
        .iter()
        .filter(|entry| pair_passes(&entry.pair, entry.metric, threshold))
        .count();
    let unfavorable = pairs
        .iter()
        .filter(|entry| pair_regresses(&entry.pair, entry.metric, threshold))
        .count();
    match (favorable, unfavorable) {
        (0, 0) => Verdict::Inconclusive,
        (_, 0) => Verdict::Improved,
        (0, _) => Verdict::Regressed,
        _ => Verdict::Mixed,
    }
}
