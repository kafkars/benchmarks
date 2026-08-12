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
//! | no comparable pair, or `F == 0 && U == 0` | `inconclusive` |
//! | `F > 0 && U == 0` | `improved` |
//! | `U > 0 && F == 0` | `regressed` |
//! | `F > 0 && U > 0` | `mixed` |
//!
//! Unresolved pairs never decide a verdict, and they are never silent either:
//! each one is listed in `anomalies`, so `improved` alongside four unresolved
//! metrics reads as what it is.
//!
//! Nothing downstream may disagree with this verdict. That is the entire point
//! of computing it here.

use bench_schema::{PacketFinding, SuiteSummary, Verdict};

use crate::suite::{SubjectEconomics, SuiteMetric, metric_of_field, pair_passes, pair_regresses};
use crate::summary::{COEFFICIENT_OF_VARIATION_BUDGET, MINIMUM_PAIRED_REPETITIONS};

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
    findings.extend(economics_findings(economics_ids, economics));
    findings
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

/// Findings comparing what each reporting subject spent per record.
///
/// Only subjects that actually reported are compared, and the comparison names
/// both sides. A subject whose client emits no native statistics produces no
/// finding here at all, rather than a finding about a zero.
fn economics_findings(ids: &[EconomicsIds], economics: &[SubjectEconomics]) -> Vec<PacketFinding> {
    let mut findings = Vec::new();
    for entry in ids {
        let Some(subject) = economics
            .iter()
            .find(|candidate| candidate.subject == entry.subject)
        else {
            continue;
        };
        let mut refs = Vec::new();
        let mut parts = Vec::new();
        if let (Some(id), Some(value)) = (
            entry.per_million.clone(),
            subject.totals.produce_requests_per_million_acknowledged,
        ) {
            parts.push(format!("{value:.1} produce requests per million records"));
            refs.push(id);
        }
        if let (Some(id), Some(value)) = (
            entry.records_per_request.clone(),
            subject.totals.records_per_produce_request,
        ) {
            parts.push(format!("{value:.2} records per request"));
            refs.push(id);
        }
        if let (Some(id), Some(value)) = (
            entry.payload_share.clone(),
            subject.totals.payload_bytes_per_transmitted_byte,
        ) {
            parts.push(format!("{value:.4} of transmitted bytes were payload"));
            refs.push(id);
        }
        if parts.is_empty() {
            continue;
        }
        findings.push(PacketFinding {
            text: format!("{} spent {}.", entry.subject, parts.join(", ")),
            metric_refs: refs,
        });
    }
    if ids.len() >= 2 {
        let mut pairs = Vec::new();
        for entry in ids.windows(2) {
            let (Some(first), Some(second)) = (entry.first(), entry.get(1)) else {
                continue;
            };
            let values = |name: &str| {
                economics
                    .iter()
                    .find(|candidate| candidate.subject == name)
                    .and_then(|candidate| {
                        candidate.totals.produce_requests_per_million_acknowledged
                    })
            };
            let (Some(left), Some(right)) = (values(&first.subject), values(&second.subject))
            else {
                continue;
            };
            if left <= 0.0 {
                continue;
            }
            let mut refs = Vec::new();
            refs.extend(first.per_million.clone());
            refs.extend(second.per_million.clone());
            pairs.push(PacketFinding {
                text: format!(
                    "{} spent {:.4} times as many produce requests per acknowledged record as {}.",
                    second.subject,
                    right / left,
                    first.subject
                ),
                metric_refs: refs,
            });
        }
        findings.extend(pairs);
    }
    findings
}
