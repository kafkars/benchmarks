//! The deterministic analysis packet: everything an interpreter is allowed to
//! reason from, numbered so that it can be cited instead of restated.
//!
//! # Why numbering matters
//!
//! Prose about measurements drifts. A sentence that says "roughly eighteen
//! percent faster" survives a re-run that moved the number, and nobody notices.
//! A sentence that says "M016" cannot: the value lives in one place, the
//! citation resolves or it does not, and
//! [`validate_llm_summary`] rejects a summary that cites a metric this packet
//! never defined.
//!
//! # The metric id layout
//!
//! Ids are assigned `M001`, `M002`, … in one fixed pass, in this order, over
//! whatever the summary actually contains:
//!
//! 1. **medians** — for each subject in summary order, each metric in
//!    [`SuiteMetric::ALL`] order that the subject reported;
//! 2. **pairs** — for each paired ratio in summary order, three ids: the ratio
//!    of medians, the interval's low end, and its high end;
//! 3. **dispersion** — for each dispersion entry that has a coefficient of
//!    variation;
//! 4. **request economics** — for each subject that reported native client
//!    statistics, its normalized request costs.
//!
//! The order is a function of the summary alone, so the same evidence always
//! produces the same numbering; adding an attempt does not renumber anything,
//! while adding a subject or a metric does. A packet is therefore citable
//! within itself and across a re-run of the same suite, and never across
//! different suites — which is why every citation travels with its packet.
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

use std::collections::BTreeMap;

use bench_schema::{
    AnalysisPacket, LlmSummary, PacketFinding, PacketMetric, PacketSource, PacketSubject,
    PacketValidity, PairedRatio, SchemaResult, SuiteSummary, Verdict,
};

use crate::economics::STATISTICS_FILE_NAME;
use crate::suite::{
    SubjectEconomics, SuiteMetric, SuiteReport, metric_of_field, pair_passes, pair_regresses,
};
use crate::summary::{COEFFICIENT_OF_VARIATION_BUDGET, MINIMUM_PAIRED_REPETITIONS};

/// Builds the analysis packet for a suite summary.
///
/// Request economics are absent from a packet built this way, because
/// `kafkars.suite-summary.v1` has no field to carry them. Build the packet from
/// a [`SuiteReport`] — [`SuiteReport::packet`] — to include them.
#[must_use]
pub fn build_packet(summary: &SuiteSummary) -> AnalysisPacket {
    packet(summary, &[])
}

impl SuiteReport {
    /// Builds the analysis packet, including the request economics.
    #[must_use]
    pub fn packet(&self) -> AnalysisPacket {
        packet(&self.summary, &self.economics)
    }
}

/// Checks a model-written summary against the packet it claims to be about.
///
/// A thin delegation to
/// [`LlmSummary::validate_against`](bench_schema::LlmSummary::validate_against),
/// so that the control plane has one import point for the whole reporting
/// surface and the rule stays defined in exactly one place.
///
/// # Errors
///
/// Returns an error when the summary cites a metric or an evidence reference
/// the packet does not define, or when it states a verdict other than the
/// packet's.
pub fn validate_llm_summary(summary: &LlmSummary, packet: &AnalysisPacket) -> SchemaResult<()> {
    summary.validate_against(packet)
}

/// Builds a packet from a summary and whatever economics were measured.
fn packet(summary: &SuiteSummary, economics: &[SubjectEconomics]) -> AnalysisPacket {
    let mut metrics = MetricTable::default();
    let pair_ids = number_metrics(&mut metrics, summary);
    let economics_ids = economics_metrics(&mut metrics, economics);

    let runs_total = u32::try_from(summary.attempts.len()).unwrap_or(u32::MAX);
    let valid_attempts = summary.attempts.iter().filter(|a| a.run_valid).count();
    let runs_valid = u32::try_from(valid_attempts).unwrap_or(u32::MAX);

    AnalysisPacket {
        schema: AnalysisPacket::SCHEMA.to_owned(),
        source: PacketSource {
            suite: Some(summary.experiment_id.clone()),
            bundle_digests: summary
                .attempts
                .iter()
                .map(|attempt| attempt.bundle_digest.clone())
                .collect(),
        },
        verdict: verdict(valid_attempts, &pair_ids, summary.practical_threshold),
        scenario_name: summary.scenario_name.clone(),
        subjects: packet_subjects(summary),
        validity: PacketValidity {
            runs_valid,
            runs_total,
            claim_eligible: false,
        },
        metrics: metrics.entries,
        deterministic_findings: findings(
            summary,
            &pair_ids,
            &economics_ids,
            economics,
            runs_valid,
            runs_total,
        ),
        anomalies: anomalies(summary, &pair_ids, valid_attempts),
        evidence_refs: evidence_refs(summary, economics),
    }
}

/// Assigns ids to the medians, pairs, and dispersion, in the documented order.
fn number_metrics(metrics: &mut MetricTable, summary: &SuiteSummary) -> Vec<PairIds> {
    for median in &summary.medians {
        for metric in SuiteMetric::ALL {
            if let Some(value) = metric.median_of(median) {
                metrics.add(
                    format!("{} {} (median)", median.name, metric.label()),
                    value,
                    metric.unit(),
                );
            }
        }
    }
    let mut pair_ids = Vec::new();
    for pair in &summary.pairs {
        let Some(metric) = metric_of_field(&pair.metric) else {
            continue;
        };
        let label = format!(
            "{} over {} {}",
            pair.numerator_subject, pair.denominator_subject, pair.metric
        );
        let ratio = metrics.add(
            format!("{label} ratio of medians"),
            pair.ratio_of_medians,
            "ratio",
        );
        let low = metrics.add(format!("{label} interval low"), pair.ci_low, "ratio");
        let high = metrics.add(format!("{label} interval high"), pair.ci_high, "ratio");
        pair_ids.push(PairIds {
            pair: pair.clone(),
            metric,
            ratio,
            low,
            high,
        });
    }
    for entry in &summary.dispersion {
        if let Some(value) = entry.coefficient_of_variation {
            metrics.add(
                format!("{} {} coefficient of variation", entry.name, entry.metric),
                value,
                "ratio",
            );
        }
    }
    pair_ids
}

/// The statements the deterministic layer is willing to make.
fn findings(
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
fn anomalies(summary: &SuiteSummary, pair_ids: &[PairIds], valid_attempts: usize) -> Vec<String> {
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
fn verdict(valid_attempts: usize, pairs: &[PairIds], threshold: f64) -> Verdict {
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

/// The subjects the packet is about, from the medians when there are any and
/// from the first attempt's observations when there are not.
fn packet_subjects(summary: &SuiteSummary) -> Vec<PacketSubject> {
    if !summary.medians.is_empty() {
        return summary
            .medians
            .iter()
            .map(|median| PacketSubject {
                name: median.name.clone(),
                role: median.role.clone(),
            })
            .collect();
    }
    summary
        .attempts
        .first()
        .map(|attempt| {
            attempt
                .subjects
                .iter()
                .map(|observation| PacketSubject {
                    name: observation.name.clone(),
                    role: observation.role.clone(),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Short keys pointing at the evidence behind the packet.
fn evidence_refs(
    summary: &SuiteSummary,
    economics: &[SubjectEconomics],
) -> BTreeMap<String, String> {
    let mut refs = BTreeMap::new();
    for (index, attempt) in summary.attempts.iter().enumerate() {
        refs.insert(
            format!("A{:03}", index.saturating_add(1)),
            attempt.bundle_digest.clone(),
        );
    }
    for (index, subject) in packet_subjects(summary).iter().enumerate() {
        refs.insert(
            format!("R{:03}", index.saturating_add(1)),
            format!("adapters/{}/result.json", subject.name),
        );
    }
    for (index, subject) in economics.iter().enumerate() {
        refs.insert(
            format!("S{:03}", index.saturating_add(1)),
            format!("adapters/{}/{STATISTICS_FILE_NAME}", subject.subject),
        );
    }
    refs
}

/// One pair, with the ids its three numbers were assigned.
#[derive(Debug, Clone)]
struct PairIds {
    /// The pair itself.
    pair: PairedRatio,
    /// The metric it is over.
    metric: SuiteMetric,
    /// Metric id of the ratio of medians.
    ratio: String,
    /// Metric id of the interval's low end.
    low: String,
    /// Metric id of the interval's high end.
    high: String,
}

impl PairIds {
    /// The finding this pair supports, citing its own three metrics.
    fn finding(&self, verb: &str, threshold: f64) -> PacketFinding {
        PacketFinding {
            text: format!(
                "{} over {} {} is {:.4} of the baseline, and the whole interval \
                 [{:.4}, {:.4}] {verb} the practical threshold of {threshold:.4} \
                 ({}).",
                self.pair.numerator_subject,
                self.pair.denominator_subject,
                self.pair.metric,
                self.pair.ratio_of_medians,
                self.pair.ci_low,
                self.pair.ci_high,
                if self.metric.higher_is_better() {
                    "larger is better"
                } else {
                    "smaller is better"
                }
            ),
            metric_refs: vec![self.ratio.clone(), self.low.clone(), self.high.clone()],
        }
    }

    /// How far this pair's interval sits from parity, on the log scale.
    fn distance(&self) -> f64 {
        f64::midpoint(self.pair.ci_low.ln(), self.pair.ci_high.ln()).abs()
    }
}

/// The pair whose interval sits furthest from parity.
fn extreme<'a>(pairs: &[&'a PairIds]) -> Option<&'a PairIds> {
    pairs
        .iter()
        .copied()
        .max_by(|left, right| left.distance().total_cmp(&right.distance()))
}

/// One subject's economics ids.
#[derive(Debug, Clone)]
struct EconomicsIds {
    /// Subject name.
    subject: String,
    /// Metric id of produce requests per million acknowledged records.
    per_million: Option<String>,
    /// Metric id of records per produce request.
    records_per_request: Option<String>,
    /// Metric id of the payload share of transmitted bytes.
    payload_share: Option<String>,
}

/// Adds the request-economics metrics and returns their ids.
fn economics_metrics(
    metrics: &mut MetricTable,
    economics: &[SubjectEconomics],
) -> Vec<EconomicsIds> {
    economics
        .iter()
        .map(|entry| EconomicsIds {
            subject: entry.subject.clone(),
            per_million: entry
                .totals
                .produce_requests_per_million_acknowledged
                .map(|value| {
                    metrics.add(
                        format!(
                            "{} produce requests per million acknowledged records",
                            entry.subject
                        ),
                        value,
                        "requests",
                    )
                }),
            records_per_request: entry.totals.records_per_produce_request.map(|value| {
                metrics.add(
                    format!("{} records per produce request", entry.subject),
                    value,
                    "records",
                )
            }),
            payload_share: entry
                .totals
                .payload_bytes_per_transmitted_byte
                .map(|value| {
                    metrics.add(
                        format!("{} payload share of transmitted bytes", entry.subject),
                        value,
                        "ratio",
                    )
                }),
        })
        .collect()
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

/// Assigns `M001`, `M002`, … in insertion order.
#[derive(Debug, Default)]
struct MetricTable {
    /// The metrics assigned so far.
    entries: BTreeMap<String, PacketMetric>,
}

impl MetricTable {
    /// Adds one metric and returns the id it was given.
    fn add(&mut self, name: String, value: f64, unit: &str) -> String {
        let id = format!("M{:03}", self.entries.len().saturating_add(1));
        self.entries.insert(
            id.clone(),
            PacketMetric {
                name,
                value,
                unit: unit.to_owned(),
            },
        );
        id
    }
}
