//! The numbered metric table, and the ids each part of the summary is given.
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

use std::collections::BTreeMap;

use bench_schema::{PacketFinding, PacketMetric, PairedRatio, SuiteSummary};

use crate::suite::{SubjectEconomics, SuiteMetric, metric_of_field};

/// Assigns `M001`, `M002`, … in insertion order.
#[derive(Debug, Default)]
pub(super) struct MetricTable {
    /// The metrics assigned so far.
    pub(super) entries: BTreeMap<String, PacketMetric>,
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

/// One pair, with the ids its three numbers were assigned.
#[derive(Debug, Clone)]
pub(super) struct PairIds {
    /// The pair itself.
    pub(super) pair: PairedRatio,
    /// The metric it is over.
    pub(super) metric: SuiteMetric,
    /// Metric id of the ratio of medians.
    ratio: String,
    /// Metric id of the interval's low end.
    low: String,
    /// Metric id of the interval's high end.
    high: String,
}

impl PairIds {
    /// The finding this pair supports, citing its own three metrics.
    pub(super) fn finding(&self, verb: &str, threshold: f64) -> PacketFinding {
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
pub(super) fn extreme<'a>(pairs: &[&'a PairIds]) -> Option<&'a PairIds> {
    pairs
        .iter()
        .copied()
        .max_by(|left, right| left.distance().total_cmp(&right.distance()))
}

/// One subject's economics ids.
#[derive(Debug, Clone)]
pub(super) struct EconomicsIds {
    /// Subject name.
    pub(super) subject: String,
    /// Metric id of produce requests per million acknowledged records.
    pub(super) per_million: Option<String>,
    /// Metric id of records per produce request.
    pub(super) records_per_request: Option<String>,
    /// Metric id of the payload share of transmitted bytes.
    pub(super) payload_share: Option<String>,
}

/// Assigns ids to the medians, pairs, and dispersion, in the documented order.
pub(super) fn number_metrics(metrics: &mut MetricTable, summary: &SuiteSummary) -> Vec<PairIds> {
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

/// Adds the request-economics metrics and returns their ids.
pub(super) fn economics_metrics(
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
