//! Assembling one `kafkars.analysis-packet.v1` out of a decided summary.
//!
//! The numbering pass runs first and everything else cites its ids, so the
//! packet is internally consistent by construction: no finding can name a
//! metric the table does not define, because the table is what handed out the
//! name.

use std::collections::BTreeMap;

use bench_schema::{AnalysisPacket, PacketSource, PacketSubject, PacketValidity, SuiteSummary};

use crate::economics::STATISTICS_FILE_NAME;
use crate::suite::{SubjectEconomics, SuiteReport};

use super::findings::{anomalies, findings, verdict};
use super::metrics::{MetricTable, PairIds, economics_metrics, number_metrics};

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

/// Builds a packet from a summary and whatever economics were measured.
fn packet(summary: &SuiteSummary, economics: &[SubjectEconomics]) -> AnalysisPacket {
    let mut metrics = MetricTable::default();
    let pair_ids = number_metrics(&mut metrics, summary);
    // Attribution pairs are numbered and citable, so prose may point at them —
    // and are kept out of everything that decides. A verdict drawn from the
    // accepted-to-terminal portion would say "faster" about a client that got
    // there by refusing work for longer.
    let claimable: Vec<PairIds> = pair_ids
        .iter()
        .filter(|entry| entry.metric.claimable())
        .cloned()
        .collect();
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
        verdict: verdict(valid_attempts, &claimable, summary.practical_threshold),
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
            &claimable,
            &economics_ids,
            economics,
            runs_valid,
            runs_total,
        ),
        anomalies: anomalies(summary, &claimable, valid_attempts),
        evidence_refs: evidence_refs(summary, economics),
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
