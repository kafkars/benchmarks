//! What each reporting subject spent per acknowledged record, and how those
//! costs compare.
//!
//! Kept apart from the verdict rule because it answers a different question.
//! The verdict is about whether one subject beat another on the metrics a claim
//! could rest on; these findings are about what the traffic cost to produce,
//! and they exist only for subjects whose client emits native statistics.
//!
//! A subject that emits none produces no finding here at all. The alternative —
//! a finding about a zero — would read as "this client sent no produce
//! requests", which is the opposite of what an absent measurement means.

use bench_schema::PacketFinding;

use crate::suite::SubjectEconomics;

use super::metrics::EconomicsIds;

/// Findings comparing what each reporting subject spent per record.
///
/// Only subjects that actually reported are compared, and the comparison names
/// both sides. A subject whose client emits no native statistics produces no
/// finding here at all, rather than a finding about a zero.
pub(super) fn economics_findings(
    ids: &[EconomicsIds],
    economics: &[SubjectEconomics],
) -> Vec<PacketFinding> {
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
