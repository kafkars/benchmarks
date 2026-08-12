//! The two sealed verdict documents: what may be believed, and what may be
//! compared.
//!
//! [`DEFERRED_CHECKS`] is the honest half of the verdict. Every check the legacy
//! Node harness performs that a single attempt's classification does not is named
//! in the classification document, so that a reader knows exactly what "valid"
//! does not cover here.

use bench_schema::{
    Classification, Comparison, ComparisonPair, PRODUCER_BENCHMARK_V2, SubjectValidity,
};

use super::outcome::SubjectOutcome;

/// Checks a single attempt's classification does not perform, named in every
/// classification.
///
/// These are the gates the legacy Node control plane still owns, or that only a
/// repetition suite can answer. Listing them by name is the alternative to a
/// footnote nobody reads: a bundle states what it did not check, in the same
/// document that states what it did. `benchctl suite` answers the first two
/// across attempts; one attempt still cannot.
pub const DEFERRED_CHECKS: [&str; 7] = [
    "bootstrap-confidence-intervals",
    "paired-repetition-suites",
    "latency-csv-row-validation",
    "native-metric-gates",
    "librdkafka-statistics-summaries",
    "compressed-payload-coverage",
    "process-resource-capture",
];

/// Builds the attempt's classification from its subjects.
///
/// `extra_reasons` carries facts the subjects cannot see — a failed topic
/// creation, an interrupt, a phase that never ran — which invalidate the run as
/// a whole even when a subject's own documents look fine.
#[must_use]
pub fn classify(subjects: &[SubjectOutcome], extra_reasons: &[String]) -> Classification {
    let verdicts: Vec<SubjectValidity> = subjects.iter().map(SubjectOutcome::validity).collect();
    let mut reasons: Vec<String> = extra_reasons.to_vec();
    if subjects.is_empty() {
        reasons.push("the attempt ran no subjects".to_owned());
    }
    for verdict in &verdicts {
        for reason in &verdict.reasons {
            reasons.push(format!("{}: {reason}", verdict.name));
        }
    }
    Classification {
        schema: Classification::SCHEMA.to_owned(),
        run_valid: reasons.is_empty(),
        // Never true from one attempt: the deferred checks below are exactly the
        // evidence a published claim would need, and most of them need a suite.
        claim_eligible: false,
        subjects: verdicts,
        deferred_checks: DEFERRED_CHECKS
            .iter()
            .map(|check| (*check).to_owned())
            .collect(),
        reasons,
    }
}

/// Builds the attempt's comparison document.
///
/// Subjects are comparable only when every one of them produced a readable
/// `kafkars.producer-benchmark.v2` result: ratios between documents of different
/// shapes would be comparing different measurements. The baseline is the first
/// subject in execution order, and every later subject is a candidate against
/// it, so a ratio above one on goodput and below one on latency both favour the
/// candidate.
///
/// Both ratios come from the v2 document itself. Goodput is
/// `throughput.acknowledged_records_per_second`; latency is the 99th percentile
/// of `timing.intended_to_terminal`, decoded from the histogram here rather than
/// read from a percentile the adapter computed, because the adapter never
/// computes one.
#[must_use]
#[expect(
    clippy::cast_precision_loss,
    reason = "latency percentiles are nanosecond counts far below the f64 integer limit, and a \
              ratio is evidence rather than identity"
)]
pub fn compare(order: &[String], subjects: &[SubjectOutcome]) -> Comparison {
    let mut reasons = Vec::new();
    let ordered: Vec<&SubjectOutcome> = order
        .iter()
        .filter_map(|name| subjects.iter().find(|subject| &subject.name == name))
        .collect();
    if ordered.len() < 2 {
        reasons.push("a comparison needs at least two subjects that ran".to_owned());
    }
    for subject in &ordered {
        if subject.evidence.result.is_none() {
            let what = if subject.evidence.result_present {
                "an unreadable result"
            } else {
                "no result"
            };
            reasons.push(format!(
                "{} produced {what} rather than {PRODUCER_BENCHMARK_V2}",
                subject.name
            ));
        }
    }
    let comparable = reasons.is_empty();
    let mut pairs = Vec::new();
    if comparable {
        if let Some((baseline, candidates)) = ordered.split_first() {
            for candidate in candidates {
                pairs.push(ComparisonPair {
                    baseline: baseline.name.clone(),
                    candidate: candidate.name.clone(),
                    acknowledged_goodput_ratio: ratio(
                        candidate.evidence.goodput(),
                        baseline.evidence.goodput(),
                    ),
                    p99_latency_ratio: ratio(
                        candidate
                            .evidence
                            .intended_to_terminal_p99_ns()
                            .map(|value| value as f64),
                        baseline
                            .evidence
                            .intended_to_terminal_p99_ns()
                            .map(|value| value as f64),
                    ),
                });
            }
        }
    }
    Comparison {
        schema: Comparison::SCHEMA.to_owned(),
        comparable,
        pairs,
        reasons,
    }
}

/// Divides two measurements, refusing anything that would not be a number.
fn ratio(candidate: Option<f64>, baseline: Option<f64>) -> Option<f64> {
    let candidate = candidate?;
    let baseline = baseline?;
    if baseline <= 0.0 || !candidate.is_finite() || !baseline.is_finite() {
        return None;
    }
    Some(candidate / baseline)
}
