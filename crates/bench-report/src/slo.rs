//! One measurement judged against one declared objective.
//!
//! # Why this is separate from validity
//!
//! An attempt can be perfectly valid evidence that a subject *missed* its
//! objective — that is the normal outcome at every rate above capacity, and it
//! is a measurement, not a malfunction. So this module answers a different
//! question from `classification.json`: not "may this evidence be believed" but
//! "did the subject do what the experiment asked at this offered rate". The
//! capacity search is the caller that cares, because the answer is exactly the
//! bit it bisects on.
//!
//! # What is checked
//!
//! Four structural conditions, always, and the two latency ceilings when the
//! experiment declares them:
//!
//! - **the adapter declared the measurement valid.** An adapter cannot vouch
//!   for its own validity in general, but it can disqualify itself, and a
//!   subject that says its own numbers are wrong is not meeting an objective;
//! - **every accepted offer reached a terminal** — `unknown == 0`. An offer
//!   with no terminal is neither a success nor a failure, and a rate that
//!   leaves offers in limbo is not a sustained rate;
//! - **the application queue drained** — `final_outstanding == 0`;
//! - **nothing failed or timed out**, see [`MAX_FAILED_RECORDS`];
//! - **p99 offer-to-terminal within `corrected_p99_ms`**, when declared. The
//!   v2 `intended_to_terminal` distribution is measured from the intended offer
//!   time, so it already carries the coordinated-omission correction the field
//!   name refers to, and it includes admission wait;
//! - **p99 scheduler lateness within `schedule_delay_p99_ms`**, when declared
//!   and when the load mode produced a schedule to be late against.
//!
//! # What is not checked, and why it is named here
//!
//! `SloSpec` also declares `drain_tail_ms`, `queue_slope_percent`,
//! `queue_slope_floor_records_per_second`, `minimum_queue_samples`,
//! `native_retries`, and `native_timeouts`. None is evaluated here: the first
//! needs a schedule span that `kafkars.producer-benchmark.v2` does not carry,
//! and the rest need the native statistics stream, which not every subject
//! emits. Judging a subject against an objective on evidence only one subject
//! can produce would make the objective a property of the client rather than of
//! the experiment. These are deferred checks, listed rather than silently
//! skipped.

use bench_schema::{ProducerBenchmarkV2, SloSpec};

use crate::stats::histogram_percentile;

/// Failed or timed-out records an attempt may have and still meet its
/// objectives.
///
/// Zero, and not configurable: `SloSpec` carries no failure budget today, so
/// the strictest reading is the honest one. A relaxation belongs in `SloSpec`
/// as an appended field, where it is hashed into the experiment id and a reader
/// can see that the run was allowed to lose records — never as a default
/// tolerance decided by the reporting layer.
pub const MAX_FAILED_RECORDS: u64 = 0;

/// Nanoseconds in a millisecond; `SloSpec` bounds are whole milliseconds
/// because the experiment document is hashed, and every measurement here is in
/// nanoseconds.
const NANOS_PER_MILLISECOND: u64 = 1_000_000;

/// Whether one measurement met one set of objectives, and what missed.
///
/// `reasons` is empty exactly when `satisfied` is true, and each entry names
/// the observed value beside the bound it broke, so a failing probe is readable
/// without re-reading the measurement.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SloVerdict {
    /// Whether every evaluated objective held.
    pub satisfied: bool,
    /// One reason per objective that did not hold.
    pub reasons: Vec<String>,
}

/// Judges one measurement against one set of objectives.
///
/// Never fails: an undecodable histogram is a *reason*, not an error, because a
/// probe whose latency cannot be read has certainly not demonstrated that it
/// met a latency objective.
#[must_use]
pub fn evaluate_slo(result: &ProducerBenchmarkV2, slo: &SloSpec) -> SloVerdict {
    let mut reasons = Vec::new();

    if !result.valid {
        let detail = result
            .invalid_reason
            .clone()
            .unwrap_or_else(|| "no reason given".to_owned());
        reasons.push(format!(
            "the adapter declared the measurement invalid: {detail}"
        ));
    }
    let failed = result
        .outcomes
        .failed
        .saturating_add(result.outcomes.timed_out);
    if failed > MAX_FAILED_RECORDS {
        reasons.push(format!(
            "{failed} records failed or timed out, above the budget of {MAX_FAILED_RECORDS}"
        ));
    }
    if result.outcomes.unknown != 0 {
        reasons.push(format!(
            "{} accepted offers reached no terminal by the drain deadline",
            result.outcomes.unknown
        ));
    }
    if result.queue.final_outstanding != 0 {
        reasons.push(format!(
            "{} offers were still outstanding when the run ended",
            result.queue.final_outstanding
        ));
    }

    if let Some(bound_ms) = slo.corrected_p99_ms {
        check_percentile(
            &mut reasons,
            "p99 offer-to-terminal",
            histogram_percentile(&result.timing.intended_to_terminal, 0.99),
            bound_ms,
        );
    }
    if let Some(bound_ms) = slo.schedule_delay_p99_ms {
        match &result.timing.intended_to_call_start {
            Some(lateness) => check_percentile(
                &mut reasons,
                "p99 scheduler lateness",
                histogram_percentile(lateness, 0.99),
                bound_ms,
            ),
            None => reasons.push(
                "a schedule-delay objective was declared but the measurement carries no \
                 scheduler lateness distribution"
                    .to_owned(),
            ),
        }
    }

    SloVerdict {
        satisfied: reasons.is_empty(),
        reasons,
    }
}

/// Pushes a reason when a percentile is missing, unreadable, or over budget.
fn check_percentile(
    reasons: &mut Vec<String>,
    label: &str,
    observed: crate::error::ReportResult<Option<u64>>,
    bound_ms: u64,
) {
    let bound_ns = bound_ms.saturating_mul(NANOS_PER_MILLISECOND);
    match observed {
        Ok(Some(value)) if value <= bound_ns => {}
        Ok(Some(value)) => reasons.push(format!(
            "{label} {value} ns exceeds the {bound_ms} ms objective ({bound_ns} ns)"
        )),
        Ok(None) => reasons.push(format!(
            "{label} has no recorded values to judge against the {bound_ms} ms objective"
        )),
        Err(error) => reasons.push(format!("{label} could not be read: {error}")),
    }
}
