//! What the four histograms and six counters say about a phase.
#![expect(clippy::unwrap_used, reason = "test fixtures are exact")]

use bench_schema::Histogram;

use super::admission::AdmissionClock;
use super::measurement::{Measurement, OfferGroup, Terminal};

/// A group of `count` offers whose admission began at `call_start_ns`.
fn group(first_sequence: u64, count: u64, call_start_ns: u64, accepted_ns: u64) -> OfferGroup {
    OfferGroup {
        first_sequence,
        count,
        admission: AdmissionClock::start(call_start_ns),
        accepted_ns,
    }
}

/// The same group after `retries` queue-full refusals.
fn retried_group(
    first_sequence: u64,
    count: u64,
    call_start_ns: u64,
    accepted_ns: u64,
    retries: u32,
) -> OfferGroup {
    let mut admission = AdmissionClock::start(call_start_ns);
    for _ in 0..retries {
        admission.rejected();
    }
    OfferGroup {
        first_sequence,
        count,
        admission,
        accepted_ns,
    }
}

fn decoded(encoded: &bench_schema::EncodedHistogram) -> Histogram {
    Histogram::decode(encoded).unwrap()
}

#[test]
fn the_admission_wait_a_retried_offer_reports_includes_the_backpressure() {
    // Two offers, first attempt at 1_000, refused twice, taken at 9_000.
    let mut measurement = Measurement::closed_loop();
    let retried = retried_group(0, 2, 1_000, 9_000, 2);
    measurement.record_offered(&retried).unwrap();
    measurement.record_accepted(&retried);

    let timing = measurement.timing();
    let wait = decoded(&timing.call_start_to_accepted);

    assert_eq!(wait.total(), 2, "one recording per offer, not per attempt");
    assert_eq!(
        wait.min(),
        Some(8_000),
        "the wait spans the refused attempts, not just the accepted one"
    );
    assert_eq!(wait.max(), Some(8_000));
    assert_eq!(
        measurement.admission_attempts(),
        3,
        "the refusals stay visible as attempts"
    );
}

#[test]
fn a_closed_loop_offer_has_no_schedule_and_no_lateness() {
    let mut measurement = Measurement::closed_loop();
    let offered = group(0, 4, 5_000, 5_200);
    measurement.record_offered(&offered).unwrap();
    measurement.record_accepted(&offered);
    measurement
        .record_terminals(&offered, 9_200, [Terminal::Acknowledged; 4].into_iter())
        .unwrap();

    let timing = measurement.timing();

    assert!(
        timing.intended_to_call_start.is_none(),
        "a closed-loop phase has no schedule to be late against"
    );
    let end_to_end = decoded(&timing.intended_to_terminal);
    assert_eq!(end_to_end.total(), 4);
    assert_eq!(
        end_to_end.min(),
        Some(4_200),
        "intended is call_start, so end-to-end spans admission and delivery"
    );
    let after_accept = decoded(&timing.accepted_to_terminal);
    assert_eq!(after_accept.min(), Some(4_000));
}

#[test]
fn a_scheduled_offer_reports_lateness_per_record() {
    // At 1000 records per second a record is due every 1_000_000ns, so the
    // group's records are due at 0 and 1_000_000. Both were offered at
    // 3_000_000.
    let mut measurement = Measurement::scheduled(1_000);
    let offered = group(0, 2, 3_000_000, 3_000_100);
    measurement.record_offered(&offered).unwrap();
    measurement.record_accepted(&offered);

    let timing = measurement.timing();
    let lateness = decoded(timing.intended_to_call_start.as_ref().unwrap());

    assert_eq!(lateness.total(), 2, "every offered record has a due time");
    assert_eq!(lateness.max(), Some(3_000_000), "record 0 was due at zero");
    assert_eq!(
        lateness.min(),
        Some(2_000_000),
        "record 1 was due a millisecond later, so it is a millisecond less late"
    );
}

#[test]
fn terminals_are_counted_by_kind_and_every_one_lands_in_both_histograms() {
    let mut measurement = Measurement::closed_loop();
    let offered = group(10, 3, 1_000, 2_000);
    measurement.record_offered(&offered).unwrap();
    measurement.record_accepted(&offered);
    measurement
        .record_terminals(
            &offered,
            6_000,
            [Terminal::Acknowledged, Terminal::Failed, Terminal::TimedOut].into_iter(),
        )
        .unwrap();
    measurement.record_unknown(2);

    let outcomes = measurement.outcomes();
    let timing = measurement.timing();

    assert_eq!(outcomes.offered, 3);
    assert_eq!(outcomes.accepted, 3);
    assert_eq!(outcomes.acknowledged, 1);
    assert_eq!(outcomes.failed, 1);
    assert_eq!(outcomes.timed_out, 1);
    assert_eq!(outcomes.unknown, 2);
    assert_eq!(
        decoded(&timing.intended_to_terminal).total(),
        3,
        "a failed offer still reached a terminal and still has a latency"
    );
    assert_eq!(decoded(&timing.accepted_to_terminal).total(), 3);
}

#[test]
fn a_terminal_count_that_disagrees_with_the_group_is_refused() {
    let mut measurement = Measurement::closed_loop();
    let offered = group(0, 4, 1_000, 2_000);

    let error = measurement
        .record_terminals(&offered, 3_000, [Terminal::Acknowledged; 3].into_iter())
        .unwrap_err()
        .to_string();

    assert!(error.contains("carried 3 terminals"), "{error}");
}

#[test]
fn merging_callers_sums_outcomes_and_unions_histograms() {
    let mut left = Measurement::scheduled(1_000);
    let first = group(0, 1, 1_000, 1_500);
    left.record_offered(&first).unwrap();
    left.record_accepted(&first);
    left.record_terminals(&first, 4_000, [Terminal::Acknowledged].into_iter())
        .unwrap();

    let mut right = Measurement::scheduled(1_000);
    let second = group(1, 1, 2_000, 2_900);
    right.record_offered(&second).unwrap();
    right.record_accepted(&second);
    right
        .record_terminals(&second, 9_000, [Terminal::Failed].into_iter())
        .unwrap();

    let mut merged = Measurement::scheduled(1_000);
    merged.merge(&left);
    merged.merge(&right);

    let outcomes = merged.outcomes();
    assert_eq!(outcomes.offered, 2);
    assert_eq!(outcomes.accepted, 2);
    assert_eq!(outcomes.acknowledged, 1);
    assert_eq!(outcomes.failed, 1);
    let timing = merged.timing();
    assert_eq!(decoded(&timing.call_start_to_accepted).total(), 2);
    assert_eq!(
        decoded(timing.intended_to_call_start.as_ref().unwrap()).total(),
        2,
        "lateness survives the merge, once per offered record"
    );
}
