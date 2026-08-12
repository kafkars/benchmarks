//! The emitted document must satisfy the schema's own accounting, in both
//! load modes, when read back exactly as the control plane reads it.
#![expect(clippy::unwrap_used, reason = "test fixtures are exact")]

use bench_schema::{LoadMode, ProducerBenchmarkV2};

use super::admission::AdmissionClock;
use super::document::{
    COMPLETION_MODE, DocumentRequest, OWNERSHIP, PAYLOAD_CONSTRUCTION, SERIALIZATION, build,
};
use super::measurement::{Measurement, OfferAttempt, OfferGroup, Terminal};
use super::outstanding::OutstandingGauge;

const RUN_ID: &str = "0123456789abcdef";
const RATE: u64 = 1_000;

/// One phase's worth of evidence: `groups` batches of `count` offers each,
/// every one acknowledged.
fn measured(
    mut measurement: Measurement,
    groups: u64,
    count: u64,
) -> (Measurement, OutstandingGauge) {
    let outstanding = OutstandingGauge::default();
    for index in 0..groups {
        let call_start_ns = 1_000_000 * (index + 1);
        let group = OfferGroup {
            attempt: OfferAttempt {
                first_sequence: index * count,
                count,
                admission: AdmissionClock::start(call_start_ns),
            },
            accepted_ns: call_start_ns + 20_000,
        };
        measurement.record_offered(&group.attempt).unwrap();
        measurement.record_accepted(&group);
        outstanding.admitted(count);
        measurement
            .record_terminals(
                &group,
                call_start_ns + 900_000,
                std::iter::repeat_n(Terminal::Acknowledged, usize::try_from(count).unwrap()),
            )
            .unwrap();
        outstanding.settled(count);
    }
    (measurement, outstanding)
}

/// Serializes exactly as the protocol writes it, then reads it back exactly as
/// the control plane does.
fn round_trip(document: &ProducerBenchmarkV2) -> ProducerBenchmarkV2 {
    let line = format!("{}\n", serde_json::to_string(document).unwrap());
    assert_eq!(
        line.matches('\n').count(),
        1,
        "one line, one newline: {line}"
    );
    ProducerBenchmarkV2::from_slice(line.as_bytes()).unwrap()
}

#[test]
fn a_closed_loop_measurement_round_trips_with_its_invariants_intact() {
    let (measurement, outstanding) = measured(Measurement::closed_loop(), 4, 250);

    let document = build(&DocumentRequest {
        run_id: RUN_ID,
        load_mode: LoadMode::ClosedLoop,
        payload_bytes: 1_024,
        expected_records: 1_000,
        measured_duration_ns: 2_000_000_000,
        measurement: &measurement,
        outstanding: &outstanding,
    })
    .unwrap();
    let parsed = round_trip(&document);

    assert_eq!(parsed.schema, "kafkars.producer-benchmark.v2");
    assert_eq!(parsed.adapter, "kafkars");
    assert_eq!(parsed.run_id, RUN_ID);
    assert_eq!(parsed.load_mode, LoadMode::ClosedLoop);
    assert!(parsed.valid, "{:?}", parsed.invalid_reason);
    assert_eq!(parsed.outcomes.offered, 1_000);
    assert_eq!(parsed.outcomes.accepted, 1_000);
    assert_eq!(parsed.outcomes.acknowledged, 1_000);
    assert_eq!(parsed.outcomes.unknown, 0);
    assert!(
        parsed.timing.intended_to_call_start.is_none(),
        "a closed-loop document must not carry scheduler lateness"
    );
    assert_eq!(parsed.timing.call_start_to_accepted.total, 1_000);
    assert_eq!(parsed.timing.intended_to_terminal.total, 1_000);
    assert_eq!(parsed.throughput.measured_duration_ns, 2_000_000_000);
    assert!(
        (parsed.throughput.acknowledged_records_per_second - 500.0).abs() < 1e-9,
        "{}",
        parsed.throughput.acknowledged_records_per_second
    );
    assert!((parsed.throughput.acknowledged_payload_bytes_per_second - 512_000.0).abs() < 1e-6);
    assert_eq!(parsed.queue.max_outstanding_observed, 250);
    assert_eq!(parsed.queue.final_outstanding, 0);
}

#[test]
fn a_fixed_rate_measurement_round_trips_with_its_lateness() {
    let (measurement, outstanding) = measured(Measurement::scheduled(RATE), 4, 250);

    let document = build(&DocumentRequest {
        run_id: RUN_ID,
        load_mode: LoadMode::ScheduledOpenLoopFixedRate,
        payload_bytes: 1_024,
        expected_records: 1_000,
        measured_duration_ns: 1_000_000_000,
        measurement: &measurement,
        outstanding: &outstanding,
    })
    .unwrap();
    let parsed = round_trip(&document);

    assert_eq!(parsed.load_mode, LoadMode::ScheduledOpenLoopFixedRate);
    assert!(parsed.valid, "{:?}", parsed.invalid_reason);
    let lateness = parsed.timing.intended_to_call_start.as_ref().unwrap();
    assert_eq!(
        lateness.total, parsed.outcomes.offered,
        "every offered record has a due time it was early or late against"
    );
}

#[test]
fn the_declared_execution_says_what_the_measured_path_actually_did() {
    let (measurement, outstanding) = measured(Measurement::closed_loop(), 1, 4);

    let document = build(&DocumentRequest {
        run_id: RUN_ID,
        load_mode: LoadMode::ClosedLoop,
        payload_bytes: 64,
        expected_records: 4,
        measured_duration_ns: 1_000,
        measurement: &measurement,
        outstanding: &outstanding,
    })
    .unwrap();

    assert_eq!(document.declared.payload_construction, PAYLOAD_CONSTRUCTION);
    assert_eq!(
        document.declared.payload_construction,
        "prebuilt-pool-per-offer-sequence"
    );
    assert_eq!(document.declared.ownership, OWNERSHIP);
    assert_eq!(
        document.declared.ownership, "owned-per-offer-from-pool",
        "the client takes ownership, so the pool saves the computation and not the copy"
    );
    assert_eq!(document.declared.completion_mode, COMPLETION_MODE);
    assert_eq!(
        document.declared.completion_mode, "aggregate-batch-terminal",
        "this is the completion shape the capability document already claims"
    );
    assert_eq!(document.declared.serialization, SERIALIZATION);
    assert_eq!(document.declared.serialization, "excluded");
    assert_eq!(document.timing.clock, "monotonic-ns");
    assert_eq!(
        document.native_metrics_path, None,
        "this adapter emits no statistics file to point at"
    );
    assert_eq!(document.resources, None, "process resources are deferred");
}

#[test]
fn undrained_offers_are_unknown_and_stay_visible_as_outstanding() {
    let mut measurement = Measurement::closed_loop();
    let group = OfferGroup {
        attempt: OfferAttempt {
            first_sequence: 0,
            count: 10,
            admission: AdmissionClock::start(1_000),
        },
        accepted_ns: 2_000,
    };
    measurement.record_offered(&group.attempt).unwrap();
    measurement.record_accepted(&group);
    measurement.record_unknown(10);
    let outstanding = OutstandingGauge::default();
    outstanding.admitted(10);

    let document = build(&DocumentRequest {
        run_id: RUN_ID,
        load_mode: LoadMode::ClosedLoop,
        payload_bytes: 64,
        expected_records: 10,
        measured_duration_ns: 1_000_000,
        measurement: &measurement,
        outstanding: &outstanding,
    })
    .unwrap();
    let parsed = round_trip(&document);

    assert!(!parsed.valid);
    assert_eq!(parsed.outcomes.unknown, 10);
    assert_eq!(parsed.queue.final_outstanding, 10);
    assert_eq!(
        parsed.timing.intended_to_terminal.total, 0,
        "an offer with no terminal contributes no terminal latency"
    );
    assert_eq!(
        parsed.timing.call_start_to_accepted.total, 10,
        "it was still accepted, and its admission wait is still evidence"
    );
    let reason = parsed.invalid_reason.unwrap();
    assert!(reason.contains("10 unknown"), "{reason}");
}

#[test]
fn a_wholly_refused_batch_is_offered_never_accepted_and_still_reads_back() {
    // The client turned the whole group away, so the public call happened and
    // the acceptance did not. `offered` has to show the attempt — an `offered`
    // that only counts what was accepted cannot report a refusal at all — and
    // every total still has to line up, because the document a failed run
    // leaves behind is the only place a reader finds out what it failed at.
    let mut measurement = Measurement::scheduled(RATE);
    let refused = OfferAttempt {
        first_sequence: 0,
        count: 256,
        admission: AdmissionClock::start(5_000_000),
    };
    measurement.record_offered(&refused).unwrap();
    measurement.record_refusal();
    let outstanding = OutstandingGauge::default();

    let document = build(&DocumentRequest {
        run_id: RUN_ID,
        load_mode: LoadMode::ScheduledOpenLoopFixedRate,
        payload_bytes: 1_024,
        expected_records: 256,
        measured_duration_ns: 10_000_000,
        measurement: &measurement,
        outstanding: &outstanding,
    })
    .unwrap();
    let parsed = round_trip(&document);

    assert_eq!(parsed.outcomes.offered, 256, "the public call began");
    assert_eq!(parsed.outcomes.accepted, 0);
    assert_eq!(
        parsed.timing.call_start_to_accepted.total, 0,
        "no admission wait exists for an admission that never completed"
    );
    assert_eq!(
        parsed.timing.intended_to_call_start.as_ref().unwrap().total,
        256,
        "the schema requires one lateness sample per offered record, and a \
         refused offer is an offered record"
    );
    assert!(!parsed.valid);
    let reason = parsed.invalid_reason.as_deref().unwrap();
    assert!(reason.contains("accepted 0 of 256"), "{reason}");
    assert!(
        reason.contains("2 public admission attempts"),
        "the refusal is visible as a second attempt: {reason}"
    );
}

#[test]
fn a_short_phase_is_invalid_and_says_how_short() {
    let (measurement, outstanding) = measured(Measurement::closed_loop(), 1, 100);

    let document = build(&DocumentRequest {
        run_id: RUN_ID,
        load_mode: LoadMode::ClosedLoop,
        payload_bytes: 64,
        expected_records: 1_000,
        measured_duration_ns: 1_000_000,
        measurement: &measurement,
        outstanding: &outstanding,
    })
    .unwrap();

    assert!(!document.valid);
    let reason = document.invalid_reason.clone().unwrap();
    assert!(reason.contains("offered 100 of 1000"), "{reason}");
    // An invalid measurement is still a well-formed document: the control
    // plane must be able to read why a subject failed, not just that it did.
    ProducerBenchmarkV2::from_slice(
        format!("{}\n", serde_json::to_string(&document).unwrap()).as_bytes(),
    )
    .unwrap();
}

#[test]
fn a_zero_length_interval_reports_zero_goodput_rather_than_infinity() {
    let (measurement, outstanding) = measured(Measurement::closed_loop(), 1, 4);

    let document = build(&DocumentRequest {
        run_id: RUN_ID,
        load_mode: LoadMode::ClosedLoop,
        payload_bytes: 64,
        expected_records: 4,
        measured_duration_ns: 0,
        measurement: &measurement,
        outstanding: &outstanding,
    })
    .unwrap();
    let line = serde_json::to_string(&document).unwrap();

    assert!(
        !line.contains("null,") || !line.contains("per_second\":null"),
        "an infinite rate would serialize as null and fail to read back: {line}"
    );
    let parsed = round_trip(&document);
    assert!((parsed.throughput.acknowledged_records_per_second - 0.0).abs() < f64::EPSILON);
}
