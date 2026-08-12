//! Round-trips and the accounting-invariant matrix for the v2 producer
//! measurement document.
#![expect(clippy::unwrap_used, reason = "test assertions may unwrap")]

use crate::experiment::LoadMode;
use crate::histogram::Histogram;
use crate::result_v2::{
    DeclaredExecution, MeasuredThroughput, OfferOutcomes, OfferTiming, ProducerBenchmarkV2,
    QueueObservation,
};

fn distribution(values: &[u64]) -> Histogram {
    let mut histogram = Histogram::new();
    for value in values {
        histogram.record(*value);
    }
    histogram
}

fn fixture(load_mode: LoadMode) -> ProducerBenchmarkV2 {
    let terminals = [1_000_000u64, 2_000_000, 3_000_000];
    let lateness = match load_mode {
        LoadMode::ScheduledOpenLoopFixedRate => Some(distribution(&[10, 20, 30, 40]).encode()),
        LoadMode::ClosedLoop => None,
    };
    ProducerBenchmarkV2 {
        schema: ProducerBenchmarkV2::SCHEMA.to_owned(),
        adapter: "fake-adapter".to_owned(),
        adapter_version: "0.1.0".to_owned(),
        run_id: "0123456789abcdef".to_owned(),
        load_mode,
        declared: DeclaredExecution {
            payload_construction: "prebuilt-pool".to_owned(),
            ownership: "copy-in".to_owned(),
            completion_mode: "public-future".to_owned(),
            serialization: "excluded".to_owned(),
        },
        outcomes: OfferOutcomes {
            offered: 4,
            accepted: 4,
            acknowledged: 3,
            failed: 0,
            timed_out: 0,
            unknown: 1,
        },
        timing: OfferTiming {
            clock: "monotonic-ns".to_owned(),
            intended_to_terminal: distribution(&terminals).encode(),
            accepted_to_terminal: distribution(&terminals).encode(),
            call_start_to_accepted: distribution(&[100, 200, 300, 400]).encode(),
            intended_to_call_start: lateness,
        },
        throughput: MeasuredThroughput {
            measured_duration_ns: 1_000_000_000,
            acknowledged_records_per_second: 3.0,
            acknowledged_payload_bytes_per_second: 3_072.0,
        },
        queue: QueueObservation {
            max_outstanding_observed: 4,
            final_outstanding: 1,
        },
        resources: None,
        native_metrics_path: None,
        valid: true,
        invalid_reason: None,
    }
}

#[test]
fn both_load_modes_round_trip_through_bytes() {
    for load_mode in [LoadMode::ClosedLoop, LoadMode::ScheduledOpenLoopFixedRate] {
        let document = fixture(load_mode);
        document.validate().unwrap();
        let bytes = serde_json::to_vec(&document).unwrap();
        let reparsed = ProducerBenchmarkV2::from_slice(&bytes).unwrap();
        assert_eq!(reparsed, document);
    }
}

#[test]
fn absent_options_are_omitted_from_the_bytes() {
    let bytes = serde_json::to_string(&fixture(LoadMode::ClosedLoop)).unwrap();
    for key in [
        "resources",
        "native_metrics_path",
        "invalid_reason",
        "intended_to_call_start",
    ] {
        assert!(!bytes.contains(key), "{key} must be omitted when absent");
    }
}

#[test]
fn accounting_violations_are_rejected() {
    let mut wrong_schema = fixture(LoadMode::ClosedLoop);
    wrong_schema.schema = "kafkars.producer-benchmark.v1".to_owned();
    assert!(wrong_schema.validate().is_err());

    let mut offered_below_accepted = fixture(LoadMode::ClosedLoop);
    offered_below_accepted.outcomes.offered = 3;
    assert!(offered_below_accepted.validate().is_err());

    let mut broken_terminal_sum = fixture(LoadMode::ClosedLoop);
    broken_terminal_sum.outcomes.unknown = 0;
    assert!(broken_terminal_sum.validate().is_err());

    let mut admission_total_off = fixture(LoadMode::ClosedLoop);
    admission_total_off.timing.call_start_to_accepted = distribution(&[100]).encode();
    assert!(admission_total_off.validate().is_err());

    let mut terminal_total_off = fixture(LoadMode::ClosedLoop);
    terminal_total_off.timing.intended_to_terminal = distribution(&[1]).encode();
    assert!(terminal_total_off.validate().is_err());

    let mut fixed_missing_lateness = fixture(LoadMode::ScheduledOpenLoopFixedRate);
    fixed_missing_lateness.timing.intended_to_call_start = None;
    assert!(fixed_missing_lateness.validate().is_err());

    let mut closed_with_lateness = fixture(LoadMode::ClosedLoop);
    closed_with_lateness.timing.intended_to_call_start = Some(distribution(&[1, 2, 3, 4]).encode());
    assert!(closed_with_lateness.validate().is_err());

    let mut lateness_total_off = fixture(LoadMode::ScheduledOpenLoopFixedRate);
    lateness_total_off.timing.intended_to_call_start = Some(distribution(&[10]).encode());
    assert!(lateness_total_off.validate().is_err());

    let mut wrong_clock = fixture(LoadMode::ClosedLoop);
    wrong_clock.timing.clock = "wall".to_owned();
    assert!(wrong_clock.validate().is_err());

    let mut invalid_without_reason = fixture(LoadMode::ClosedLoop);
    invalid_without_reason.valid = false;
    assert!(invalid_without_reason.validate().is_err());
}
