//! What the C adapter accepts, and every reason it gives for declining.
//!
//! Each test mutates exactly one field of the shared fixture, because a
//! validate report is read by a person asking why a subject is missing from a
//! comparison, and a reason that names the wrong field is worse than no reason.
#![expect(clippy::unwrap_used, reason = "test fixtures are exact")]

use bench_schema::{ArrivalModel, LoadMode, ResolvedExperiment, ValidateReport};

use crate::fixture::experiment;
use crate::validate::report;

/// One field mutation applied to the shared fixture.
type Mutation = Box<dyn Fn(&mut ResolvedExperiment)>;

fn declined(mutate: impl FnOnce(&mut ResolvedExperiment)) -> ValidateReport {
    let mut document = experiment(LoadMode::ClosedLoop);
    mutate(&mut document);
    report(&document, Some("librdkafka-c"))
}

fn reasons_mentioning(report: &ValidateReport, needle: &str) -> bool {
    report.reasons.iter().any(|reason| reason.contains(needle))
}

#[test]
fn the_balanced_scenario_is_supported_in_both_load_modes() {
    for load_mode in [LoadMode::ClosedLoop, LoadMode::ScheduledOpenLoopFixedRate] {
        let verdict = report(&experiment(load_mode), Some("librdkafka-c"));

        assert!(
            verdict.supported,
            "{load_mode:?} was declined: {:?}",
            verdict.reasons
        );
        assert!(verdict.reasons.is_empty());
        assert!(verdict.has_expected_schema());
    }
}

#[test]
fn an_incoherent_experiment_is_declined_before_anything_else_is_read() {
    let verdict = declined(|document| document.records = 0);

    assert!(!verdict.supported);
    assert_eq!(verdict.reasons.len(), 1, "{:?}", verdict.reasons);
    assert!(reasons_mentioning(&verdict, "not coherent"));
}

#[test]
fn a_payload_below_the_identity_envelope_is_declined() {
    let verdict = declined(|document| document.payload.bytes = 32);

    assert!(!verdict.supported);
    assert!(reasons_mentioning(&verdict, "64 payload bytes"));
}

#[test]
fn more_outstanding_records_than_the_client_queue_holds_is_declined() {
    let verdict = declined(|document| document.application.max_outstanding_records = 100_001);

    assert!(!verdict.supported);
    assert!(reasons_mentioning(&verdict, "100000 records"));
}

#[test]
fn a_different_queue_budget_is_declined() {
    let verdict = declined(|document| document.application.queue_bytes = 1_024);

    assert!(!verdict.supported);
    assert!(reasons_mentioning(&verdict, "client queue"));
}

#[test]
fn the_closed_loop_shape_admits_from_one_caller_only() {
    let verdict = declined(|document| document.application.callers_per_producer = 4);

    assert!(!verdict.supported);
    assert!(reasons_mentioning(&verdict, "closed-loop phase"));
}

#[test]
fn the_fixed_rate_shape_demands_exactly_four_callers() {
    let mut document = experiment(LoadMode::ScheduledOpenLoopFixedRate);
    document.application.callers_per_producer = 8;

    let verdict = report(&document, Some("librdkafka-c"));

    assert!(!verdict.supported);
    assert!(reasons_mentioning(&verdict, "exactly 4 callers"));
}

#[test]
fn an_offered_rate_above_a_billion_is_declined() {
    let mut document = experiment(LoadMode::ScheduledOpenLoopFixedRate);
    document.offered_records_per_second = Some(1_000_000_001);

    let verdict = report(&document, Some("librdkafka-c"));

    assert!(!verdict.supported);
    assert!(reasons_mentioning(
        &verdict,
        "1000000000 records per second"
    ));
}

#[test]
fn a_poisson_arrival_process_is_declined() {
    let mut document = experiment(LoadMode::ScheduledOpenLoopFixedRate);
    document.arrival = Some(ArrivalModel::Poisson);

    let verdict = report(&document, Some("librdkafka-c"));

    assert!(!verdict.supported);
    assert!(reasons_mentioning(&verdict, "evenly spaced arrivals"));
}

#[test]
fn every_hard_coded_client_setting_is_checked() {
    let cases: Vec<(&str, Mutation)> = vec![
        (
            "acks",
            Box::new(|document: &mut ResolvedExperiment| {
                document.producer.as_mut().unwrap().acks = "1".to_owned();
            }),
        ),
        (
            "enable.idempotence",
            Box::new(|document: &mut ResolvedExperiment| {
                document.producer.as_mut().unwrap().idempotence = false;
            }),
        ),
        (
            "compression.type",
            Box::new(|document: &mut ResolvedExperiment| {
                document.producer.as_mut().unwrap().compression = "gzip".to_owned();
            }),
        ),
        (
            "linger.ms",
            Box::new(|document: &mut ResolvedExperiment| {
                document.producer.as_mut().unwrap().linger_ms = 20;
            }),
        ),
        (
            "batch.num.messages",
            Box::new(|document: &mut ResolvedExperiment| {
                document.producer.as_mut().unwrap().batch_records = 512;
            }),
        ),
        (
            "batch.size",
            Box::new(|document: &mut ResolvedExperiment| {
                document.producer.as_mut().unwrap().batch_bytes = 131_072;
            }),
        ),
        (
            "message.timeout.ms",
            Box::new(|document: &mut ResolvedExperiment| {
                document.producer.as_mut().unwrap().delivery_timeout_ms = 30_000;
            }),
        ),
        (
            "max.in.flight.requests.per.connection",
            Box::new(|document: &mut ResolvedExperiment| {
                document
                    .producer
                    .as_mut()
                    .unwrap()
                    .max_in_flight_requests_per_broker = 1;
            }),
        ),
        (
            "message.send.max.retries",
            Box::new(|document: &mut ResolvedExperiment| {
                document.producer.as_mut().unwrap().retry_max_replacements = 3;
            }),
        ),
        (
            "retry.backoff.ms",
            Box::new(|document: &mut ResolvedExperiment| {
                document.producer.as_mut().unwrap().retry_backoff_ms = 250;
            }),
        ),
    ];

    for (setting, mutate) in cases {
        let verdict = declined(mutate);

        assert!(!verdict.supported, "{setting} was accepted");
        assert!(
            reasons_mentioning(&verdict, setting),
            "{setting} was declined for the wrong reason: {:?}",
            verdict.reasons
        );
    }
}

#[test]
fn a_partitioning_the_c_adapter_does_not_implement_is_declined() {
    let verdict = declined(|document| {
        document.producer.as_mut().unwrap().partitioning = "sticky".to_owned();
    });

    assert!(!verdict.supported);
    assert!(reasons_mentioning(&verdict, "round robin"));
}

#[test]
fn a_warmup_primer_that_is_not_one_record_per_partition_is_declined() {
    let verdict = declined(|document| {
        document
            .producer
            .as_mut()
            .unwrap()
            .warmup_serialized_partition_primer_records = Some(3);
    });

    assert!(!verdict.supported);
    assert!(reasons_mentioning(&verdict, "one record per partition"));
}

#[test]
fn an_experiment_with_no_runtime_binding_is_declined() {
    let verdict = declined(|document| document.runtime = None);

    assert!(!verdict.supported);
    assert!(reasons_mentioning(&verdict, "no runtime binding"));
}

#[test]
fn an_experiment_that_names_no_topics_for_this_subject_is_declined() {
    let mut document = experiment(LoadMode::ClosedLoop);

    let verdict = report(&document, Some("sarama"));

    assert!(!verdict.supported);
    assert!(reasons_mentioning(&verdict, "sarama"));
    document.runtime = None;
}

#[test]
fn request_bytes_are_not_checked_because_the_c_adapter_never_sets_them() {
    let verdict = declined(|document| {
        document.producer.as_mut().unwrap().request_bytes = 8_388_608;
    });

    assert!(
        verdict.supported,
        "the C adapter has no message.max.bytes setting to disagree with: {:?}",
        verdict.reasons
    );
}
