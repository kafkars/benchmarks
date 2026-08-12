//! Reading subject evidence: what a bundle holds, and what it means when a
//! document is missing, unreadable, or self-contradicting.
//!
//! Every result fixture here is a real `kafkars.producer-benchmark.v2` document
//! built through the schema types, because the parse is half the gate: a
//! document whose offers do not add up is unreadable by construction. This
//! module also hosts those fixtures for the sibling test modules, because every
//! one of them needs the same shape and a second copy would drift.
#![expect(
    clippy::unwrap_used,
    reason = "an evidence fixture that cannot be written must fail the test immediately"
)]

use bench_schema::{
    AdapterOutcome, DeclaredExecution, EncodedHistogram, Histogram, LoadMode, MeasuredThroughput,
    OfferOutcomes, OfferTiming, ProcessExit, ProducerBenchmarkV2, QueueObservation, SloSpec,
    SubjectVerification,
};

use crate::attempt::AttemptPaths;
use crate::results::{SubjectOutcome, VerificationVerdict, read_subject};
use crate::seal_test::workspace;

/// Records for every fixture measurement below.
pub(super) const RECORDS: u64 = 1_000;

/// A process exit that says nothing went wrong.
pub(super) fn clean_exit() -> ProcessExit {
    ProcessExit {
        exit_code: Some(0),
        signal: None,
        timed_out: false,
        duration_ms: 12,
    }
}

/// Writes an adapter's two documents into the bundle.
fn write_adapter_output(paths: &AttemptPaths, subject: &str, status: &str, result: &str) {
    std::fs::create_dir_all(paths.adapter_dir(subject)).unwrap();
    if !status.is_empty() {
        std::fs::write(paths.adapter_status_json(subject), status).unwrap();
    }
    if !result.is_empty() {
        std::fs::write(paths.adapter_result_json(subject), result).unwrap();
    }
}

/// A histogram of `count` recordings of exactly `value`.
///
/// Every percentile of such a histogram is `value` itself: the bucket's upper
/// bound is clamped to the exact recorded maximum, so a single-valued sample
/// reports the value it holds rather than the width of its bucket.
pub(super) fn flat(count: u64, value: u64) -> EncodedHistogram {
    let mut histogram = Histogram::new();
    for _ in 0..count {
        histogram.record(value);
    }
    histogram.encode()
}

/// A closed-loop v2 measurement declaring `goodput` and a p99 of `p99`.
pub(super) fn measurement(goodput: f64, p99: u64) -> ProducerBenchmarkV2 {
    ProducerBenchmarkV2 {
        schema: ProducerBenchmarkV2::SCHEMA.to_owned(),
        adapter: "fake".to_owned(),
        adapter_version: "0.1.0".to_owned(),
        run_id: "0123456789abcdef".to_owned(),
        load_mode: LoadMode::ClosedLoop,
        declared: DeclaredExecution {
            payload_construction: "prebuilt-pool-per-offer-sequence".to_owned(),
            ownership: "copy-in".to_owned(),
            completion_mode: "aggregate-batch-terminal".to_owned(),
            serialization: "excluded".to_owned(),
        },
        outcomes: OfferOutcomes {
            offered: RECORDS,
            accepted: RECORDS,
            acknowledged: RECORDS,
            failed: 0,
            timed_out: 0,
            unknown: 0,
        },
        timing: OfferTiming {
            clock: "monotonic-ns".to_owned(),
            intended_to_terminal: flat(RECORDS, p99),
            accepted_to_terminal: flat(RECORDS, p99 / 2),
            call_start_to_accepted: flat(RECORDS, 10_000),
            intended_to_call_start: None,
        },
        throughput: MeasuredThroughput {
            measured_duration_ns: 1_000_000_000,
            acknowledged_records_per_second: goodput,
            acknowledged_payload_bytes_per_second: goodput * 1_024.0,
        },
        queue: QueueObservation {
            max_outstanding_observed: 256,
            final_outstanding: 0,
        },
        resources: None,
        native_metrics_path: None,
        valid: true,
        invalid_reason: None,
    }
}

/// The rendered bytes of a v2 measurement.
fn result_document(goodput: f64, p99: u64) -> String {
    render(&measurement(goodput, p99))
}

/// Renders a v2 measurement the way an adapter would.
fn render(document: &ProducerBenchmarkV2) -> String {
    String::from_utf8(bench_schema::pretty_bytes(document).unwrap()).unwrap()
}

/// A succeeded adapter status document.
fn status_document() -> &'static str {
    r#"{"schema":"kafkars.adapter-status.v1","outcome":"succeeded",
    "started_at":"2026-08-12T14:03:05.000Z","finished_at":"2026-08-12T14:03:06.000Z"}"#
}

/// A subject whose evidence is on disk, built from an arbitrary measurement.
pub(super) fn subject_with(name: &str, document: &ProducerBenchmarkV2) -> SubjectOutcome {
    let (results_root, paths) = workspace(&format!("evidence-{name}"));
    write_adapter_output(&paths, name, status_document(), &render(document));
    let evidence = read_subject(&paths, name);
    std::fs::remove_dir_all(&results_root).unwrap();
    SubjectOutcome {
        name: name.to_owned(),
        execution: Some(clean_exit()),
        interrupted: false,
        evidence,
        verification: SubjectVerification::default(),
        measured: VerificationVerdict::Satisfied,
        warmup: VerificationVerdict::Satisfied,
        slo: SloSpec::default(),
        declared_adapter_version: "0.1.0".to_owned(),
    }
}

/// A subject that did everything right.
pub(super) fn healthy(name: &str, goodput: f64, p99: u64) -> SubjectOutcome {
    subject_with(name, &measurement(goodput, p99))
}

/// A measurement whose latency histogram claims a bucket no `u64` can reach.
///
/// `8_320` is the first index whose scale is 64: decoding it used to shift a
/// `u64` by its own width. The document is otherwise a perfectly ordinary
/// measurement, which is the point — the hostile part is one integer.
pub(super) fn measurement_with_an_impossible_bucket() -> ProducerBenchmarkV2 {
    let mut document = measurement(100_000.0, 1_000_000);
    document.timing.intended_to_terminal.counts = vec![(8_320, RECORDS)];
    document
}

#[test]
fn reading_a_subject_derives_its_headline_numbers_from_the_v2_document() {
    let (results_root, paths) = workspace("read");
    write_adapter_output(
        &paths,
        "only",
        status_document(),
        &result_document(1234.5, 990_000),
    );
    let evidence = read_subject(&paths, "only");
    assert!(evidence.result_present);
    assert!(evidence.notes.is_empty(), "{:?}", evidence.notes);
    assert_eq!(evidence.adapter_outcome(), Some(AdapterOutcome::Succeeded));
    assert_eq!(evidence.goodput(), Some(1234.5));
    assert_eq!(
        evidence.intended_to_terminal_p99_ns(),
        Some(990_000),
        "the percentile is derived from the histogram, never read from the document"
    );
    let result = evidence.result.unwrap();
    assert!(result.valid);
    assert_eq!(result.outcomes.acknowledged, RECORDS);
    std::fs::remove_dir_all(&results_root).unwrap();
}

#[test]
fn a_v1_result_document_is_no_longer_a_producer_result() {
    let (results_root, paths) = workspace("read-v1");
    write_adapter_output(
        &paths,
        "legacy",
        status_document(),
        r#"{"schema":"kafkars.producer-benchmark.v1","adapter":"fake","valid":true,
        "acknowledged_records_per_second":100000.0,"latency_ns":{"p99":1000000}}"#,
    );
    let evidence = read_subject(&paths, "legacy");
    assert!(evidence.result_present);
    assert!(evidence.result.is_none(), "the v1 path is gone");
    assert_eq!(evidence.notes.len(), 1, "{:?}", evidence.notes);
    let outcome = SubjectOutcome {
        evidence,
        ..healthy("legacy", 1.0, 1)
    };
    let validity = outcome.validity();
    assert!(!validity.valid);
    assert!(
        validity
            .reasons
            .iter()
            .any(|reason| reason.contains("kafkars.producer-benchmark.v2")),
        "{:?}",
        validity.reasons
    );
    std::fs::remove_dir_all(&results_root).unwrap();
}

#[test]
fn a_result_whose_offers_do_not_add_up_is_unreadable() {
    let mut document = measurement(100_000.0, 1_000_000);
    document.outcomes.acknowledged = RECORDS - 1;
    let (results_root, paths) = workspace("read-incoherent");
    write_adapter_output(&paths, "only", status_document(), &render(&document));
    let evidence = read_subject(&paths, "only");
    assert!(evidence.result_present);
    assert!(evidence.result.is_none());
    assert!(
        evidence
            .result_error
            .is_some_and(|error| error.contains("accepted")),
        "the accounting invariant names the field it broke"
    );
    std::fs::remove_dir_all(&results_root).unwrap();
}

#[test]
fn a_missing_document_is_recorded_rather_than_fatal() {
    let (results_root, paths) = workspace("read-missing");
    let evidence = read_subject(&paths, "absent");
    assert!(!evidence.result_present);
    assert_eq!(evidence.adapter_outcome(), None);
    assert!(evidence.notes.is_empty(), "an absent file is not a note");
    std::fs::remove_dir_all(&results_root).unwrap();
}

#[test]
fn an_unreadable_document_is_noted_and_the_subject_stays_invalid() {
    let (results_root, paths) = workspace("read-garbage");
    write_adapter_output(&paths, "only", "not json", "also not json");
    let evidence = read_subject(&paths, "only");
    assert_eq!(evidence.notes.len(), 2, "{:?}", evidence.notes);
    assert!(
        evidence.result_present,
        "the file exists; it is just unreadable"
    );
    let outcome = SubjectOutcome {
        name: "only".to_owned(),
        execution: Some(clean_exit()),
        interrupted: false,
        evidence,
        verification: SubjectVerification::default(),
        measured: VerificationVerdict::Satisfied,
        warmup: VerificationVerdict::Satisfied,
        slo: SloSpec::default(),
        declared_adapter_version: "0.1.0".to_owned(),
    };
    let validity = outcome.validity();
    assert!(!validity.valid);
    assert!(
        validity
            .reasons
            .iter()
            .any(|reason| reason.contains("no readable status")),
        "{:?}",
        validity.reasons
    );
    std::fs::remove_dir_all(&results_root).unwrap();
}

#[test]
fn a_result_naming_an_impossible_bucket_is_unreadable_rather_than_fatal() {
    let (results_root, paths) = workspace("read-impossible-bucket");
    write_adapter_output(
        &paths,
        "kafkars",
        status_document(),
        &render(&measurement_with_an_impossible_bucket()),
    );
    let evidence = read_subject(&paths, "kafkars");
    assert!(evidence.result_present, "the file is there");
    assert!(
        evidence.result.is_none(),
        "an index above the layout ceiling makes the document unreadable"
    );
    assert!(
        evidence
            .result_error
            .as_deref()
            .is_some_and(|error| error.contains("8320")),
        "the parse error names the offending bucket: {:?}",
        evidence.result_error
    );
    std::fs::remove_dir_all(&results_root).unwrap();
}
