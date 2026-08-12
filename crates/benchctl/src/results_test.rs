//! Reading subject evidence, the validity verdict and its reasons, and the
//! rules that decide whether two subjects may be compared at all.
//!
//! Every result fixture here is a real `kafkars.producer-benchmark.v2` document
//! built through the schema types, because the parse is half the gate: a
//! document whose offers do not add up is unreadable by construction, and a test
//! that hand-wrote the JSON would be free to disagree with the schema about
//! what "valid" means.
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
use crate::results::{
    DEFERRED_CHECKS, SubjectOutcome, VerificationVerdict, classify, compare, read_subject,
};
use crate::seal_test::workspace;

/// Records for every fixture measurement below.
const RECORDS: u64 = 1_000;

/// A process exit that says nothing went wrong.
fn clean_exit() -> ProcessExit {
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
fn flat(count: u64, value: u64) -> EncodedHistogram {
    let mut histogram = Histogram::new();
    for _ in 0..count {
        histogram.record(value);
    }
    histogram.encode()
}

/// A closed-loop v2 measurement declaring `goodput` and a p99 of `p99`.
fn measurement(goodput: f64, p99: u64) -> ProducerBenchmarkV2 {
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
fn subject_with(name: &str, document: &ProducerBenchmarkV2) -> SubjectOutcome {
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
fn healthy(name: &str, goodput: f64, p99: u64) -> SubjectOutcome {
    subject_with(name, &measurement(goodput, p99))
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
fn a_healthy_subject_is_valid_and_a_valid_run_names_no_reasons() {
    let subject = healthy("kafkars", 100_000.0, 1_000_000);
    assert!(subject.validity().valid, "{:?}", subject.validity().reasons);
    let classification = classify(std::slice::from_ref(&subject), &[]);
    assert!(classification.run_valid);
    assert!(!classification.claim_eligible, "never in this milestone");
    assert!(classification.reasons.is_empty());
    assert_eq!(classification.deferred_checks, DEFERRED_CHECKS.to_vec());
}

#[test]
fn an_incomplete_drain_invalidates_a_subject_that_otherwise_passed() {
    let mut document = measurement(100_000.0, 1_000_000);
    document.outcomes.acknowledged = RECORDS - 7;
    document.outcomes.unknown = 7;
    document.timing.intended_to_terminal = flat(RECORDS - 7, 1_000_000);
    document.timing.accepted_to_terminal = flat(RECORDS - 7, 500_000);
    let subject = subject_with("kafkars", &document);
    let validity = subject.validity();
    assert!(!validity.valid);
    assert!(
        validity
            .reasons
            .iter()
            .any(|reason| reason.contains("7 accepted offers reached no terminal state")),
        "{:?}",
        validity.reasons
    );
}

#[test]
fn offers_still_outstanding_at_the_end_are_an_incomplete_drain() {
    let mut document = measurement(100_000.0, 1_000_000);
    document.queue.final_outstanding = 3;
    let subject = subject_with("kafkars", &document);
    let validity = subject.validity();
    assert!(!validity.valid);
    assert!(
        validity
            .reasons
            .iter()
            .any(|reason| reason.contains("3 offers were still outstanding")),
        "{:?}",
        validity.reasons
    );
}

#[test]
fn failed_records_invalidate_a_subject_whether_or_not_objectives_were_declared() {
    let mut document = measurement(100_000.0, 1_000_000);
    document.outcomes.acknowledged = RECORDS - 2;
    document.outcomes.failed = 1;
    document.outcomes.timed_out = 1;
    let mut subject = subject_with("kafkars", &document);

    // Declaring no objectives is not a licence to lose records. It used to be:
    // the ceiling was only asked about when the scenario stated a latency
    // bound, which made a scenario with no `[slo]` the most permissive one in
    // the repository — and disagreed with `evaluate_slo`, which always asks.
    let without = subject.validity();
    assert!(
        !without.valid,
        "a lost record is a different run, whatever else the scenario declared"
    );
    assert!(
        without
            .reasons
            .iter()
            .any(|reason| reason.contains("2 records failed or timed out")),
        "{:?}",
        without.reasons
    );

    subject.slo = SloSpec {
        corrected_p99_ms: Some(250),
        ..SloSpec::default()
    };
    let with = subject.validity();
    assert!(!with.valid);
    assert_eq!(
        with.reasons, without.reasons,
        "the ceiling does not move when objectives are declared"
    );

    // And the reporting layer's gate agrees on the same measurement, which is
    // the asymmetry this rule exists to remove.
    let verdict = bench_report::evaluate_slo(&document, &SloSpec::default());
    assert!(!verdict.satisfied);
    assert!(
        verdict
            .reasons
            .iter()
            .any(|reason| reason.contains("2 records failed or timed out")),
        "{:?}",
        verdict.reasons
    );
}

#[test]
fn a_measurement_that_lost_nothing_is_still_valid_without_objectives() {
    let subject = healthy("kafkars", 100_000.0, 1_000_000);
    assert!(subject.validity().valid, "{:?}", subject.validity().reasons);
}

#[test]
fn every_way_a_subject_can_fail_is_named_in_its_reasons() {
    let mut subject = healthy("kafkars", 100_000.0, 1_000_000);
    subject.execution = Some(ProcessExit {
        exit_code: None,
        signal: Some(9),
        timed_out: false,
        duration_ms: 5,
    });
    subject.measured = VerificationVerdict::Failed;
    subject.warmup = VerificationVerdict::NotRun;
    let validity = subject.validity();
    assert!(!validity.valid);
    assert_eq!(
        validity.reasons,
        vec![
            "the adapter died on signal 9".to_owned(),
            "the measured topic did not satisfy the verification contract".to_owned(),
            "the warmup topic was never verified".to_owned(),
        ]
    );
}

#[test]
fn a_warmup_that_was_never_required_does_not_invalidate_the_subject() {
    let mut subject = healthy("kafkars", 100_000.0, 1_000_000);
    subject.warmup = VerificationVerdict::NotRequired;
    assert!(subject.validity().valid);
}

#[test]
fn a_subject_that_never_ran_says_exactly_that() {
    let subject = SubjectOutcome::skipped("never");
    let validity = subject.validity();
    assert!(!validity.valid);
    assert_eq!(validity.reasons.first().unwrap(), "the adapter never ran");
    let classification = classify(&[subject], &["topic creation failed".to_owned()]);
    assert!(!classification.run_valid);
    assert_eq!(
        classification.reasons.first().unwrap(),
        "topic creation failed",
        "run-level reasons come before the per-subject ones"
    );
}

#[test]
fn an_attempt_with_no_subjects_is_never_valid() {
    let classification = classify(&[], &[]);
    assert!(!classification.run_valid);
    assert_eq!(
        classification.reasons,
        vec!["the attempt ran no subjects".to_owned()]
    );
}

#[test]
fn two_producer_results_compare_as_candidate_over_baseline() {
    let baseline = healthy("librdkafka-c", 100_000.0, 2_000_000);
    let candidate = healthy("kafkars", 150_000.0, 1_000_000);
    let order = vec!["librdkafka-c".to_owned(), "kafkars".to_owned()];
    let comparison = compare(&order, &[candidate, baseline]);
    assert!(comparison.comparable);
    assert!(comparison.reasons.is_empty());
    assert_eq!(comparison.pairs.len(), 1);
    let pair = &comparison.pairs[0];
    assert_eq!(pair.baseline, "librdkafka-c");
    assert_eq!(pair.candidate, "kafkars");
    assert_eq!(pair.acknowledged_goodput_ratio, Some(1.5));
    assert_eq!(
        pair.p99_latency_ratio,
        Some(0.5),
        "the ratio is over intended-to-terminal, decoded from both histograms"
    );
}

#[test]
fn a_subject_without_a_producer_result_makes_the_attempt_incomparable() {
    let baseline = healthy("librdkafka-c", 100_000.0, 2_000_000);
    let candidate = SubjectOutcome::skipped("kafkars");
    let order = vec!["librdkafka-c".to_owned(), "kafkars".to_owned()];
    let comparison = compare(&order, &[baseline, candidate]);
    assert!(!comparison.comparable);
    assert!(comparison.pairs.is_empty());
    assert!(
        comparison
            .reasons
            .iter()
            .any(|r| r.starts_with("kafkars produced no result")),
        "{:?}",
        comparison.reasons
    );
}

#[test]
fn one_subject_is_not_a_comparison() {
    let only = healthy("kafkars", 100_000.0, 1_000_000);
    let comparison = compare(&["kafkars".to_owned()], &[only]);
    assert!(!comparison.comparable);
    assert_eq!(
        comparison.reasons,
        vec!["a comparison needs at least two subjects that ran".to_owned()]
    );
}

#[test]
fn an_adapter_whose_result_reports_a_different_version_than_it_described_is_invalid() {
    // The shape this exists for: a wrapper whose `describe` pins a version by
    // hand while the client writes its real one at run time. The described
    // string is what the experiment id is built from, so a mismatch means the
    // attempt is filed under a client that did not run — and a suite would
    // compute a median across two different clients without noticing.
    let mut document = measurement(100_000.0, 1_000_000);
    "2.16.1".clone_into(&mut document.adapter_version);
    let mut subject = subject_with("librdkafka-c", &document);
    "2.15.0".clone_into(&mut subject.declared_adapter_version);

    let validity = subject.validity();

    assert!(!validity.valid);
    let reason = validity
        .reasons
        .iter()
        .find(|reason| reason.contains("described itself as version"))
        .unwrap_or_else(|| panic!("{:?}", validity.reasons));
    assert!(reason.contains("2.15.0"), "{reason}");
    assert!(reason.contains("2.16.1"), "{reason}");
    assert!(!classify(&[subject], &[]).run_valid);
}

#[test]
fn a_matching_adapter_version_says_nothing() {
    // The ordinary case must stay silent, and a subject whose declared version
    // was never learned must not be accused of disagreeing with itself.
    let matching = healthy("kafkars", 100_000.0, 1_000_000);
    assert_eq!(matching.declared_adapter_version, "0.1.0");
    assert_eq!(
        matching.evidence.result.as_ref().unwrap().adapter_version,
        "0.1.0"
    );
    assert!(
        matching.validity().valid,
        "{:?}",
        matching.validity().reasons
    );

    let mut unknown = healthy("kafkars", 100_000.0, 1_000_000);
    unknown.declared_adapter_version = String::new();
    assert!(unknown.validity().valid, "{:?}", unknown.validity().reasons);
}

#[test]
fn a_hand_edited_classification_is_not_a_readable_one() {
    // The read side of the same gate the seal applies on the way out. A bundle
    // whose `classification.json` grants itself claim eligibility, or declares
    // the run invalid without saying why, is not an answer to "may this be
    // believed" — and a reader that shrugged and used it would be treating a
    // hand edit as a verdict.
    let (results_root, paths) = workspace("classification-edited");
    let honest = classify(&[healthy("kafkars", 100_000.0, 1_000_000)], &[]);
    std::fs::write(
        paths.classification_json(),
        bench_schema::pretty_bytes(&honest).unwrap(),
    )
    .unwrap();
    assert!(
        crate::pipeline::read_classification(&paths).is_some_and(|read| read == honest),
        "an honest classification reads back unchanged"
    );

    for edit in [
        |document: &mut bench_schema::Classification| document.claim_eligible = true,
        |document: &mut bench_schema::Classification| {
            document.run_valid = false;
            document.reasons.clear();
        },
        |document: &mut bench_schema::Classification| {
            "kafkars.comparison.v1".clone_into(&mut document.schema);
        },
    ] {
        let mut edited = honest.clone();
        edit(&mut edited);
        std::fs::write(
            paths.classification_json(),
            bench_schema::pretty_bytes(&edited).unwrap(),
        )
        .unwrap();
        assert!(
            crate::pipeline::read_classification(&paths).is_none(),
            "an edited classification must not read as a verdict: {edited:?}"
        );
    }
    std::fs::remove_dir_all(&results_root).unwrap();
}

/// A measurement whose latency histogram claims a bucket no `u64` can reach.
///
/// `8_320` is the first index whose scale is 64: decoding it used to shift a
/// `u64` by its own width. The document is otherwise a perfectly ordinary
/// measurement, which is the point — the hostile part is one integer.
fn measurement_with_an_impossible_bucket() -> ProducerBenchmarkV2 {
    let mut document = measurement(100_000.0, 1_000_000);
    document.timing.intended_to_terminal.counts = vec![(8_320, RECORDS)];
    document
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

#[test]
fn an_impossible_bucket_classifies_and_compares_without_panicking() {
    // Before the index bound was enforced, this document parsed, and reading a
    // percentile out of it shifted a `u64` by 64 bits inside `compare`. The
    // property under test is that a hostile histogram costs the run its
    // validity, never the control plane its stack.
    let hostile = subject_with("kafkars", &measurement_with_an_impossible_bucket());
    let baseline = healthy("librdkafka-c", 100_000.0, 2_000_000);
    assert_eq!(
        hostile.evidence.intended_to_terminal_p99_ns(),
        None,
        "there is no percentile to read out of a rejected histogram"
    );

    let validity = hostile.validity();
    assert!(!validity.valid);
    assert!(
        validity
            .reasons
            .iter()
            .any(|reason| reason.contains("is not a readable")),
        "{:?}",
        validity.reasons
    );

    let order = vec!["librdkafka-c".to_owned(), "kafkars".to_owned()];
    let comparison = compare(&order, &[baseline.clone(), hostile.clone()]);
    assert!(!comparison.comparable);
    assert!(comparison.pairs.is_empty());
    assert!(
        comparison
            .reasons
            .iter()
            .any(|reason| reason.starts_with("kafkars produced an unreadable result")),
        "{:?}",
        comparison.reasons
    );
    assert!(!classify(&[baseline, hostile], &[]).run_valid);
}
