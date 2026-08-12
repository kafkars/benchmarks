//! Reading subject evidence, the validity verdict and its reasons, and the
//! rules that decide whether two subjects may be compared at all.
#![expect(
    clippy::unwrap_used,
    reason = "an evidence fixture that cannot be written must fail the test immediately"
)]

use bench_schema::{AdapterOutcome, ProcessExit, SubjectVerification};

use crate::attempt::AttemptPaths;
use crate::results::{
    DEFERRED_CHECKS, SubjectOutcome, VerificationVerdict, classify, compare, read_subject,
};
use crate::seal_test::workspace;

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

/// A producer result document declaring `goodput` and a p99 of `p99`.
fn result_document(goodput: f64, p99: u64) -> String {
    format!(
        r#"{{"schema":"kafkars.producer-benchmark.v1","adapter":"fake","run_id":"0123456789abcdef",
        "valid":true,"acknowledged_records":1000,"acknowledged_records_per_second":{goodput},
        "latency_ns":{{"p99":{p99}}},"an_unknown_key":[1,2,3]}}"#
    )
}

/// A succeeded adapter status document.
fn status_document() -> &'static str {
    r#"{"schema":"kafkars.adapter-status.v1","outcome":"succeeded",
    "started_at":"2026-08-12T14:03:05.000Z","finished_at":"2026-08-12T14:03:06.000Z"}"#
}

/// A subject that did everything right.
fn healthy(name: &str, goodput: f64, p99: u64) -> SubjectOutcome {
    let (results_root, paths) = workspace(&format!("evidence-{name}"));
    write_adapter_output(
        &paths,
        name,
        status_document(),
        &result_document(goodput, p99),
    );
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
    }
}

#[test]
fn reading_a_subject_keeps_the_fields_it_needs_and_ignores_the_rest() {
    let (results_root, paths) = workspace("read");
    write_adapter_output(
        &paths,
        "only",
        status_document(),
        &result_document(1234.5, 99),
    );
    let evidence = read_subject(&paths, "only");
    assert!(evidence.result_present);
    assert!(evidence.notes.is_empty(), "{:?}", evidence.notes);
    assert_eq!(evidence.adapter_outcome(), Some(AdapterOutcome::Succeeded));
    let result = evidence.result.unwrap();
    assert!(result.has_known_schema());
    assert!(result.adapter_declared_valid());
    assert_eq!(result.acknowledged_records_per_second, Some(1234.5));
    assert_eq!(result.headline_p99_ns(), Some(99));
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
    assert_eq!(pair.p99_latency_ratio, Some(0.5));
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
