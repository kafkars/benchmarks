//! The validity verdict and every reason a subject's evidence may not be
//! believed: the process, the documents, the provenance, and the accounting.
#![expect(
    clippy::unwrap_used,
    reason = "an evidence fixture that cannot be written must fail the test immediately"
)]

use bench_schema::{ProcessExit, SloSpec};

use crate::results::{DEFERRED_CHECKS, SubjectOutcome, VerificationVerdict, classify};

use super::evidence_test::{RECORDS, flat, healthy, measurement, subject_with};

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
