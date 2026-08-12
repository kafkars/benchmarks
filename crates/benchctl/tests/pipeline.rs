//! The offline happy path and the evidence it produces: a complete bundle, a
//! valid classification, a comparison with real ratios, and checksums a person
//! can verify with the tool already on their machine.
//!
//! Every subject and all three cluster tools are the `fake-adapter` fixture, so
//! these tests need no Kafka, no network, and no cooperation from the resolver
//! workstream.
#![expect(
    clippy::unwrap_used,
    reason = "an integration fixture that misbehaves must fail the test immediately"
)]

mod common;

use bench_schema::{AdapterOutcome, ExecutionStatus, LoadMode, PhaseOutcome};
use benchctl::seal::run_attempt;

use common::{
    RUN_ID, SOURCE_TOML, assert_bundle_is_self_consistent, assert_layout_is_complete,
    assert_system_checksum_check_passes, cleanup, experiment, phase_names, read_classification,
    read_comparison, read_execution_order, read_status, request, tools, workspace,
};

#[test]
fn a_successful_attempt_seals_a_complete_and_valid_bundle() {
    let (results_root, paths) = workspace("success");
    let resolved = experiment(&[("kafkars", "ok"), ("librdkafka-c", "ok")]);
    let status = run_attempt(request(&paths, resolved, tools("ok"))).unwrap();
    assert_eq!(status, ExecutionStatus::Complete);

    let sealed = read_status(&paths);
    assert!(sealed.has_expected_schema());
    assert_eq!(sealed.execution_status, ExecutionStatus::Complete);
    assert_eq!(sealed.failure_reason, None);
    assert!(!sealed.interrupted);
    assert!(sealed.experiment_id.is_some());
    assert_eq!(sealed.subjects.len(), 2);
    for subject in &sealed.subjects {
        assert_eq!(subject.execution.map(|exit| exit.exit_code), Some(Some(0)));
        assert_eq!(subject.adapter_outcome, Some(AdapterOutcome::Succeeded));
        assert!(subject.result_present);
        assert_eq!(
            subject.verification.measured.and_then(|check| check.valid),
            Some(true)
        );
        assert_eq!(
            subject.verification.warmup.and_then(|check| check.valid),
            Some(true)
        );
    }
    assert!(
        sealed
            .phases
            .iter()
            .all(|phase| phase.outcome == PhaseOutcome::Succeeded),
        "{:?}",
        sealed.phases
    );
    assert_eq!(
        phase_names(&sealed),
        vec![
            "seal-inputs".to_owned(),
            "topic-create".to_owned(),
            "subject:kafkars:run".to_owned(),
            "subject:kafkars:verify-measured".to_owned(),
            "subject:kafkars:verify-warmup".to_owned(),
            "subject:librdkafka-c:run".to_owned(),
            "subject:librdkafka-c:verify-measured".to_owned(),
            "subject:librdkafka-c:verify-warmup".to_owned(),
            "topic-cleanup".to_owned(),
        ]
    );

    assert_layout_is_complete(&paths, &["kafkars", "librdkafka-c"]);
    assert_eq!(
        std::fs::read_to_string(paths.experiment_source_toml()).unwrap(),
        SOURCE_TOML,
        "the scenario is sealed verbatim"
    );

    let classification = read_classification(&paths);
    assert!(classification.run_valid, "{:?}", classification.reasons);
    assert!(!classification.claim_eligible);
    assert_eq!(classification.subjects.len(), 2);
    assert!(classification.subjects.iter().all(|subject| subject.valid));
    assert!(
        classification
            .deferred_checks
            .contains(&"latency-csv-row-validation".to_owned()),
        "the verdict names what it did not check"
    );

    let order = read_execution_order(&paths);
    assert_eq!(order.order, vec!["kafkars", "librdkafka-c"]);
    assert_eq!(order.decided_by, "resolved-experiment-runtime-binding");

    let comparison = read_comparison(&paths);
    assert!(comparison.comparable, "{:?}", comparison.reasons);
    assert_eq!(comparison.pairs.len(), 1);
    let pair = &comparison.pairs[0];
    assert_eq!(pair.baseline, "kafkars");
    assert_eq!(pair.candidate, "librdkafka-c");
    assert!(pair.acknowledged_goodput_ratio.is_some_and(f64::is_finite));
    assert!(pair.p99_latency_ratio.is_some_and(f64::is_finite));

    assert_bundle_is_self_consistent(&paths);
    assert_system_checksum_check_passes(&paths);
    cleanup(&results_root);
}

#[test]
fn the_sealed_result_is_a_v2_measurement_bound_to_this_attempts_run_id() {
    let (results_root, paths) = workspace("v2-result");
    let resolved = experiment(&[("kafkars", "ok")]);
    run_attempt(request(&paths, resolved, tools("ok"))).unwrap();
    let bytes = std::fs::read(paths.adapter_result_json("kafkars")).unwrap();
    // `from_slice` checks the accounting invariants, so parsing at all is the
    // assertion that every offer this attempt made is accounted for.
    let result = bench_schema::ProducerBenchmarkV2::from_slice(&bytes).unwrap();
    assert_eq!(result.schema, bench_schema::PRODUCER_BENCHMARK_V2);
    assert_eq!(result.run_id, RUN_ID);
    assert_eq!(result.load_mode, LoadMode::ClosedLoop);
    assert_eq!(
        result.timing.intended_to_call_start, None,
        "a closed-loop measurement has no schedule to be late against"
    );
    assert_eq!(result.outcomes.unknown, 0);
    assert_eq!(result.queue.final_outstanding, 0, "the run drained");
    assert!(result.valid);

    // The topic the runtime binding named is what the verifier was pointed at,
    // which is where a v2 document leaves that fact.
    let verification =
        std::fs::read_to_string(paths.verification_json("kafkars", "measured")).unwrap();
    assert!(
        verification.contains(&format!("kfb-{RUN_ID}-kafkars")),
        "{verification}"
    );
    cleanup(&results_root);
}

#[test]
fn a_failing_subject_never_stops_the_one_after_it() {
    let (results_root, paths) = workspace("continues");
    let resolved = experiment(&[("first", "run-nonzero"), ("second", "ok")]);
    let status = run_attempt(request(&paths, resolved, tools("ok"))).unwrap();
    assert_eq!(status, ExecutionStatus::Partial);

    let sealed = read_status(&paths);
    assert_eq!(
        sealed.subjects[0].execution.map(|exit| exit.exit_code),
        Some(Some(3))
    );
    assert_eq!(
        sealed.subjects[0].adapter_outcome,
        Some(AdapterOutcome::Failed)
    );
    assert!(!sealed.subjects[0].result_present);
    assert_eq!(
        sealed.subjects[1].execution.map(|exit| exit.exit_code),
        Some(Some(0))
    );
    assert!(sealed.subjects[1].result_present);
    assert!(
        paths.adapter_result_json("second").is_file(),
        "the second subject must still have produced evidence"
    );
    assert_eq!(read_execution_order(&paths).order, vec!["first", "second"]);
    assert!(
        phase_names(&sealed).contains(&"subject:second:verify-measured".to_owned()),
        "the second subject was verified too"
    );

    let classification = read_classification(&paths);
    assert!(!classification.run_valid);
    let first = &classification.subjects[0];
    assert!(!first.valid);
    assert!(
        first
            .reasons
            .iter()
            .any(|reason| reason.contains("exited with code 3")),
        "{:?}",
        first.reasons
    );
    assert!(
        classification.subjects[1].valid,
        "{:?}",
        classification.subjects[1].reasons
    );

    let comparison = read_comparison(&paths);
    assert!(!comparison.comparable, "a missing result is not comparable");
    assert_bundle_is_self_consistent(&paths);
    cleanup(&results_root);
}

#[test]
fn a_verifier_that_disagrees_leaves_the_run_complete_and_invalid() {
    let (results_root, paths) = workspace("verifier-invalid");
    let resolved = experiment(&[("kafkars", "ok")]);
    let status = run_attempt(request(&paths, resolved, tools("verifier-invalid"))).unwrap();
    assert_eq!(
        status,
        ExecutionStatus::Complete,
        "the machinery worked; only the answer was no"
    );

    let sealed = read_status(&paths);
    assert_eq!(
        sealed.subjects[0]
            .verification
            .measured
            .and_then(|check| check.valid),
        Some(false)
    );
    assert_eq!(
        sealed
            .phases
            .iter()
            .find(|phase| phase.name == "subject:kafkars:verify-measured")
            .map(|phase| phase.outcome),
        Some(PhaseOutcome::Failed),
        "a check that did not pass is a failed phase"
    );

    let classification = read_classification(&paths);
    assert!(!classification.run_valid);
    assert!(
        classification
            .reasons
            .iter()
            .any(|reason| reason.contains("did not satisfy the verification contract")),
        "{:?}",
        classification.reasons
    );
    assert_bundle_is_self_consistent(&paths);
    assert_system_checksum_check_passes(&paths);
    cleanup(&results_root);
}

#[test]
fn a_verifier_that_cannot_run_makes_the_attempt_partial() {
    let (results_root, paths) = workspace("verifier-fail");
    let resolved = experiment(&[("kafkars", "ok")]);
    let status = run_attempt(request(&paths, resolved, tools("verifier-fail"))).unwrap();
    assert_eq!(status, ExecutionStatus::Partial);

    let sealed = read_status(&paths);
    assert_eq!(
        sealed.subjects[0]
            .verification
            .measured
            .map(|check| check.exit_code),
        Some(Some(1))
    );
    assert!(
        sealed
            .failure_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("verify-measured")),
        "{:?}",
        sealed.failure_reason
    );
    assert!(!read_classification(&paths).run_valid);
    assert_bundle_is_self_consistent(&paths);
    cleanup(&results_root);
}
