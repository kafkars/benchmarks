//! The panic boundary around the verdict derivation.
//!
//! A child module of `seal` rather than a sibling, because the property under
//! test is what the funnel does with its own private fallback: the plan it
//! seals when the code that turns recorded facts into a verdict gives up. The
//! sibling test modules drive `run_attempt` from outside and cannot reach it.
#![expect(
    clippy::unwrap_used,
    reason = "a seal fixture that cannot be built must fail the test immediately"
)]

use bench_schema::{ExecutionStatus, PhaseOutcome, ProcessExit};

use super::{AttemptRequest, AttemptState, SealPlan, derive_plan, seal};
use crate::results::{SubjectOutcome, VerificationVerdict};
use crate::seal_test::{environment, experiment, shell, workspace};

/// A derivation that fails the way a bug in the reporting code would: partway
/// through interpreting a subject's own bytes.
fn panicking_derivation(_state: &AttemptState, _request: &AttemptRequest) -> SealPlan {
    panic!("the percentile reader gave up on a subject's histogram");
}

/// An attempt state carrying two subjects that both ran and both reported.
fn state_with_two_subjects() -> AttemptState {
    let mut state = AttemptState::new();
    state.record("seal-inputs", PhaseOutcome::Succeeded, None);
    for name in ["kafkars", "librdkafka-c"] {
        state.record(format!("subject:{name}:run"), PhaseOutcome::Succeeded, None);
        let mut outcome = SubjectOutcome::skipped(name);
        outcome.execution = Some(ProcessExit {
            exit_code: Some(0),
            signal: None,
            timed_out: false,
            duration_ms: 12,
        });
        outcome.measured = VerificationVerdict::Satisfied;
        outcome.warmup = VerificationVerdict::Satisfied;
        state.subjects.push(outcome);
    }
    state
}

#[test]
fn a_panic_while_deriving_the_verdict_seals_crashed_with_the_record_intact() {
    let (results_root, paths) = workspace("derive-panics");
    let resolved = experiment(&[
        ("kafkars", shell("exit 0")),
        ("librdkafka-c", shell("exit 0")),
    ]);
    let request = AttemptRequest {
        resolved,
        lock: bench_schema::SubjectsLock::new(Vec::new()),
        source_toml: "name = \"boundary\"\n".to_owned(),
        environment: environment(),
        tools: bench_schema::ClusterTools::default(),
        paths: paths.clone(),
        started_at: std::time::SystemTime::now(),
    };
    let mut state = state_with_two_subjects();

    let plan = derive_plan(&mut state, &request, panicking_derivation);

    assert_eq!(
        plan.status.execution_status,
        ExecutionStatus::Crashed,
        "a verdict nobody could derive is a crashed control plane"
    );
    assert_eq!(
        plan.status.subjects.len(),
        2,
        "every subject the attempt recorded survives the panic"
    );
    assert!(
        plan.status.subjects.iter().all(|subject| subject
            .execution
            .is_some_and(|exit| exit.exit_code == Some(0))),
        "the subject records are the real ones, not blanks"
    );
    assert!(
        plan.status
            .phases
            .iter()
            .any(|phase| phase.name == "subject:kafkars:run"),
        "the phase record survives too: {:?}",
        plan.status.phases
    );
    assert!(
        plan.status
            .phases
            .iter()
            .any(|phase| phase.name == "classify" && phase.outcome == PhaseOutcome::Failed),
        "the failed derivation is itself a recorded phase: {:?}",
        plan.status.phases
    );
    assert!(
        plan.status
            .failure_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("percentile reader gave up")),
        "{:?}",
        plan.status.failure_reason
    );

    let classification = plan.classification.as_ref().unwrap();
    assert!(
        !classification.run_valid,
        "a verdict nobody derived is not a pass"
    );
    assert!(
        classification
            .reasons
            .iter()
            .any(|reason| reason.contains("could not be derived")),
        "{:?}",
        classification.reasons
    );
    assert!(
        plan.comparison.is_none(),
        "a comparison invented here would be a fabrication"
    );

    // The plan still seals: a bundle exists and says what happened.
    let sealed = seal(&paths, &plan).unwrap();
    assert_eq!(sealed, ExecutionStatus::Crashed);
    assert!(paths.status_json().is_file());
    assert!(paths.classification_json().is_file());
    assert!(!paths.comparison_json().exists());
    assert!(paths.checksums_txt().is_file());
    assert!(paths.bundle_json().is_file());
    std::fs::remove_dir_all(&results_root).unwrap();
}

#[test]
fn a_derivation_that_succeeds_is_passed_through_untouched() {
    let (results_root, paths) = workspace("derive-succeeds");
    let resolved = experiment(&[("kafkars", shell("exit 0"))]);
    let request = AttemptRequest {
        resolved,
        lock: bench_schema::SubjectsLock::new(Vec::new()),
        source_toml: "name = \"boundary\"\n".to_owned(),
        environment: environment(),
        tools: bench_schema::ClusterTools::default(),
        paths,
        started_at: std::time::SystemTime::now(),
    };
    let mut state = state_with_two_subjects();

    let plan = derive_plan(&mut state, &request, AttemptState::plan);

    assert_eq!(plan.status.execution_status, ExecutionStatus::Complete);
    assert!(
        plan.comparison.is_some(),
        "the ordinary derivation still produces a comparison"
    );
    assert!(
        !plan
            .status
            .phases
            .iter()
            .any(|phase| phase.name == "classify"),
        "no failure is recorded when nothing failed"
    );
    std::fs::remove_dir_all(&results_root).unwrap();
}
