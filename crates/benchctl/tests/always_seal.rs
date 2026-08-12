//! The always-seal guarantee, exercised against every way an attempt can end
//! badly: an adapter killed from outside, one that aborts, one that ignores its
//! deadline, a topic tool that fails, and a failure before the attempt began.
//!
//! Each case asserts the same two things — a bundle exists, and it says the
//! right thing — because a harness whose failure paths produce silence is a
//! harness that reports only its successes.
#![expect(
    clippy::unwrap_used,
    reason = "an integration fixture that misbehaves must fail the test immediately"
)]

mod common;

use std::path::PathBuf;
use std::time::Duration;

use bench_schema::{BudgetSpec, ExecutionStatus, PhaseOutcome};
use benchctl::CtlErrorKind;
use benchctl::seal::{run_attempt, seal_failure};

use common::{
    SOURCE_TOML, assert_bundle_is_self_consistent, assert_process_is_gone,
    assert_system_checksum_check_passes, cleanup, experiment, phase_names, read_classification,
    read_status, request, tools, workspace,
};

/// Waits for the fixture to write its process id, then kills it hard.
///
/// The kill has to come from outside the control plane: a signal the supervisor
/// sent itself would be policy, and policy is not a crash.
fn kill_when_pid_appears(pid_file: PathBuf) -> std::thread::JoinHandle<bool> {
    std::thread::spawn(move || {
        for _ in 0..600 {
            if let Ok(text) = std::fs::read_to_string(&pid_file) {
                let pid = text.trim().to_owned();
                if !pid.is_empty() {
                    return std::process::Command::new("kill")
                        .args(["-9", &pid])
                        .status()
                        .is_ok_and(|status| status.success());
                }
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        false
    })
}

/// A budget whose run deadline is `seconds`.
fn budget(seconds: u64) -> BudgetSpec {
    BudgetSpec {
        run_timeout_seconds: seconds,
        ..BudgetSpec::default()
    }
}

#[test]
fn an_adapter_killed_from_outside_is_a_crash() {
    let (results_root, paths) = workspace("killed");
    let mut resolved = experiment(&[("victim", "run-write-pid-then-hang")]);
    // Short enough that a failed kill surfaces as a wrong status rather than a
    // ten-minute hang, long enough that the kill wins the race.
    resolved.budget = budget(60);
    let killer = kill_when_pid_appears(paths.adapter_dir("victim").join("pid"));
    let status = run_attempt(request(&paths, resolved, tools("ok"))).unwrap();
    assert!(
        killer.join().unwrap(),
        "the fixture never published its pid"
    );
    assert_eq!(status, ExecutionStatus::Crashed);

    let sealed = read_status(&paths);
    let exit = sealed.subjects[0].execution.unwrap();
    assert_eq!(exit.signal, Some(9));
    assert_eq!(exit.exit_code, None);
    assert!(!exit.timed_out, "nobody's deadline expired");
    assert!(!sealed.interrupted);
    assert_bundle_is_self_consistent(&paths);
    cleanup(&results_root);
}

#[test]
fn an_adapter_that_aborts_is_a_crash() {
    let (results_root, paths) = workspace("aborted");
    let resolved = experiment(&[("aborts", "run-abort")]);
    let status = run_attempt(request(&paths, resolved, tools("ok"))).unwrap();
    assert_eq!(status, ExecutionStatus::Crashed);

    let sealed = read_status(&paths);
    let exit = sealed.subjects[0].execution.unwrap();
    assert_eq!(exit.signal, Some(6), "SIGABRT is signal six");
    assert!(!sealed.subjects[0].result_present);
    assert!(!read_classification(&paths).run_valid);
    assert_bundle_is_self_consistent(&paths);
    assert_system_checksum_check_passes(&paths);
    cleanup(&results_root);
}

#[test]
fn an_adapter_past_its_deadline_times_out_and_is_reaped() {
    let (results_root, paths) = workspace("timed-out");
    let mut resolved = experiment(&[("hangs", "run-write-pid-then-hang")]);
    resolved.budget = budget(2);
    let status = run_attempt(request(&paths, resolved, tools("ok"))).unwrap();
    assert_eq!(status, ExecutionStatus::TimedOut);

    let sealed = read_status(&paths);
    let exit = sealed.subjects[0].execution.unwrap();
    assert!(exit.timed_out);
    assert_eq!(exit.signal, Some(9), "the supervisor kills hard");
    assert!(exit.duration_ms >= 2_000, "it ran until its deadline");
    let pid = std::fs::read_to_string(paths.adapter_dir("hangs").join("pid")).unwrap();
    assert_process_is_gone(pid.trim());
    assert_bundle_is_self_consistent(&paths);
    cleanup(&results_root);
}

#[test]
fn a_silent_hang_still_times_out() {
    let (results_root, paths) = workspace("hangs");
    let mut resolved = experiment(&[("hangs", "run-hang")]);
    resolved.budget = budget(2);
    let status = run_attempt(request(&paths, resolved, tools("ok"))).unwrap();
    assert_eq!(status, ExecutionStatus::TimedOut);
    assert!(
        read_status(&paths).subjects[0]
            .execution
            .is_some_and(|exit| exit.timed_out)
    );
    assert_bundle_is_self_consistent(&paths);
    cleanup(&results_root);
}

#[test]
fn a_failed_topic_tool_seals_before_any_subject_runs() {
    let (results_root, paths) = workspace("topics-fail");
    let resolved = experiment(&[("first", "ok"), ("second", "ok")]);
    let status = run_attempt(request(&paths, resolved, tools("topics-fail"))).unwrap();
    assert_eq!(status, ExecutionStatus::Partial);

    let sealed = read_status(&paths);
    assert_eq!(
        sealed
            .phases
            .iter()
            .find(|phase| phase.name == "topic-create")
            .map(|phase| phase.outcome),
        Some(PhaseOutcome::Failed)
    );
    assert!(
        sealed
            .subjects
            .iter()
            .all(|subject| subject.execution.is_none()),
        "no subject may run against topics that were never created"
    );
    assert!(
        sealed
            .phases
            .iter()
            .filter(|phase| phase.name.ends_with(":run"))
            .all(|phase| phase.outcome == PhaseOutcome::Skipped)
    );
    assert!(
        !phase_names(&sealed).contains(&"subject:first:verify-measured".to_owned()),
        "nothing is verified when nothing ran"
    );
    assert!(!paths.adapter_result_json("first").exists());
    let classification = read_classification(&paths);
    assert!(!classification.run_valid);
    assert!(
        classification
            .reasons
            .iter()
            .any(|reason| reason.contains("topic creation failed")),
        "{:?}",
        classification.reasons
    );
    assert_bundle_is_self_consistent(&paths);
    assert_system_checksum_check_passes(&paths);
    cleanup(&results_root);
}

#[test]
fn a_failure_before_the_attempt_seals_the_source_and_the_reason() {
    let (results_root, paths) = workspace("pre-attempt");
    let status = seal_failure(
        &paths,
        Some("name = \"unusable\"\n"),
        ExecutionStatus::Partial,
        "probe failed",
    )
    .unwrap();
    assert_eq!(status, ExecutionStatus::Partial);

    assert_eq!(
        std::fs::read_to_string(paths.experiment_source_toml()).unwrap(),
        "name = \"unusable\"\n"
    );
    let sealed = read_status(&paths);
    assert_eq!(sealed.execution_status, ExecutionStatus::Partial);
    assert_eq!(sealed.failure_reason.as_deref(), Some("probe failed"));
    assert_eq!(phase_names(&sealed), vec!["pre-attempt".to_owned()]);
    assert!(sealed.subjects.is_empty());
    assert!(!read_classification(&paths).run_valid);
    assert!(
        !paths.comparison_json().exists(),
        "an attempt that never ran has nothing to compare"
    );
    assert_bundle_is_self_consistent(&paths);
    assert_system_checksum_check_passes(&paths);
    cleanup(&results_root);
}

/// Makes `path` unreadable, reporting whether the operating system agreed.
///
/// Running as root defeats mode bits, so the caller skips rather than fails:
/// asserting that a privileged process cannot read a file would be asserting
/// something untrue about the machine.
fn make_unreadable(path: &std::path::Path) -> bool {
    let unreadable = std::process::Command::new("chmod")
        .args(["000", &path.display().to_string()])
        .status()
        .is_ok_and(|status| status.success());
    unreadable && std::fs::read(path).is_err()
}

#[test]
fn a_seal_that_fails_late_keeps_the_evidence_it_had_already_written() {
    let (results_root, paths) = workspace("seal-fails-late");
    // The checksum walk is the last thing a seal does, and it opens every file
    // in the bundle. A file it cannot open fails the seal *after* `status.json`,
    // `classification.json`, and `comparison.json` are already on disk — which
    // is the state the recovery path used to overwrite with a stub.
    let unreadable = paths.root().join("operator-notes.txt");
    std::fs::write(&unreadable, b"a file the seal cannot read\n").unwrap();
    if !make_unreadable(&unreadable) {
        eprintln!("skipping: this process can read a chmod-000 file");
        cleanup(&results_root);
        return;
    }

    let resolved = experiment(&[("kafkars", "ok"), ("librdkafka-c", "ok")]);
    let error = run_attempt(request(&paths, resolved, tools("ok"))).unwrap_err();
    assert_eq!(error.kind(), CtlErrorKind::Seal, "{error}");
    assert_eq!(
        error.exit_code(),
        74,
        "a bundle that could not be written has its own exit code"
    );

    // What the pipeline does next, verbatim: report the failure through the
    // pre-attempt sealer. It must not touch the status already there.
    let rich = read_status(&paths);
    assert_eq!(
        rich.subjects.len(),
        2,
        "the seal got as far as the subjects"
    );
    let preserved = seal_failure(
        &paths,
        Some(SOURCE_TOML),
        ExecutionStatus::Partial,
        &error.to_string(),
    )
    .unwrap();

    let after = read_status(&paths);
    assert_eq!(after, rich, "the richer status.json survives untouched");
    assert_eq!(after.subjects.len(), 2);
    assert!(!after.phases.is_empty());
    assert_eq!(
        preserved, rich.execution_status,
        "the preserved status is the one the bundle records, not an invention"
    );
    assert!(
        paths.classification_json().is_file() && paths.comparison_json().is_file(),
        "every document written before the failure is still there"
    );

    // The terminal files could not be produced — the unreadable file is still
    // unreadable — so the bundle says so in the bundle, not only on stderr.
    let note = std::fs::read_to_string(paths.seal_failure_txt()).unwrap();
    assert!(note.contains("sealing did not finish"), "{note}");
    assert!(note.contains("checksum") || note.contains("open"), "{note}");

    // Once the obstruction is gone the same call completes the bundle, which is
    // the property that makes this a pause rather than a dead end.
    std::fs::remove_file(&unreadable).unwrap();
    seal_failure(
        &paths,
        Some(SOURCE_TOML),
        ExecutionStatus::Partial,
        "retried",
    )
    .unwrap();
    assert!(paths.checksums_txt().is_file(), "checksums.txt completed");
    assert!(paths.bundle_json().is_file(), "bundle.json completed");
    assert_eq!(read_status(&paths), rich, "still not overwritten");
    assert_bundle_is_self_consistent(&paths);
    cleanup(&results_root);
}
