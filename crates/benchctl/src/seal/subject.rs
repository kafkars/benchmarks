//! Running one subject: the supervision spec it is given, and how its process
//! ending is recorded.

use std::path::Path;
use std::time::Duration;

use bench_schema::{ExecutionStatus, PhaseOutcome};

use crate::interrupt::InterruptFlag;
use crate::results::SubjectOutcome;
use crate::supervise::{SupervisedRun, ToolSpec};
use crate::verify::VerificationPhase;

use super::request::AttemptRequest;
use super::state::AttemptState;
use super::verification::verify_topic;

/// Runs one subject and verifies both of its topics.
pub(super) fn run_subject(
    request: &AttemptRequest,
    state: &mut AttemptState,
    interrupt: &InterruptFlag,
    name: &str,
) -> SubjectOutcome {
    let mut outcome = SubjectOutcome::skipped(name);
    // The objectives travel with the subject because the validity gate reads
    // them.
    outcome.slo = request.resolved.slo;
    // So does the version the adapter claimed at probe time, because the gate
    // compares it against the one the client reports at run time. Taken from the
    // resolved experiment rather than the subjects lock: the resolved document
    // is what the experiment id is computed over, so this is exactly the string
    // the identity was built from.
    if let Some(subject) = request.resolved.subject(name) {
        outcome
            .declared_adapter_version
            .clone_from(&subject.adapter_version);
    }
    let phase = format!("subject:{name}:run");
    if interrupt.is_set() {
        state.record(
            phase,
            PhaseOutcome::Skipped,
            Some("the run was interrupted".to_owned()),
        );
        return outcome;
    }
    let Some(spec) = subject_spec(request, state, name, &phase) else {
        return outcome;
    };
    match crate::supervise::run(&spec, interrupt) {
        Ok(run) => {
            outcome.execution = Some(run.exit);
            outcome.interrupted = run.interrupted;
            record_subject_exit(state, &phase, run);
        }
        Err(error) => {
            let detail = error.to_string();
            state.record(&phase, PhaseOutcome::Failed, Some(detail.clone()));
            state.degrade(
                ExecutionStatus::Partial,
                format!("{name} did not run: {detail}"),
            );
            return outcome;
        }
    }
    outcome.evidence = crate::results::read_subject(&request.paths, name);
    verify_topic(
        request,
        state,
        interrupt,
        &mut outcome,
        VerificationPhase::Measured,
    );
    verify_topic(
        request,
        state,
        interrupt,
        &mut outcome,
        VerificationPhase::Warmup,
    );
    outcome
}

/// Builds the supervision spec for one subject, or records why it cannot.
fn subject_spec(
    request: &AttemptRequest,
    state: &mut AttemptState,
    name: &str,
    phase: &str,
) -> Option<ToolSpec> {
    let paths = &request.paths;
    let directory = paths.adapter_dir(name);
    if let Err(error) = std::fs::create_dir_all(&directory) {
        let detail = format!("create {}: {error}", directory.display());
        state.record(phase, PhaseOutcome::Failed, Some(detail.clone()));
        state.degrade(ExecutionStatus::Partial, detail);
        return None;
    }
    let Some(subject) = request.resolved.subject(name) else {
        let detail = format!("the resolved experiment has no subject named {name}");
        state.record(phase, PhaseOutcome::Failed, Some(detail.clone()));
        state.degrade(ExecutionStatus::Partial, detail);
        return None;
    };
    let mut argv = subject.command.clone();
    argv.push("run".to_owned());
    argv.push("--experiment".to_owned());
    argv.push(path_argument(&paths.experiment_resolved_json()));
    argv.push("--output".to_owned());
    argv.push(path_argument(&directory));
    Some(
        ToolSpec::new(
            argv,
            Duration::from_secs(request.resolved.budget.run_timeout_seconds),
        )
        .with_stdout_file(paths.adapter_stdout_log(name))
        .with_stderr_file(paths.adapter_stderr_log(name)),
    )
}

/// Records how a subject's process ended and worsens the status accordingly.
fn record_subject_exit(state: &mut AttemptState, phase: &str, run: SupervisedRun) {
    let detail = run.describe();
    if run.succeeded() {
        state.record(phase, PhaseOutcome::Succeeded, None);
        return;
    }
    state.record(phase, PhaseOutcome::Failed, Some(detail.clone()));
    let status = if run.exit.timed_out {
        ExecutionStatus::TimedOut
    } else if run.interrupted {
        // Order matters here. A subject that died on `SIGINT` because the
        // terminal signalled the whole process group arrives with a signal set
        // *and* the interrupt flag set, and it is an interruption, not a crash.
        // The supervisor is the one that decides which of those it was, by
        // consulting its latch after the child is reaped; this branch only has
        // to be asked first.
        state.interrupted = true;
        ExecutionStatus::Partial
    } else if run.exit.signal.is_some() {
        // A signal with no interrupt latched is a signal nobody in this process
        // asked for: a crash, not a policy.
        ExecutionStatus::Crashed
    } else {
        ExecutionStatus::Partial
    };
    state.degrade(status, format!("{phase}: {detail}"));
}

/// Renders a path as a command-line argument.
fn path_argument(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
