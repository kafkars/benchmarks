//! The phase machine of one attempt: the sealed inputs, the topic lifecycle,
//! and the loop over subjects.
//!
//! Records everything; returns nothing; never gives up early except where
//! continuing would produce noise instead of evidence.

use bench_schema::{ExecutionStatus, PhaseOutcome};

use crate::interrupt::InterruptFlag;
use crate::results::SubjectOutcome;
use crate::supervise::SupervisedRun;
use crate::{topics, utc_rfc3339_millis};

use super::request::{AttemptRequest, execution_order};
use super::state::AttemptState;
use super::subject::run_subject;

/// The phase machine of one attempt. Records everything; returns nothing;
/// never gives up early except where continuing would produce noise instead of
/// evidence.
pub(super) fn supervise_attempt(
    request: &AttemptRequest,
    state: &mut AttemptState,
    interrupt: &InterruptFlag,
) {
    seal_inputs(request, state);
    let order = execution_order(&request.resolved);
    if order.is_empty() {
        state.record(
            "subjects",
            PhaseOutcome::Skipped,
            Some("the resolved experiment carries no runtime execution order".to_owned()),
        );
        state.degrade(
            ExecutionStatus::Partial,
            "the resolved experiment carries no runtime execution order",
        );
        return;
    }
    if !create_topics(request, state, interrupt) {
        // Topic creation is a precondition, not a phase: subjects run against
        // topics whose geometry the control plane chose, and a subject that
        // produces to a topic nobody created is producing noise. The attempt
        // stops here and seals what it has.
        for name in &order {
            state.subjects.push(SubjectOutcome::skipped(name));
            state.record(
                format!("subject:{name}:run"),
                PhaseOutcome::Skipped,
                Some("topic creation failed".to_owned()),
            );
        }
        state.invalidate("no subject ran because topic creation failed");
        cleanup_topics(request, state, interrupt);
        return;
    }
    for name in &order {
        let outcome = run_subject(request, state, interrupt, name);
        state.subjects.push(outcome);
    }
    cleanup_topics(request, state, interrupt);
}

/// Writes the four sealed inputs: the scenario verbatim, and the three resolved
/// documents.
fn seal_inputs(request: &AttemptRequest, state: &mut AttemptState) {
    let paths = &request.paths;
    let mut failures = Vec::new();
    if let Err(error) = std::fs::write(
        paths.experiment_source_toml(),
        request.source_toml.as_bytes(),
    ) {
        failures.push(format!("experiment.source.toml: {error}"));
    }
    for (path, bytes) in [
        (
            paths.experiment_resolved_json(),
            bench_schema::pretty_bytes(&request.resolved),
        ),
        (
            paths.subjects_lock_json(),
            bench_schema::pretty_bytes(&request.lock),
        ),
        (
            paths.environment_json(),
            bench_schema::pretty_bytes(&request.environment),
        ),
    ] {
        match bytes {
            Ok(bytes) => {
                if let Err(error) = std::fs::write(&path, &bytes) {
                    failures.push(format!("{}: {error}", path.display()));
                }
            }
            Err(error) => failures.push(format!("{}: {error}", path.display())),
        }
    }
    if failures.is_empty() {
        // The run status has no timestamp field and the attempt id carries only
        // whole seconds, so the first phase record is where the attempt's start
        // is stated to the precision it was taken at.
        state.record(
            "seal-inputs",
            PhaseOutcome::Succeeded,
            Some(format!(
                "attempt started at {}",
                utc_rfc3339_millis(request.started_at)
            )),
        );
    } else {
        let detail = failures.join("; ");
        state.record("seal-inputs", PhaseOutcome::Failed, Some(detail.clone()));
        state.degrade(
            ExecutionStatus::Partial,
            format!("sealed inputs incomplete: {detail}"),
        );
        state.invalidate(format!(
            "the attempt could not seal its own inputs: {detail}"
        ));
    }
}

/// Creates every topic. Returns whether the attempt may proceed to its subjects.
fn create_topics(
    request: &AttemptRequest,
    state: &mut AttemptState,
    interrupt: &InterruptFlag,
) -> bool {
    if request.tools.topic_create.is_empty() {
        // An operator who configured no topic tool is opting out, not failing:
        // the topics exist by some other arrangement. The skipped check is
        // recorded, and the run is partial because nothing verified it.
        state.record(
            "topic-create",
            PhaseOutcome::Skipped,
            Some("no topic-create tool is configured".to_owned()),
        );
        state.degrade(
            ExecutionStatus::Partial,
            "topic creation was not configured",
        );
        state.invalidate("topic geometry was not established by the control plane");
        return true;
    }
    match topics::create(&request.paths, &request.tools, &request.resolved, interrupt) {
        Ok(outcome) if outcome.succeeded() => {
            state.record("topic-create", PhaseOutcome::Succeeded, None);
            true
        }
        Ok(outcome) => {
            let detail = format!("the topic tool {}", outcome.describe());
            state.record("topic-create", PhaseOutcome::Failed, Some(detail.clone()));
            state.degrade(
                topic_failure_status(&outcome),
                format!("topic creation failed: {detail}"),
            );
            false
        }
        Err(error) => {
            let detail = error.to_string();
            state.record("topic-create", PhaseOutcome::Failed, Some(detail.clone()));
            state.degrade(
                ExecutionStatus::Partial,
                format!("topic creation failed: {detail}"),
            );
            false
        }
    }
}

/// Deletes every topic, best effort: a failure is recorded and never changes the
/// execution status.
fn cleanup_topics(request: &AttemptRequest, state: &mut AttemptState, interrupt: &InterruptFlag) {
    if request.tools.topic_delete.is_empty() {
        state.record(
            "topic-cleanup",
            PhaseOutcome::Skipped,
            Some("no topic-delete tool is configured".to_owned()),
        );
        return;
    }
    let detail = match topics::delete(&request.paths, &request.tools, &request.resolved, interrupt)
    {
        Ok(outcome) if outcome.succeeded() => {
            state.record("topic-cleanup", PhaseOutcome::Succeeded, None);
            return;
        }
        Ok(outcome) => format!("the topic tool {}", outcome.describe()),
        Err(error) => error.to_string(),
    };
    eprintln!("benchctl: topic cleanup failed, leaving topics behind: {detail}");
    state.record("topic-cleanup", PhaseOutcome::Failed, Some(detail));
}

/// Maps a failed topic tool onto an execution status.
fn topic_failure_status(outcome: &SupervisedRun) -> ExecutionStatus {
    if outcome.exit.timed_out {
        ExecutionStatus::TimedOut
    } else {
        ExecutionStatus::Partial
    }
}
