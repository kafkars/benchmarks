//! Read-back verification of one subject's two topics, and the difference
//! between a check that could not run and a check that said no.
//!
//! A verifier that ran cleanly and found missing records is a *failed phase* —
//! the check did not pass — but not a broken machine, so the execution status
//! stays where it was and the verdict lands in `classification.json`.

use bench_schema::{ExecutionStatus, PhaseOutcome};

use crate::interrupt::InterruptFlag;
use crate::results::{SubjectOutcome, VerificationVerdict};
use crate::verify::{self, VerificationPhase};

use super::request::AttemptRequest;
use super::state::AttemptState;

/// Verifies one of a subject's topics, recording the phase and the verdict.
pub(super) fn verify_topic(
    request: &AttemptRequest,
    state: &mut AttemptState,
    interrupt: &InterruptFlag,
    outcome: &mut SubjectOutcome,
    phase: VerificationPhase,
) {
    let name = outcome.name.clone();
    let label = format!("subject:{name}:verify-{}", phase.as_str());
    let verdict = if request.tools.verify.is_empty() {
        state.record(
            &label,
            PhaseOutcome::Skipped,
            Some("no verifier is configured".to_owned()),
        );
        state.degrade(
            ExecutionStatus::Partial,
            "read-back verification was not configured",
        );
        VerificationVerdict::NotRun
    } else if phase == VerificationPhase::Warmup && request.resolved.warmup_records == 0 {
        state.record(
            &label,
            PhaseOutcome::Succeeded,
            Some("the experiment produces no warmup records".to_owned()),
        );
        VerificationVerdict::NotRequired
    } else {
        run_verifier(request, state, interrupt, outcome, phase, &label)
    };
    match phase {
        VerificationPhase::Measured => outcome.measured = verdict,
        VerificationPhase::Warmup => outcome.warmup = verdict,
    }
}

/// Runs the configured verifier for one topic.
fn run_verifier(
    request: &AttemptRequest,
    state: &mut AttemptState,
    interrupt: &InterruptFlag,
    outcome: &mut SubjectOutcome,
    phase: VerificationPhase,
    label: &str,
) -> VerificationVerdict {
    match verify::verify(
        &request.paths,
        &request.tools,
        &request.resolved,
        &outcome.name,
        phase,
        interrupt,
    ) {
        Ok(run) => {
            match phase {
                VerificationPhase::Measured => outcome.verification.measured = Some(run.outcome),
                VerificationPhase::Warmup => outcome.verification.warmup = Some(run.outcome),
            }
            if !run.tool_succeeded {
                state.record(label, PhaseOutcome::Failed, Some(run.detail.clone()));
                state.degrade(ExecutionStatus::Partial, format!("{label}: {}", run.detail));
                return VerificationVerdict::NotRun;
            }
            if run.satisfies_contract {
                state.record(label, PhaseOutcome::Succeeded, Some(run.detail));
                VerificationVerdict::Satisfied
            } else {
                // The machinery worked and the answer was no: a failed check,
                // not a failed run.
                state.record(label, PhaseOutcome::Failed, Some(run.detail));
                VerificationVerdict::Failed
            }
        }
        Err(error) => {
            let detail = error.to_string();
            state.record(label, PhaseOutcome::Failed, Some(detail.clone()));
            state.degrade(ExecutionStatus::Partial, format!("{label}: {detail}"));
            VerificationVerdict::NotRun
        }
    }
}
