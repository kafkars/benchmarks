//! The funnel: one attempt in, one sealed bundle out, whatever happened in
//! between.
//!
//! Deriving the verdict — classification, comparison, and the percentiles they
//! read out of the subjects' own histograms — runs inside the panic boundary
//! too, because that derivation is the first code in the attempt to *interpret*
//! bytes an adapter wrote. A panic there is evidence-triggered, and
//! evidence-triggered failures are exactly the ones that must still seal.

use std::panic::AssertUnwindSafe;
use std::sync::Mutex;

use bench_schema::{ExecutionStatus, PhaseOutcome};

use crate::error::CtlResult;
use crate::interrupt;

use super::guard::SealOnDrop;
use super::phases::supervise_attempt;
use super::request::AttemptRequest;
use super::state::AttemptState;
use super::write::{SealPlan, seal};

/// Runs one attempt to completion and seals its evidence bundle.
///
/// Returns the sealed execution status, which the caller maps to a process exit
/// code. A returned status of `partial`, `crashed`, or `timed_out` is a
/// successful call: the bundle exists and says what went wrong.
///
/// # Errors
///
/// Returns a [`CtlErrorKind::Seal`](crate::CtlErrorKind::Seal) error only when
/// the bundle could not be written. Everything else that can go wrong is
/// recorded in the bundle instead.
#[expect(
    clippy::needless_pass_by_value,
    reason = "the seam takes the request by value so that nothing can mutate it while the \
              attempt is in flight"
)]
pub fn run_attempt(request: AttemptRequest) -> CtlResult<ExecutionStatus> {
    let paths = request.paths.clone();
    let mut guard = SealOnDrop::arm(&paths);
    let interrupt = interrupt::process_latch();
    let state = Mutex::new(AttemptState::new());
    let panic = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let mut held = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        supervise_attempt(&request, &mut held, &interrupt);
    }))
    .err();
    let mut state = state
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(payload) = panic {
        let reason = describe_panic(&*payload);
        state.record("supervise", PhaseOutcome::Failed, Some(reason.clone()));
        state.degrade(
            ExecutionStatus::Crashed,
            format!("the control plane panicked: {reason}"),
        );
    }
    if interrupt.is_set() {
        state.interrupted = true;
        state.degrade(ExecutionStatus::Partial, "the run was interrupted");
    }
    let plan = derive_plan(&mut state, &request, AttemptState::plan);
    let sealed = seal(&paths, &plan)?;
    guard.disarm();
    Ok(sealed)
}

/// Runs `derive` inside the panic boundary, falling back to a bundle that says
/// the verdict could not be derived.
///
/// [`AttemptState::plan`] — the `derive` every caller but the tests passes — is
/// where the attempt stops recording facts and starts interpreting them: it
/// decodes each subject's histograms to take a percentile, which makes it the
/// first place a document an adapter wrote reaches code that could give up on
/// it. Running it outside the panic boundary would send an evidence-triggered
/// panic all the way out of [`run_attempt`], past the funnel, into a stub.
/// Running it inside means the same panic seals `crashed` with every subject
/// record the attempt collected still in the bundle.
///
/// The derivation arrives as a parameter rather than being called directly so
/// that the boundary can be tested for the property it exists for. A panic here
/// is by definition a bug nobody has written yet, and a guard nobody can
/// exercise is a guard nobody can trust.
pub(super) fn derive_plan(
    state: &mut AttemptState,
    request: &AttemptRequest,
    derive: fn(&AttemptState, &AttemptRequest) -> SealPlan,
) -> SealPlan {
    match std::panic::catch_unwind(AssertUnwindSafe(|| derive(state, request))) {
        Ok(plan) => plan,
        Err(payload) => {
            let reason = describe_panic(&*payload);
            eprintln!("benchctl: deriving the verdict panicked: {reason}");
            state.record("classify", PhaseOutcome::Failed, Some(reason.clone()));
            state.degrade(
                ExecutionStatus::Crashed,
                format!("deriving the verdict panicked: {reason}"),
            );
            state.undecided_plan(request, &reason)
        }
    }
}

/// Extracts a readable reason from a caught panic payload.
fn describe_panic(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|text| (*text).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "a panic with no message".to_owned())
}
