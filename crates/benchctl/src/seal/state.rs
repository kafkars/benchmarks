//! Everything the attempt has learned so far, and the two plans it can turn
//! into.

use bench_schema::{
    Classification, ExecutionOrder, ExecutionStatus, PhaseOutcome, PhaseRecord, RunStatus,
};

use crate::results::{self, SubjectOutcome};

use super::request::{AttemptRequest, attempt_id_of, execution_order};
use super::write::SealPlan;

/// Everything the attempt has learned so far.
#[derive(Debug)]
pub(super) struct AttemptState {
    pub(super) status: ExecutionStatus,
    pub(super) failure_reason: Option<String>,
    pub(super) interrupted: bool,
    pub(super) phases: Vec<PhaseRecord>,
    pub(super) subjects: Vec<SubjectOutcome>,
    pub(super) invalidating: Vec<String>,
}

impl AttemptState {
    pub(super) fn new() -> Self {
        Self {
            status: ExecutionStatus::Complete,
            failure_reason: None,
            interrupted: false,
            phases: Vec::new(),
            subjects: Vec::new(),
            invalidating: Vec::new(),
        }
    }

    /// Records one phase of the state machine.
    pub(super) fn record(
        &mut self,
        name: impl Into<String>,
        outcome: PhaseOutcome,
        detail: Option<String>,
    ) {
        self.phases.push(PhaseRecord {
            name: name.into(),
            outcome,
            detail,
        });
    }

    /// Worsens the execution status, keeping the reason that caused the worst
    /// status seen so far.
    pub(super) fn degrade(&mut self, status: ExecutionStatus, reason: impl Into<String>) {
        let worse = status.severity() > self.status.severity();
        if worse || (status == self.status && self.failure_reason.is_none()) {
            self.failure_reason = Some(reason.into());
        }
        self.status = self.status.worst(status);
    }

    /// Records a reason the run's evidence may not be believed, separate from
    /// whether the machinery worked.
    pub(super) fn invalidate(&mut self, reason: impl Into<String>) {
        self.invalidating.push(reason.into());
    }

    /// The run status this state describes: pure record-keeping, nothing
    /// derived from a subject's own bytes, so nothing here can be made to fail
    /// by the evidence.
    fn run_status(&self, request: &AttemptRequest) -> RunStatus {
        RunStatus {
            schema: RunStatus::SCHEMA.to_owned(),
            experiment_id: bench_schema::experiment_id(&request.resolved).ok(),
            attempt_id: attempt_id_of(&request.paths),
            execution_status: self.status,
            failure_reason: self.failure_reason.clone(),
            interrupted: self.interrupted,
            subjects: self
                .subjects
                .iter()
                .map(SubjectOutcome::execution_record)
                .collect(),
            phases: self.phases.clone(),
        }
    }

    /// Turns the accumulated state into the documents to seal.
    ///
    /// Borrows rather than consumes so that [`derive_plan`](super::driver::derive_plan)
    /// still holds the state if this panics, and can seal the part of it that
    /// never depended on reading a subject's evidence.
    pub(super) fn plan(&self, request: &AttemptRequest) -> SealPlan {
        let order = execution_order(&request.resolved);
        SealPlan {
            status: self.run_status(request),
            classification: Some(results::classify(&self.subjects, &self.invalidating)),
            comparison: Some(results::compare(&order, &self.subjects)),
            execution_order: Some(ExecutionOrder::new(
                order,
                "resolved-experiment-runtime-binding",
            )),
        }
    }

    /// The plan for an attempt whose verdict could not be derived at all.
    ///
    /// The status is the full record — every subject, every phase — because
    /// that is precisely the evidence a reader needs when the derivation over it
    /// is the thing that broke. The classification names the panic and refuses
    /// the run; the comparison is absent, because computing it is what failed,
    /// and a comparison invented here would be the fabrication the whole
    /// always-seal design exists to prevent.
    pub(super) fn undecided_plan(&self, request: &AttemptRequest, reason: &str) -> SealPlan {
        SealPlan {
            status: self.run_status(request),
            classification: Some(Classification {
                schema: Classification::SCHEMA.to_owned(),
                run_valid: false,
                claim_eligible: false,
                subjects: Vec::new(),
                deferred_checks: results::DEFERRED_CHECKS
                    .iter()
                    .map(|check| (*check).to_owned())
                    .collect(),
                reasons: vec![format!(
                    "the attempt's verdict could not be derived: {reason}"
                )],
            }),
            comparison: None,
            execution_order: Some(ExecutionOrder::new(
                execution_order(&request.resolved),
                "resolved-experiment-runtime-binding",
            )),
        }
    }
}
