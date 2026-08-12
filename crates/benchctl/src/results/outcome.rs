//! Everything the verdict needs to know about one subject: how its process
//! ended, what its documents said, and what the read-back verifier concluded.

use bench_schema::{ProcessExit, SloSpec, SubjectExecution, SubjectVerification};

use super::evidence::SubjectEvidence;

/// What a read-back verification concluded about one topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationVerdict {
    /// Nothing was produced to this topic, so there was nothing to read back.
    NotRequired,
    /// A verification was expected and did not happen.
    NotRun,
    /// The verifier ran and the topic did not satisfy the contract.
    Failed,
    /// The verifier ran and the topic satisfied the contract.
    Satisfied,
}

impl VerificationVerdict {
    /// Reports whether this verdict permits the subject to be believed.
    #[must_use]
    pub const fn permits_validity(self) -> bool {
        matches!(self, Self::NotRequired | Self::Satisfied)
    }
}

/// Everything the verdict needs to know about one subject.
#[derive(Debug, Clone, PartialEq)]
pub struct SubjectOutcome {
    /// Subject name, matching the resolved experiment.
    pub name: String,
    /// How the adapter process ended; absent when it never ran.
    pub execution: Option<ProcessExit>,
    /// Whether the supervisor killed it in response to an interrupt.
    pub interrupted: bool,
    /// What the subject's own documents say.
    pub evidence: SubjectEvidence,
    /// Verification outcomes as the run status records them.
    pub verification: SubjectVerification,
    /// Whether the measured topic satisfied its contract.
    pub measured: VerificationVerdict,
    /// Whether the warmup topic satisfied its contract.
    pub warmup: VerificationVerdict,
    /// The objectives the experiment declared, empty when it declared none.
    pub slo: SloSpec,
    /// The adapter version this subject claimed at probe time, as the resolved
    /// experiment records it. Empty when the subject never got that far.
    ///
    /// Kept beside the measurement so the two can be compared: this string is
    /// hashed into the experiment id, and the result document's is written at
    /// run time by the client itself.
    pub declared_adapter_version: String,
}

impl SubjectOutcome {
    /// Creates an outcome for a subject that never ran.
    #[must_use]
    pub fn skipped(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            execution: None,
            interrupted: false,
            evidence: SubjectEvidence::default(),
            verification: SubjectVerification::default(),
            measured: VerificationVerdict::NotRun,
            warmup: VerificationVerdict::NotRun,
            slo: SloSpec::default(),
            declared_adapter_version: String::new(),
        }
    }

    /// The run-status view of this subject.
    #[must_use]
    pub fn execution_record(&self) -> SubjectExecution {
        SubjectExecution {
            name: self.name.clone(),
            execution: self.execution,
            adapter_outcome: self.evidence.adapter_outcome(),
            result_present: self.evidence.result_present,
            verification: self.verification,
        }
    }
}
