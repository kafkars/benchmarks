//! `kafkars.run-status.v1` and `kafkars.execution-order.v1`: how one attempt
//! ended, and in what order it did things.
//!
//! Every attempt produces a status document, including the ones that crashed.
//! That is the always-seal rule, and it is why almost everything here is
//! optional: an attempt that died during probing has no experiment id, no
//! subject exits, and still has to be describable.
//!
//! Execution status is *not* validity. A run can complete perfectly and still
//! be invalid — a verifier found missing records, the environment was noisy,
//! the comparison was between mismatched configurations. Execution status
//! answers "did the machinery do what it was told"; validity is
//! [`Classification`](crate::Classification)'s question, and the two are kept in
//! separate documents so that neither can quietly stand in for the other.
//!
//! The four execution-status strings and their precedence are pinned by tests,
//! because the exit-code table of the control plane is derived from them.

use serde::{Deserialize, Serialize};

use crate::adapter::AdapterOutcome;
use crate::identity::ExperimentId;
use crate::schema_id::{EXECUTION_ORDER_V1, RUN_STATUS_V1};

/// How far an attempt got.
///
/// Worst wins when several phases disagree; see [`ExecutionStatus::worst`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    /// Every phase ran and every subject produced a result.
    Complete,
    /// Something was skipped or failed, and the attempt continued.
    Partial,
    /// A subject died on a signal, or the control plane itself panicked.
    Crashed,
    /// A deadline expired and the control plane killed what it was waiting on.
    TimedOut,
}

impl ExecutionStatus {
    /// Returns the wire string for this status.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Partial => "partial",
            Self::Crashed => "crashed",
            Self::TimedOut => "timed_out",
        }
    }

    /// Returns the precedence rank, higher being worse.
    ///
    /// The order is `timed_out` > `crashed` > `partial` > `complete`. A timeout
    /// outranks a crash because a killed process usually *also* looks like a
    /// crash, and the reason it died is the more useful fact.
    pub const fn severity(self) -> u8 {
        match self {
            Self::Complete => 0,
            Self::Partial => 1,
            Self::Crashed => 2,
            Self::TimedOut => 3,
        }
    }

    /// Returns the worse of two statuses.
    pub const fn worst(self, other: Self) -> Self {
        if other.severity() > self.severity() {
            other
        } else {
            self
        }
    }
}

/// How a child process ended.
///
/// `exit_code` and `signal` are mutually exclusive on Unix; both are absent
/// when the control plane never managed to spawn the process at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessExit {
    /// Exit status, when the process exited normally.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Signal number, when the process was killed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    /// Whether the control plane killed it for exceeding its deadline.
    pub timed_out: bool,
    /// Wall-clock milliseconds between spawn and reap.
    pub duration_ms: u64,
}

/// What a verification tool did for one topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationOutcome {
    /// Whether the tool ran at all. False means the check was skipped, which is
    /// a deferred check rather than a pass.
    pub ran: bool,
    /// The tool's verdict, when it produced one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid: Option<bool>,
    /// The tool's exit status, when it exited normally.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
}

/// Read-back verification for one subject's two topics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectVerification {
    /// Verification of the measured topic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measured: Option<VerificationOutcome>,
    /// Verification of the warmup topic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warmup: Option<VerificationOutcome>,
}

/// Everything the control plane observed about one subject's execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectExecution {
    /// Subject name, matching the resolved experiment.
    pub name: String,
    /// How the adapter process ended; absent when it never ran.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<ProcessExit>,
    /// What the adapter said about itself; absent when it wrote no status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adapter_outcome: Option<AdapterOutcome>,
    /// Whether a result document was found where one was expected.
    pub result_present: bool,
    /// Read-back verification for this subject's topics.
    pub verification: SubjectVerification,
}

/// Whether a phase of the attempt ran, and how it went.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhaseOutcome {
    /// The phase ran and did what it was supposed to.
    Succeeded,
    /// The phase ran and failed.
    Failed,
    /// The phase did not run.
    Skipped,
}

/// One phase of the attempt's state machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseRecord {
    /// Phase name, from the control plane's state machine.
    pub name: String,
    /// How the phase went.
    pub outcome: PhaseOutcome,
    /// One line of detail, when there is something worth saying.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// `kafkars.run-status.v1`: the control plane's account of one attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunStatus {
    /// Schema id, always [`RunStatus::SCHEMA`].
    pub schema: String,
    /// Experiment this attempt was of.
    ///
    /// Absent when the attempt failed before resolution produced an id, which
    /// is exactly the case the always-seal rule exists for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub experiment_id: Option<ExperimentId>,
    /// Attempt id, which also names the bundle directory.
    pub attempt_id: String,
    /// How far the attempt got.
    pub execution_status: ExecutionStatus,
    /// Why it did not complete, in one line.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    /// Whether a signal asked the control plane to stop.
    pub interrupted: bool,
    /// One entry per subject the experiment named.
    pub subjects: Vec<SubjectExecution>,
    /// Phases in the order they were attempted.
    pub phases: Vec<PhaseRecord>,
}

impl RunStatus {
    /// Schema id this document declares.
    pub const SCHEMA: &'static str = RUN_STATUS_V1;

    /// Reports whether the document declares the expected schema id.
    pub fn has_expected_schema(&self) -> bool {
        self.schema == Self::SCHEMA
    }
}

/// `kafkars.execution-order.v1`: which subject ran first, and why.
///
/// Order matters in a paired comparison — the second subject runs on a cluster
/// the first one warmed — so it is recorded rather than assumed, and the reason
/// is recorded too, because "the operator chose it" and "a coin flip derived
/// from the run id chose it" are very different claims.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionOrder {
    /// Schema id, always [`ExecutionOrder::SCHEMA`].
    pub schema: String,
    /// Subject names in execution order.
    pub order: Vec<String>,
    /// What decided the order — for example `run-id-coin-flip` or `operator`.
    pub decided_by: String,
}

impl ExecutionOrder {
    /// Schema id this document declares.
    pub const SCHEMA: &'static str = EXECUTION_ORDER_V1;

    /// Creates an execution-order document.
    pub fn new(order: Vec<String>, decided_by: impl Into<String>) -> Self {
        Self {
            schema: Self::SCHEMA.to_owned(),
            order,
            decided_by: decided_by.into(),
        }
    }

    /// Reports whether the document declares the expected schema id.
    pub fn has_expected_schema(&self) -> bool {
        self.schema == Self::SCHEMA
    }
}
