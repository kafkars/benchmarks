//! The control plane's single failure type and the exit-code table that maps
//! every way an invocation can end to a distinct, documented process exit.
//!
//! Two different vocabularies end a `benchctl` process. A *sealed outcome* is
//! the normal case — even a crashed attempt seals a bundle — and exits with the
//! code for its [`ExecutionStatus`]. A [`CtlError`] is the abnormal case: the
//! invocation itself was wrong, or sealing was impossible. The table keeps the
//! two ranges disjoint so a caller can distinguish "the attempt failed and the
//! evidence says why" from "there is no evidence to read".

use std::fmt;

use bench_schema::{ExecutionStatus, SchemaError};

/// Exit code for a sealed attempt whose execution status is `complete`.
pub const EXIT_SEALED_COMPLETE: i32 = 0;
/// Exit code for a sealed attempt whose execution status is `partial`.
pub const EXIT_SEALED_PARTIAL: i32 = 20;
/// Exit code for a sealed attempt whose execution status is `crashed`.
pub const EXIT_SEALED_CRASHED: i32 = 21;
/// Exit code for a sealed attempt whose execution status is `timed_out`.
pub const EXIT_SEALED_TIMED_OUT: i32 = 22;
/// Exit code for a usage error: unknown verb, malformed or missing flags.
pub const EXIT_USAGE: i32 = 64;
/// Exit code for invalid input discovered before an attempt workspace exists.
pub const EXIT_INVALID: i32 = 65;
/// Exit code for an internal fault, including a caught panic.
pub const EXIT_INTERNAL: i32 = 70;
/// Exit code when the attempt directory already exists and cannot be claimed.
pub const EXIT_ATTEMPT_EXISTS: i32 = 73;
/// Exit code when sealing itself failed to write evidence.
pub const EXIT_SEAL_WRITE: i32 = 74;

/// Returns the process exit code for a sealed attempt's execution status.
#[must_use]
pub fn exit_code_for_status(status: ExecutionStatus) -> i32 {
    match status {
        ExecutionStatus::Complete => EXIT_SEALED_COMPLETE,
        ExecutionStatus::Partial => EXIT_SEALED_PARTIAL,
        ExecutionStatus::Crashed => EXIT_SEALED_CRASHED,
        ExecutionStatus::TimedOut => EXIT_SEALED_TIMED_OUT,
    }
}

/// What kind of abnormal ending a [`CtlError`] reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CtlErrorKind {
    /// The command line could not be understood.
    Usage,
    /// The experiment, subjects, or cluster input was invalid before any
    /// attempt workspace existed.
    InvalidExperiment,
    /// A named input file was missing, unreadable, or not the document the flag
    /// asked for.
    ///
    /// Shares [`EXIT_INVALID`] with [`Self::InvalidExperiment`], because from a
    /// caller's point of view both are "the inputs were wrong before anything
    /// ran". They are separate kinds only so the *message* can be true: a
    /// missing `--llm-summary` file reported as an invalid experiment sends a
    /// reader to look at their scenario, which is the one file that was fine.
    InvalidInput,
    /// The attempt directory already exists; refusing to reuse evidence paths.
    AttemptExists,
    /// Sealing failed to write evidence; the primary failure is already on
    /// stderr and, when possible, in `status.json`.
    Seal,
    /// An internal fault in the control plane itself.
    Internal,
}

/// The control plane's failure type: a kind plus human-readable context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CtlError {
    kind: CtlErrorKind,
    message: String,
}

impl CtlError {
    /// Creates an error with an explicit kind and message.
    #[must_use]
    pub fn new(kind: CtlErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Creates a [`CtlErrorKind::Usage`] error.
    #[must_use]
    pub fn usage(message: impl Into<String>) -> Self {
        Self::new(CtlErrorKind::Usage, message)
    }

    /// Creates a [`CtlErrorKind::InvalidExperiment`] error.
    #[must_use]
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(CtlErrorKind::InvalidExperiment, message)
    }

    /// Creates a [`CtlErrorKind::InvalidInput`] error naming the flag that
    /// carried the offending path.
    #[must_use]
    pub fn invalid_input(flag: &str, message: impl std::fmt::Display) -> Self {
        Self::new(CtlErrorKind::InvalidInput, format!("--{flag}: {message}"))
    }

    /// Creates a [`CtlErrorKind::AttemptExists`] error.
    #[must_use]
    pub fn attempt_exists(message: impl Into<String>) -> Self {
        Self::new(CtlErrorKind::AttemptExists, message)
    }

    /// Creates a [`CtlErrorKind::Seal`] error.
    #[must_use]
    pub fn seal(message: impl Into<String>) -> Self {
        Self::new(CtlErrorKind::Seal, message)
    }

    /// Creates a [`CtlErrorKind::Internal`] error.
    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(CtlErrorKind::Internal, message)
    }

    /// The kind of failure.
    #[must_use]
    pub fn kind(&self) -> CtlErrorKind {
        self.kind
    }

    /// The human-readable context.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// The process exit code this error maps to.
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        match self.kind {
            CtlErrorKind::Usage => EXIT_USAGE,
            CtlErrorKind::InvalidExperiment | CtlErrorKind::InvalidInput => EXIT_INVALID,
            CtlErrorKind::AttemptExists => EXIT_ATTEMPT_EXISTS,
            CtlErrorKind::Seal => EXIT_SEAL_WRITE,
            CtlErrorKind::Internal => EXIT_INTERNAL,
        }
    }
}

impl fmt::Display for CtlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self.kind {
            CtlErrorKind::Usage => "usage",
            CtlErrorKind::InvalidExperiment => "invalid experiment",
            CtlErrorKind::InvalidInput => "invalid input",
            CtlErrorKind::AttemptExists => "attempt exists",
            CtlErrorKind::Seal => "seal",
            CtlErrorKind::Internal => "internal",
        };
        write!(f, "{kind}: {}", self.message)
    }
}

impl std::error::Error for CtlError {}

impl From<SchemaError> for CtlError {
    /// Schema failures reaching the control plane mean the input documents were
    /// invalid; they map to the pre-attempt invalid slot.
    fn from(error: SchemaError) -> Self {
        Self::invalid(error.to_string())
    }
}

/// Shorthand result type for control-plane operations.
pub type CtlResult<T> = Result<T, CtlError>;
