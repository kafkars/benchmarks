//! How an attempt ended, from the caller's point of view, and the two sealed
//! documents a caller reads back out of the bundle.

use bench_schema::{Classification, ExecutionStatus, ExperimentId, RunStatus};

use crate::attempt::AttemptPaths;
use crate::error::CtlError;

/// One attempt that reached a sealed bundle.
#[derive(Debug, Clone)]
pub struct SealedAttempt {
    /// Where the bundle is.
    pub paths: AttemptPaths,
    /// How the attempt ended.
    pub status: ExecutionStatus,
    /// The experiment every repetition of this intent shares, when resolution
    /// got far enough to compute one.
    pub experiment_id: Option<ExperimentId>,
    /// The process exit code this ending maps to.
    pub exit_code: i32,
}

impl SealedAttempt {
    /// Whether the sealed classification says the evidence may be believed.
    ///
    /// Read back from the bundle rather than remembered from the run: the
    /// document on disk is the evidence, and a suite that trusted its own memory
    /// could report a validity the bundle does not contain.
    #[must_use]
    pub fn run_valid(&self) -> bool {
        read_classification(&self.paths).is_some_and(|document| document.run_valid)
    }
}

/// How an attempt ended, from the caller's point of view.
#[derive(Debug, Clone)]
pub enum AttemptEnd {
    /// A bundle exists and says what happened.
    Sealed(SealedAttempt),
    /// Nothing could be sealed, because there was nowhere to seal into.
    Unsealable(CtlError),
}

impl AttemptEnd {
    /// The process exit code this ending maps to.
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Sealed(sealed) => sealed.exit_code,
            Self::Unsealable(error) => error.exit_code(),
        }
    }

    /// The sealed attempt, when there is one.
    #[must_use]
    pub fn sealed(&self) -> Option<&SealedAttempt> {
        match self {
            Self::Sealed(sealed) => Some(sealed),
            Self::Unsealable(_) => None,
        }
    }
}

/// Reads a bundle's sealed classification, when it has a readable one.
///
/// Validated, not merely deserialized. The callers of this function ask it one
/// question — may this attempt's evidence be believed — and a document that
/// contradicts its own schema is not an answer to it. Returning `None` for such
/// a document makes every caller treat it as "not believable", which is the
/// direction a reader of damaged evidence should fail in.
#[must_use]
pub fn read_classification(paths: &AttemptPaths) -> Option<Classification> {
    let bytes = std::fs::read(paths.classification_json()).ok()?;
    match Classification::from_slice(&bytes) {
        Ok(document) => Some(document),
        Err(error) => {
            eprintln!(
                "benchctl: {} is not a usable classification: {error}",
                paths.classification_json().display()
            );
            None
        }
    }
}

/// Reads a bundle's sealed run status, when it has a readable one.
#[must_use]
pub fn read_status(paths: &AttemptPaths) -> Option<RunStatus> {
    let bytes = std::fs::read(paths.status_json()).ok()?;
    bench_schema::parse_json_slice::<RunStatus>(&bytes).ok()
}
