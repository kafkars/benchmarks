//! Everything one attempt is asked to do, and the two facts every sealing path
//! reads back off it.

use std::time::SystemTime;

use bench_schema::{ClusterTools, EnvironmentDocument, ResolvedExperiment, SubjectsLock};

use crate::attempt::AttemptPaths;

/// Everything one attempt needs, once resolution and probing have succeeded.
///
/// The workspace is already finalized when this arrives: creating it, claiming
/// it, and moving it under the experiment id are the caller's job, because those
/// are the failures that happen *before* there is anywhere to seal into. From
/// here on, every failure produces a bundle.
#[derive(Debug, Clone)]
pub struct AttemptRequest {
    /// The resolved experiment, with its runtime binding present.
    pub resolved: ResolvedExperiment,
    /// What each subject was at probe time.
    pub lock: SubjectsLock,
    /// The human-authored scenario, sealed verbatim.
    pub source_toml: String,
    /// The machine the attempt is running on.
    pub environment: EnvironmentDocument,
    /// Argument-vector prefixes for the configured cluster tools.
    pub tools: ClusterTools,
    /// The finalized bundle layout.
    pub paths: AttemptPaths,
    /// When the attempt started, for the status document.
    pub started_at: SystemTime,
}

/// The attempt id, taken from the directory the bundle lives in.
pub(super) fn attempt_id_of(paths: &AttemptPaths) -> String {
    paths.root().file_name().map_or_else(
        || "unknown".to_owned(),
        |name| name.to_string_lossy().into_owned(),
    )
}

/// The subject order the runtime binding fixed.
pub(super) fn execution_order(experiment: &ResolvedExperiment) -> Vec<String> {
    experiment
        .runtime
        .as_ref()
        .map(|runtime| runtime.execution_order.clone())
        .unwrap_or_default()
}
