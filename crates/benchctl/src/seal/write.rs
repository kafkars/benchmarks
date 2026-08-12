//! The single funnel that puts a plan on disk, and the order it writes in.
//!
//! `status.json` is written first so that a seal which dies halfway still says
//! why. `checksums.txt` is written after every other document because it covers
//! them, and `bundle.json` last because it contains the checksum file's digest.
//!
//! That order has a consequence the failure paths must respect: a seal can fail
//! *after* it has already written a rich `status.json`. See
//! [`seal_failure`](super::seal_failure), which never overwrites one.

use std::path::Path;

use bench_schema::{
    BundleManifest, Classification, Comparison, ExecutionOrder, ExecutionStatus, RunStatus,
    SchemaResult, pretty_bytes,
};

use crate::attempt::AttemptPaths;
use crate::checksum::checksum_bundle;
use crate::error::{CtlError, CtlResult};

/// The documents one seal writes, in the order it writes them.
#[derive(Debug)]
pub(super) struct SealPlan {
    pub(super) status: RunStatus,
    pub(super) classification: Option<Classification>,
    pub(super) comparison: Option<Comparison>,
    pub(super) execution_order: Option<ExecutionOrder>,
}

/// The single funnel: writes every document, then the checksums, then the
/// manifest.
pub(super) fn seal(paths: &AttemptPaths, plan: &SealPlan) -> CtlResult<ExecutionStatus> {
    write_rendered(&paths.status_json(), pretty_bytes(&plan.status))?;
    if let Some(classification) = &plan.classification {
        // The verdict document is checked against its own schema before it is
        // written. Everything that builds one here satisfies the invariants by
        // construction, so this can only fire on a bug in this crate — and a bug
        // that seals a self-contradicting verdict is exactly the one worth
        // catching at the moment it would become evidence.
        classification
            .validate()
            .map_err(|error| seal_error(format!("the classification is malformed: {error}")))?;
        write_rendered(&paths.classification_json(), pretty_bytes(classification))?;
    }
    if let Some(comparison) = &plan.comparison {
        write_rendered(&paths.comparison_json(), pretty_bytes(comparison))?;
    }
    if let Some(order) = &plan.execution_order {
        write_rendered(&paths.execution_order_json(), pretty_bytes(order))?;
    }
    let manifest = checksum_bundle(paths.root())?;
    write_bytes(&paths.checksums_txt(), manifest.text.as_bytes())?;
    let bundle =
        BundleManifest::from_checksums_bytes(manifest.text.as_bytes(), manifest.total_bytes)
            .map_err(|error| seal_error(format!("build the bundle manifest: {error}")))?;
    write_rendered(&paths.bundle_json(), pretty_bytes(&bundle))?;
    Ok(plan.status.execution_status)
}

/// Writes an already-rendered document, reporting a render failure as a seal
/// failure.
///
/// The rendering is passed in rather than performed here because this crate does
/// not depend on `serde` directly; `bench_schema` owns the byte form of every
/// document, which is where that decision belongs anyway.
fn write_rendered(path: &Path, rendered: SchemaResult<Vec<u8>>) -> CtlResult<()> {
    let bytes =
        rendered.map_err(|error| seal_error(format!("render {}: {error}", path.display())))?;
    write_bytes(path, &bytes)
}

/// Writes bytes, reporting failure on stderr as well as to the caller.
fn write_bytes(path: &Path, bytes: &[u8]) -> CtlResult<()> {
    std::fs::write(path, bytes)
        .map_err(|error| seal_error(format!("write {}: {error}", path.display())))
}

/// Builds a seal error, announcing it on stderr because the caller may be about
/// to exit with nothing else to say.
fn seal_error(message: String) -> CtlError {
    eprintln!("benchctl: seal failure: {message}");
    CtlError::seal(message)
}
