//! Sealing a bundle for an attempt that failed before — or after — it could
//! run, and completing the terminal files over whatever a bundle already holds.

use std::time::SystemTime;

use bench_schema::{BundleManifest, ExecutionStatus, PhaseOutcome, PhaseRecord, RunStatus};

use crate::attempt::AttemptPaths;
use crate::checksum::checksum_bundle;
use crate::error::CtlResult;
use crate::results;
use crate::utc_rfc3339_millis;

use super::guard::SealOnDrop;
use super::request::attempt_id_of;
use super::write::{SealPlan, seal};

/// Seals a bundle for an attempt that failed before it could run.
///
/// Used by the phases that happen before [`run_attempt`](super::run_attempt) —
/// reading the sources, probing the subjects, resolving the experiment,
/// capturing the environment — so that a failure there still leaves a bundle
/// behind rather than an empty directory. The source TOML is sealed when the
/// caller managed to read it, because "the file we could not use" is the most
/// useful thing such a bundle can carry.
///
/// # When the bundle is already sealed
///
/// The pipeline also calls this when a failure is reported *after* an attempt
/// ran, including a seal that died partway through its own write order. In that
/// case `status.json` already exists and already says more than this function
/// could: the stub below carries no subjects and one invented phase. It is
/// therefore kept, the reason is recorded beside it in
/// [`seal_failure_txt`](AttemptPaths::seal_failure_txt), and the terminal files
/// are completed over whatever the bundle holds. Overwriting richer evidence
/// with poorer evidence is not a fallback, it is a loss.
///
/// # Errors
///
/// As for [`run_attempt`](super::run_attempt): only a failure to write the
/// bundle. The already-sealed path never errors — there is a bundle either way,
/// and the failure that brought us here is the news.
pub fn seal_failure(
    paths: &AttemptPaths,
    source_toml: Option<&str>,
    status: ExecutionStatus,
    failure_reason: &str,
) -> CtlResult<ExecutionStatus> {
    if paths.status_json().exists() {
        return Ok(preserve_sealed_evidence(paths, status, failure_reason));
    }
    let mut guard = SealOnDrop::arm(paths);
    if let Some(text) = source_toml {
        if let Err(error) = std::fs::write(paths.experiment_source_toml(), text.as_bytes()) {
            eprintln!("benchctl: could not seal the source scenario: {error}");
        }
    }
    let plan = SealPlan {
        status: RunStatus {
            schema: RunStatus::SCHEMA.to_owned(),
            experiment_id: None,
            attempt_id: attempt_id_of(paths),
            execution_status: status,
            failure_reason: Some(failure_reason.to_owned()),
            interrupted: false,
            subjects: Vec::new(),
            phases: vec![PhaseRecord {
                name: "pre-attempt".to_owned(),
                outcome: PhaseOutcome::Failed,
                detail: Some(failure_reason.to_owned()),
            }],
        },
        classification: Some(results::classify(&[], &[failure_reason.to_owned()])),
        comparison: None,
        execution_order: None,
    };
    let sealed = seal(paths, &plan)?;
    guard.disarm();
    Ok(sealed)
}

/// Keeps an already-sealed `status.json` and finishes the bundle around it.
///
/// Returns the execution status the bundle itself records, read back from disk
/// rather than remembered, because the document is the evidence. `fallback` is
/// used only when that document cannot be parsed — at which point the caller's
/// idea of how the attempt went is the best answer available.
///
/// Nothing already in the bundle is rewritten, including the sealed scenario: a
/// bundle that got this far sealed its own inputs, and rewriting a covered file
/// would put it at odds with a `checksums.txt` that may already exist. Only the
/// note is new, and it is written *before* the terminal files so that the
/// manifest covers it like everything else.
fn preserve_sealed_evidence(
    paths: &AttemptPaths,
    fallback: ExecutionStatus,
    failure_reason: &str,
) -> ExecutionStatus {
    eprintln!(
        "benchctl: {} is already sealed; keeping its status and recording the later failure \
         beside it",
        paths.root().display()
    );
    let note = format!(
        "sealing did not finish at {}\n{failure_reason}\n",
        utc_rfc3339_millis(SystemTime::now())
    );
    if let Err(error) = std::fs::write(paths.seal_failure_txt(), note.as_bytes()) {
        eprintln!("benchctl: could not record the seal failure in the bundle: {error}");
    }
    write_missing_terminal_files(paths);
    crate::pipeline::read_status(paths).map_or(fallback, |status| status.execution_status)
}

/// Writes `checksums.txt` and `bundle.json` when they are absent, best effort.
///
/// Shared by the last-resort guard and by [`seal_failure`]'s preservation path,
/// which want exactly the same thing: complete the bundle over whatever is
/// there, and never disturb terminal files a successful seal already wrote.
/// Every error is reported and swallowed — this only runs where something else
/// has already failed, and that failure is the one worth exiting on.
pub(super) fn write_missing_terminal_files(paths: &AttemptPaths) {
    if paths.checksums_txt().exists() && paths.bundle_json().exists() {
        return;
    }
    let manifest = match checksum_bundle(paths.root()) {
        Ok(manifest) => manifest,
        Err(error) => {
            eprintln!("benchctl: the bundle could not be checksummed: {error}");
            return;
        }
    };
    if let Err(error) = std::fs::write(paths.checksums_txt(), manifest.text.as_bytes()) {
        eprintln!("benchctl: the bundle's checksums could not be written: {error}");
        return;
    }
    let bundle =
        BundleManifest::from_checksums_bytes(manifest.text.as_bytes(), manifest.total_bytes);
    match bundle.as_ref().map(bench_schema::pretty_bytes) {
        Ok(Ok(bytes)) => {
            if let Err(error) = std::fs::write(paths.bundle_json(), &bytes) {
                eprintln!("benchctl: the bundle manifest could not be written: {error}");
            }
        }
        Ok(Err(error)) => eprintln!("benchctl: the bundle manifest could not be rendered: {error}"),
        Err(error) => eprintln!("benchctl: the bundle manifest could not be built: {error}"),
    }
}
