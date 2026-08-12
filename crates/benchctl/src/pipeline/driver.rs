//! The attempt itself: claim a workspace, run everything inside one panic
//! boundary, and make sure every ending past that point is a sealed bundle.

use std::cell::RefCell;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use bench_schema::{BudgetSpec, ExecutionStatus, ExperimentId, experiment_id};

use crate::attempt::{AttemptId, AttemptPaths};
use crate::environment;
use crate::error::{CtlErrorKind, CtlResult, EXIT_INTERNAL, exit_code_for_status};
use crate::seal::{self, AttemptRequest};

use super::inputs::{CommonArguments, LoadedInputs};
use super::outcome::{AttemptEnd, SealedAttempt};
use super::resolution::{probe_and_resolve, scratch_directory};

/// Runs one attempt to a sealed bundle.
///
/// Announces its own failures on stderr as it goes, because a caller running
/// several attempts wants to see the third one fail while the fourth is
/// starting, not at the end.
#[must_use]
pub fn attempt(
    inputs: &LoadedInputs,
    common: &CommonArguments,
    results_root: &Path,
    budget: BudgetSpec,
) -> AttemptEnd {
    let started_at = SystemTime::now();
    let id = AttemptId::generate(started_at);
    let paths = match AttemptPaths::create_pending(results_root, &id) {
        Ok(paths) => paths,
        Err(error) => {
            eprintln!("benchctl: {error}");
            return AttemptEnd::Unsealable(error);
        }
    };
    // The workspace moves out of `pending/` the moment the experiment id
    // exists, and a failure after that point has to seal into the new location.
    // The cells are how the sealing arm below learns where the bundle went and
    // what it was an attempt of.
    let paths = RefCell::new(paths);
    let identity = RefCell::new(None);

    let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
        attempt_pipeline(
            inputs,
            common,
            results_root,
            budget,
            &id,
            &paths,
            &identity,
            started_at,
        )
    }));
    let sealed_paths = paths.borrow().clone();
    let experiment_id = identity.borrow().clone();
    let finish = |status: ExecutionStatus, exit_code: i32| {
        AttemptEnd::Sealed(SealedAttempt {
            paths: sealed_paths.clone(),
            status,
            experiment_id: experiment_id.clone(),
            exit_code,
        })
    };
    match outcome {
        Ok(Ok(status)) => finish(status, exit_code_for_status(status)),
        Ok(Err(error)) => {
            eprintln!("benchctl: {error}");
            // A failure to *write* evidence is a different axis from how the
            // attempt went, and it keeps its own exit code. The bundle may still
            // say `complete` — the run did complete — while the seal over it did
            // not finish, and exiting zero on that would report a bundle nobody
            // can verify as a clean run.
            let write_failed = error.kind() == CtlErrorKind::Seal;
            let seal_exit = error.exit_code();
            match seal::seal_failure(
                &sealed_paths,
                Some(&inputs.source_toml),
                ExecutionStatus::Partial,
                &error.to_string(),
            ) {
                Ok(status) => finish(
                    status,
                    if write_failed {
                        seal_exit
                    } else {
                        exit_code_for_status(status)
                    },
                ),
                Err(seal_error) => {
                    eprintln!("benchctl: {seal_error}");
                    AttemptEnd::Unsealable(seal_error)
                }
            }
        }
        Err(payload) => {
            let reason = panic_reason(payload.as_ref());
            eprintln!("benchctl: panicked: {reason}");
            match seal::seal_failure(
                &sealed_paths,
                Some(&inputs.source_toml),
                ExecutionStatus::Crashed,
                &reason,
            ) {
                Ok(status) => finish(status, EXIT_INTERNAL),
                Err(seal_error) => {
                    eprintln!("benchctl: {seal_error}");
                    AttemptEnd::Unsealable(seal_error)
                }
            }
        }
    }
}

/// Everything that happens once the workspace exists, so that one
/// `catch_unwind` covers all of it.
#[expect(
    clippy::too_many_arguments,
    reason = "every argument is a distinct fact the attempt needs, and bundling them into a \
              struct would only move the list"
)]
fn attempt_pipeline(
    inputs: &LoadedInputs,
    common: &CommonArguments,
    results_root: &Path,
    budget: BudgetSpec,
    id: &AttemptId,
    paths: &RefCell<AttemptPaths>,
    identity: &RefCell<Option<ExperimentId>>,
    started_at: SystemTime,
) -> CtlResult<ExecutionStatus> {
    let scratch = scratch_directory(id)?;
    let resolution = probe_and_resolve(inputs, common, budget, id, &scratch);
    let _ = std::fs::remove_dir_all(&scratch);
    let (resolved, lock) = resolution?;

    let experiment = experiment_id(&resolved)?;
    identity.replace(Some(experiment.clone()));
    let pending = paths.borrow().clone();
    let finalized = pending.finalize(results_root, experiment.short())?;
    paths.replace(finalized.clone());

    let repository_root = repository_root();
    let repositories = environment::default_repositories(&repository_root);
    let mut environment = environment::capture(&repositories, &common.bootstrap, SystemTime::now());
    // The profile is the operator's statement about a cluster this control
    // plane cannot interrogate; taking it here is the only way those two facts
    // reach the sealed environment document.
    if let Some(version) = &inputs.cluster.broker_version {
        environment.broker.version.clone_from(version);
    }
    if let Some(lifecycle) = &inputs.cluster.lifecycle {
        environment.broker.lifecycle.clone_from(lifecycle);
    }

    seal::run_attempt(AttemptRequest {
        resolved,
        lock,
        source_toml: inputs.source_toml.clone(),
        environment,
        tools: inputs.cluster.tools.clone(),
        paths: finalized,
        started_at,
    })
}

/// Where the repository containing this checkout lives, for source capture.
fn repository_root() -> PathBuf {
    std::env::var_os("KAFKA_BENCH_REPO_ROOT").map_or_else(
        || std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        PathBuf::from,
    )
}

/// Extracts a readable reason from a caught panic payload.
fn panic_reason(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "the control plane panicked with an unprintable payload".to_owned()
    }
}
