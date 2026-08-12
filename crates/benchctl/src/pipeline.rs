//! One attempt, from three TOML files to a sealed evidence bundle.
//!
//! `run`, `suite`, and `capacity` differ only in how many attempts they make and
//! what they do with the bundles afterwards. The attempt itself — read the
//! inputs, probe every subject, resolve twice, claim a workspace, capture the
//! environment, hand over to the sealing supervisor — is the same act every
//! time, and it lives here so that a repetition and a capacity probe cannot
//! drift into being different kinds of run than the one `benchctl run` makes.
//!
//! # Nothing here returns early past the workspace
//!
//! [`attempt`] answers with an [`AttemptEnd`], never with a bare error. Once the
//! workspace exists, every ending — a control-plane error, a caught panic, a
//! subject that timed out — is a sealed bundle plus the exit code that ending
//! maps to. [`AttemptEnd::Unsealable`] is reserved for the failures that happen
//! before there is anywhere to seal into: unreadable inputs, or a results
//! directory that could not be claimed.
//!
//! # Two resolutions, one document
//!
//! `validate` takes a resolved experiment *as a file*, so the document must
//! exist before any adapter can be asked about it. The pipeline therefore
//! resolves twice: once from the describe documents alone, to have something to
//! hand to `validate`, and once with the verdicts in hand to build the subjects
//! lock. The resolution is pure and deterministic, so the second pass reproduces
//! the first byte for byte; the provisional copy is written to a scratch
//! directory outside the results tree, never into the bundle, because the bundle
//! is written by the sealing pass alone.

use std::cell::RefCell;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use bench_schema::{
    BudgetSpec, Classification, ClusterProfile, ExecutionStatus, ExperimentId, ResolvedExperiment,
    RunStatus, SourceExperiment, SubjectsFile, SubjectsLock, experiment_id, pretty_bytes,
    sha256_hex,
};

use crate::attempt::{AttemptId, AttemptPaths};
use crate::error::{CtlError, CtlResult, EXIT_INTERNAL, exit_code_for_status};
use crate::resolve::{ResolveInputs, RuntimeInputs, SubjectProbe};
use crate::seal::AttemptRequest;
use crate::{environment, probe, resolve, seal};

/// The inputs every attempt-making verb shares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommonArguments {
    /// Path to the scenario TOML.
    pub experiment: PathBuf,
    /// Path to the subject list TOML.
    pub subjects: PathBuf,
    /// Path to the cluster profile TOML.
    pub cluster: PathBuf,
    /// Bootstrap servers this attempt binds to.
    pub bootstrap: String,
    /// Seed override; `None` keeps the scenario's payload seed.
    pub seed: Option<u64>,
    /// Execution order override, in the order given.
    pub order: Option<Vec<String>>,
}

/// The three input documents, plus the scenario text that gets sealed verbatim.
#[derive(Debug, Clone)]
pub struct LoadedInputs {
    /// The scenario exactly as authored, sealed byte for byte.
    pub source_toml: String,
    /// The parsed scenario.
    pub source: SourceExperiment,
    /// The parsed subject list.
    pub subjects: SubjectsFile,
    /// The parsed cluster profile.
    pub cluster: ClusterProfile,
}

/// Reads and parses the three input documents.
///
/// # Errors
///
/// Returns an invalid-experiment error when a file cannot be read, does not
/// parse, or declares a subject role the vocabulary does not know.
pub fn load(common: &CommonArguments) -> CtlResult<LoadedInputs> {
    let source_toml = read(&common.experiment)?;
    let subjects_toml = read(&common.subjects)?;
    let cluster_toml = read(&common.cluster)?;
    let subjects = SubjectsFile::from_toml_str(&subjects_toml)?;
    // A misspelled role would quietly demote a subject to unlabeled, which is
    // the failure `deny_unknown_fields` exists to prevent one level down.
    subjects.validate()?;
    Ok(LoadedInputs {
        source: SourceExperiment::from_toml_str(&source_toml)?,
        subjects,
        cluster: ClusterProfile::from_toml_str(&cluster_toml)?,
        source_toml,
    })
}

/// Reads a file as text, naming it in the error.
pub fn read(path: &Path) -> CtlResult<String> {
    std::fs::read_to_string(path)
        .map_err(|error| CtlError::invalid(format!("read {}: {error}", path.display())))
}

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
#[must_use]
pub fn read_classification(paths: &AttemptPaths) -> Option<Classification> {
    let bytes = std::fs::read(paths.classification_json()).ok()?;
    bench_schema::parse_json_slice::<Classification>(&bytes).ok()
}

/// Reads a bundle's sealed run status, when it has a readable one.
#[must_use]
pub fn read_status(paths: &AttemptPaths) -> Option<RunStatus> {
    let bytes = std::fs::read(paths.status_json()).ok()?;
    bench_schema::parse_json_slice::<RunStatus>(&bytes).ok()
}

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
            match seal::seal_failure(
                &sealed_paths,
                Some(&inputs.source_toml),
                ExecutionStatus::Partial,
                &error.to_string(),
            ) {
                Ok(status) => finish(status, exit_code_for_status(status)),
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

/// Probes every subject and resolves the experiment, leaving the resolved
/// document and its lock.
///
/// # Errors
///
/// Returns an invalid-experiment error when a subject cannot be probed or the
/// three documents cannot make a coherent experiment.
pub fn probe_and_resolve(
    inputs: &LoadedInputs,
    common: &CommonArguments,
    budget: BudgetSpec,
    id: &AttemptId,
    scratch: &Path,
) -> CtlResult<(ResolvedExperiment, SubjectsLock)> {
    if inputs.subjects.subjects.is_empty() {
        return Err(CtlError::invalid(
            "the subjects file names nothing to measure",
        ));
    }
    let timeout = Duration::from_secs(budget.probe_timeout_seconds);
    let cap = budget.max_captured_output_bytes;
    let mut probes = Vec::with_capacity(inputs.subjects.subjects.len());
    for entry in &inputs.subjects.subjects {
        let describe = probe::describe(&entry.command, timeout, cap)?;
        probes.push(SubjectProbe {
            subject: entry.clone(),
            describe,
            validate: None,
            binary_sha256: binary_digest(&entry.command),
        });
    }

    let mut resolve_inputs = ResolveInputs {
        source: inputs.source.clone(),
        cluster: inputs.cluster.clone(),
        subjects: probes,
        seed: common.seed,
        budget,
        runtime: RuntimeInputs {
            bootstrap: common.bootstrap.clone(),
            attempt_id: id.as_str().to_owned(),
            order: common.order.clone(),
        },
    };

    let provisional = resolve::resolve_experiment(&resolve_inputs)?;
    let provisional_path = scratch.join("experiment.resolved.json");
    write_document(&provisional_path, &provisional)?;
    for subject in &mut resolve_inputs.subjects {
        let report = probe::validate(&subject.subject.command, &provisional_path, timeout, cap)?;
        subject.validate = Some(report);
    }
    resolve::resolve_pure(&resolve_inputs)
}

/// Hashes a subject's program file when it is a readable file.
///
/// A subject invoked through a wrapper, a shell builtin, or a bare name found
/// on `PATH` has no digest here, and the lock records that honestly rather than
/// inventing a placeholder.
fn binary_digest(command: &[String]) -> Option<String> {
    let program = command.first()?;
    let bytes = std::fs::read(program).ok()?;
    Some(sha256_hex(&bytes))
}

/// Writes a resolved experiment, creating its parent directory.
///
/// # Errors
///
/// Returns an internal error when the document cannot be rendered or written.
pub fn write_document(path: &Path, document: &ResolvedExperiment) -> CtlResult<()> {
    let bytes = pretty_bytes(document)?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| CtlError::internal(format!("create {}: {error}", parent.display())))?;
    }
    std::fs::write(path, bytes)
        .map_err(|error| CtlError::internal(format!("write {}: {error}", path.display())))
}

/// Creates a scratch directory outside the results tree for the provisional
/// resolved document that `validate` is handed.
///
/// # Errors
///
/// Returns an internal error when the directory cannot be created.
pub fn scratch_directory(id: &AttemptId) -> CtlResult<PathBuf> {
    let path = std::env::temp_dir().join(format!("benchctl-{}", id.as_str()));
    std::fs::create_dir_all(&path)
        .map_err(|error| CtlError::internal(format!("create {}: {error}", path.display())))?;
    Ok(path)
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
