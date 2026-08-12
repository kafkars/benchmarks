//! Probing every subject, and the two resolution passes.
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

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use bench_schema::{BudgetSpec, ResolvedExperiment, SubjectsLock, pretty_bytes, sha256_hex};

use crate::attempt::AttemptId;
use crate::error::{CtlError, CtlResult};
use crate::probe;
use crate::resolve::{self, ResolveInputs, RuntimeInputs, SubjectProbe};

use super::inputs::{CommonArguments, LoadedInputs};

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
            argument_binary_sha256s: argument_binary_digests(&entry.command),
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

/// Hashes every argument that names a readable regular file.
///
/// The program is only the thing being measured when the subject is invoked
/// directly. A command like `["shim", "--binary", "<a C producer>"]` measures
/// the C producer, and [`binary_digest`] above identifies the shim — a digest
/// that moves when the wrapper is rebuilt and holds still when the client under
/// test is swapped.
///
/// The rule is deliberately mechanical: every element after the first that
/// `stat`s as a regular file gets hashed, in `command` order, and nothing tries
/// to infer which flag meant "the real binary". An adapter's argument grammar is
/// the adapter's business, and a control plane that guessed at it would be
/// wrong quietly. A path that names a directory, a device, or nothing at all is
/// skipped, because there is no content to hash and an entry saying so would be
/// noise in every ordinary lock.
///
/// Each digest is keyed by the argument as written, so a reader never has to
/// re-derive which argument a digest covered.
fn argument_binary_digests(command: &[String]) -> BTreeMap<String, String> {
    command
        .iter()
        .skip(1)
        .filter(|argument| std::fs::metadata(argument).is_ok_and(|metadata| metadata.is_file()))
        .filter_map(|argument| {
            let bytes = std::fs::read(argument).ok()?;
            Some((argument.clone(), sha256_hex(&bytes)))
        })
        .collect()
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
