//! The two passes, and the subjects lock the second one produces.
//!
//! # The run-id circularity, and how it is broken
//!
//! The run id is derived from the experiment id, and the experiment id is the
//! digest of the resolved document — which carries the run id. Read naively
//! that cannot terminate. It does terminate because the identity deliberately
//! excludes two things: the whole `runtime` block, where the run id lives, and
//! every subject's `command`. So the resolution runs in two passes:
//!
//! 1. build the document with `runtime: None` and take its experiment id;
//! 2. derive `run_id` from that id and the attempt id, build the runtime
//!    binding, and attach it.
//!
//! Attaching the binding cannot change the id, and a test asserts exactly that:
//! two attempts of one scenario share an experiment id and differ in run id.
//! That is the property the whole evidence model rests on — an experiment id
//! aggregates repetitions, a run id separates the records they wrote.

use bench_schema::{ResolvedExperiment, SubjectLockEntry, SubjectsLock, experiment_id};

use crate::error::{CtlError, CtlResult};

use super::binding::{derive_run_id, runtime_binding};
use super::document::unbound_experiment;
use super::inputs::ResolveInputs;

/// Resolves a scenario into the document that will be run and hashed.
///
/// This is the first of the two passes described in the module contract: it
/// needs the describe documents (for adapter names and versions) but not the
/// validate reports, so its output is what gets handed to `validate` in the
/// first place.
///
/// # Errors
///
/// Returns an invalid-experiment error when the scenario, the cluster profile,
/// the subject list, or the requested order cannot make a coherent experiment.
pub fn resolve_experiment(inputs: &ResolveInputs) -> CtlResult<ResolvedExperiment> {
    let unbound = unbound_experiment(inputs)?;
    let identity = experiment_id(&unbound)?;
    let run_id = derive_run_id(identity.as_str(), &inputs.runtime.attempt_id);
    let binding = runtime_binding(inputs, &unbound, &run_id)?;
    let bound = ResolvedExperiment {
        runtime: Some(binding),
        ..unbound
    };
    bound.validate()?;
    Ok(bound)
}

/// Resolves a scenario and locks what every subject was when it was asked.
///
/// # Errors
///
/// Returns an invalid-experiment error for everything
/// [`resolve_experiment`] refuses, plus a subject that was never asked to
/// validate and a subject whose adapter declined the experiment.
pub fn resolve_pure(inputs: &ResolveInputs) -> CtlResult<(ResolvedExperiment, SubjectsLock)> {
    let resolved = resolve_experiment(inputs)?;
    let mut entries = Vec::with_capacity(inputs.subjects.len());
    let mut declined = Vec::new();
    for probe in &inputs.subjects {
        let Some(report) = probe.validate.clone() else {
            return Err(CtlError::invalid(format!(
                "subject {:?} was never asked to validate this experiment",
                probe.subject.name
            )));
        };
        if !report.supported {
            let reasons = if report.reasons.is_empty() {
                "no reason given".to_owned()
            } else {
                report.reasons.join("; ")
            };
            declined.push(format!("{}: {reasons}", probe.subject.name));
        }
        entries.push(SubjectLockEntry {
            name: probe.subject.name.clone(),
            command: probe.subject.command.clone(),
            binary_sha256: probe.binary_sha256.clone(),
            argument_binary_sha256s: probe.argument_binary_sha256s.clone(),
            describe: probe.describe.clone(),
            validate: report,
        });
    }
    if !declined.is_empty() {
        return Err(CtlError::invalid(format!(
            "the experiment was declined by {}",
            declined.join(" | ")
        )));
    }
    Ok((resolved, SubjectsLock::new(entries)))
}
