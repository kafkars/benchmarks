//! Which subject a probe's verdict is taken from, and what makes that verdict
//! a satisfied one.
//!
//! # What "satisfied" means
//!
//! Two gates, both required. The probe's sealed `classification.json` has to say
//! the attempt is believable evidence, *and* the target subject's measurement has
//! to meet the declared objectives. The first gate is not implied by the second:
//! an adapter can exit non-zero, or a read-back verifier can find records
//! missing, while the result document that same attempt wrote reports latencies
//! comfortably inside every ceiling. Bisecting on the objectives alone would
//! climb a ladder built out of runs the bundle itself refuses to vouch for.
//!
//! # Which subject's capacity
//!
//! Every subject's result is evaluated and every subject's reasons are recorded,
//! but one subject decides. When the subject list names exactly one subject, that
//! is the target. Otherwise the target is the **first subject in each probe's
//! execution order** — the same rule the comparison document uses to pick a
//! baseline, so "capacity" and "baseline" cannot mean two different subjects in
//! one evidence tree.

use bench_schema::{CapacityProbe, SloSpec};

use crate::attempt::AttemptPaths;
use crate::pipeline::{self, SealedAttempt};
use crate::results;

use super::ladder::SearchState;

impl SearchState<'_> {
    /// The subjects a probe ran, preferring what the bundle says it ran.
    ///
    /// A bundle that sealed before its execution order was written still has a
    /// subject list — the one the operator asked for — and judging against that
    /// produces "no readable measurement" rather than a silently empty verdict.
    pub(super) fn subject_names(&self, sealed: &SealedAttempt) -> Vec<String> {
        let sealed_order = sealed_execution_order(&sealed.paths);
        if sealed_order.is_empty() {
            self.inputs
                .subjects
                .subjects
                .iter()
                .map(|entry| entry.name.clone())
                .collect()
        } else {
            sealed_order
        }
    }

    /// The subject whose verdict decides this search.
    pub(super) fn target(&self, sealed: &SealedAttempt) -> String {
        if self.inputs.subjects.subjects.len() == 1 {
            return self.inputs.subjects.subjects[0].name.clone();
        }
        self.subject_names(sealed)
            .first()
            .cloned()
            .unwrap_or_default()
    }
}

/// One probe's verdict, plus the bundle it came from.
#[derive(Debug, Clone)]
pub(super) struct ProbeOutcome {
    /// The document entry this probe contributes.
    pub(super) record: CapacityProbe,
    /// Whether the target subject's result could be read at all.
    pub(super) readable: bool,
}

/// Why a probe's own bundle says its evidence may not be believed.
///
/// A probe is a full attempt, and that attempt has already answered this
/// question: sealing wrote `classification.json`. Reading the answer back rather
/// than re-deriving it is what keeps the ladder and the bundles from disagreeing
/// — and the two questions really are different. `evaluate_slo` asks whether the
/// numbers in a result document meet an objective; the classification asks
/// whether those numbers may be believed at all. A probe whose verifier found
/// missing records, or whose adapter exited non-zero, can still carry a result
/// document with a beautiful p99, and treating that as a demonstrated capacity
/// would be reporting a rate nobody measured.
fn validity_reasons(sealed: &SealedAttempt) -> Vec<String> {
    let Some(document) = pipeline::read_classification(&sealed.paths) else {
        return vec!["run validity: the probe sealed no readable classification".to_owned()];
    };
    if document.run_valid {
        return Vec::new();
    }
    if document.reasons.is_empty() {
        return vec!["run validity: classification.json declares the run invalid".to_owned()];
    }
    document
        .reasons
        .iter()
        .map(|reason| format!("run validity: {reason}"))
        .collect()
}

/// Judges one sealed probe.
///
/// Both gates have to hold. The reasons name which one failed, in the order they
/// are asked: whether the attempt is believable at all, then whether the target
/// subject met the objectives, then what every other subject had to say.
pub(super) fn evaluate(
    sealed: &SealedAttempt,
    subjects: &[String],
    target: &str,
    rate: u64,
    slo: &SloSpec,
) -> ProbeOutcome {
    let invalid = validity_reasons(sealed);
    let believable = invalid.is_empty();
    let mut reasons = Vec::new();
    let mut satisfied = false;
    let mut readable = false;
    for subject in subjects {
        let evidence = results::read_subject(&sealed.paths, subject);
        let Some(measurement) = evidence.result.as_ref() else {
            let reason = format!("{subject}: no readable measurement to judge");
            if subject == target {
                reasons.insert(0, reason);
            } else {
                reasons.push(reason);
            }
            continue;
        };
        let verdict = bench_report::evaluate_slo(measurement, slo);
        if subject == target {
            readable = true;
            satisfied = verdict.satisfied;
            // The target's reasons lead, so the document reads as the story of
            // the subject whose capacity this is.
            let mut theirs = verdict.reasons.clone();
            theirs.append(&mut reasons);
            reasons = theirs;
        } else {
            reasons.extend(
                verdict
                    .reasons
                    .iter()
                    .map(|reason| format!("{subject}: {reason}")),
            );
        }
    }
    let mut all_reasons = invalid;
    all_reasons.append(&mut reasons);
    ProbeOutcome {
        record: CapacityProbe {
            offered_records_per_second: rate,
            attempt_id: attempt_id_of(&sealed.paths),
            bundle_digest: bundle_digest(&sealed.paths).unwrap_or_default(),
            satisfied: satisfied && readable && believable,
            reasons: all_reasons,
        },
        readable,
    }
}

/// The subject names a sealed bundle says it ran, in execution order.
fn sealed_execution_order(paths: &AttemptPaths) -> Vec<String> {
    let Ok(bytes) = std::fs::read(paths.execution_order_json()) else {
        return Vec::new();
    };
    bench_schema::parse_json_slice::<bench_schema::ExecutionOrder>(&bytes)
        .map(|document| document.order)
        .unwrap_or_default()
}

/// The attempt id, taken from the directory the bundle lives in.
fn attempt_id_of(paths: &AttemptPaths) -> String {
    paths.root().file_name().map_or_else(
        || "unknown".to_owned(),
        |name| name.to_string_lossy().into_owned(),
    )
}

/// The sealed bundle's digest, when the manifest is readable.
fn bundle_digest(paths: &AttemptPaths) -> Option<String> {
    let bytes = std::fs::read(paths.bundle_json()).ok()?;
    bench_schema::parse_json_slice::<bench_schema::BundleManifest>(&bytes)
        .ok()
        .map(|manifest| manifest.bundle_digest)
}
