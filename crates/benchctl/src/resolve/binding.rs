//! The attempt binding: the run id, the topic names derived from it, and the
//! order the subjects run in.
//!
//! None of this participates in the experiment id. That is the point — two
//! attempts of one experiment bind to different run ids and therefore different
//! topics, while remaining repetitions of the same measurement.

use std::collections::BTreeMap;

use bench_schema::{ResolvedExperiment, RuntimeBinding, TopicPair, sha256_hex};

use crate::error::{CtlError, CtlResult};

use super::inputs::ResolveInputs;

/// Prefix every topic this repository creates starts with.
pub const TOPIC_PREFIX: &str = "kfb";

/// Suffix distinguishing a subject's warmup topic from its measured topic.
pub const WARMUP_TOPIC_SUFFIX: &str = "-warmup";

/// Characters of the run id, which is the head of a sha-256 digest.
pub const RUN_ID_LENGTH: usize = 16;

/// Returns the run id for an attempt: the head of the digest of the experiment
/// id followed by the attempt id.
///
/// Both halves matter. The experiment id makes the run id specific to what was
/// measured, and the attempt id makes it specific to *this* measurement, so two
/// repetitions of one experiment never write records the verifier could confuse
/// with each other.
#[must_use]
pub fn derive_run_id(experiment_id_text: &str, attempt_id_text: &str) -> String {
    let mut seed = String::with_capacity(experiment_id_text.len() + attempt_id_text.len());
    seed.push_str(experiment_id_text);
    seed.push_str(attempt_id_text);
    sha256_hex(seed.as_bytes())[..RUN_ID_LENGTH].to_owned()
}

/// Builds the runtime binding: bootstrap, run id, topics, execution order.
pub(super) fn runtime_binding(
    inputs: &ResolveInputs,
    experiment: &ResolvedExperiment,
    run_id: &str,
) -> CtlResult<RuntimeBinding> {
    let bootstrap = inputs.runtime.bootstrap.trim().to_owned();
    if bootstrap.is_empty() {
        return Err(CtlError::invalid(
            "the attempt names no bootstrap servers to bind to",
        ));
    }
    let topic_prefix = format!("{TOPIC_PREFIX}-{run_id}");
    let mut topics = BTreeMap::new();
    for subject in &experiment.subjects {
        let measured = format!("{topic_prefix}-{}", subject.name);
        let warmup = format!("{measured}{WARMUP_TOPIC_SUFFIX}");
        topics.insert(subject.name.clone(), TopicPair { measured, warmup });
    }
    Ok(RuntimeBinding {
        bootstrap,
        run_id: run_id.to_owned(),
        topic_prefix,
        topics,
        execution_order: execution_order(inputs, experiment)?,
    })
}

/// Returns the order subjects run in: the requested one, verbatim, or the
/// subjects-file order.
fn execution_order(
    inputs: &ResolveInputs,
    experiment: &ResolvedExperiment,
) -> CtlResult<Vec<String>> {
    let declared: Vec<String> = experiment
        .subjects
        .iter()
        .map(|subject| subject.name.clone())
        .collect();
    let Some(requested) = &inputs.runtime.order else {
        return Ok(declared);
    };
    let mut remaining = declared.clone();
    for name in requested {
        let Some(position) = remaining.iter().position(|subject| subject == name) else {
            return Err(CtlError::invalid(format!(
                "the requested execution order names {name:?}, which is not a subject, \
                 or names it twice"
            )));
        };
        remaining.remove(position);
    }
    if !remaining.is_empty() {
        return Err(CtlError::invalid(format!(
            "the requested execution order leaves out {}",
            remaining.join(", ")
        )));
    }
    Ok(requested.clone())
}
