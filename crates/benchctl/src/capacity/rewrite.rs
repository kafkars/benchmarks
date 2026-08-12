//! The rate override applied to the *scenario text*.
//!
//! The patched text is what each probe seals as `experiment.source.toml`. That
//! keeps the sealed input and the sealed resolved experiment in agreement: a
//! reader who re-runs the sealed scenario gets the rate the probe actually
//! offered. Each probe therefore has its own experiment id, which is correct — a
//! different offered rate is a different experiment.

use bench_schema::SourceExperiment;

use crate::error::{CtlError, CtlResult};
use crate::pipeline::LoadedInputs;

/// The scenario text with `offered_records_per_second` set to `rate`, parsed.
///
/// # Errors
///
/// Returns an invalid-experiment error when the patched text no longer parses,
/// which means the scenario was not the fixed-rate shape a search needs.
pub(super) fn probe_inputs(inputs: &LoadedInputs, rate: u64) -> CtlResult<LoadedInputs> {
    let source_toml = with_offered_rate(&inputs.source_toml, rate);
    let source = SourceExperiment::from_toml_str(&source_toml)?;
    // The patch is line-oriented (see [`with_offered_rate`]), so it can be
    // defeated by a scenario written in a shape the rewriter does not
    // understand. Re-reading the value it was supposed to set turns that into a
    // refusal here rather than a whole ladder of probes silently offered at the
    // scenario's original rate, sealed under experiment ids that agree with the
    // wrong number.
    if source.offered_records_per_second != Some(rate) {
        return Err(CtlError::invalid(format!(
            "the scenario's offered rate could not be rewritten to {rate}: the patched text \
             parses back as {:?}. The rewriter replaces a whole `offered_records_per_second = \
             <n>` line before the first table header, so a scenario that spreads that key over \
             several lines, or declares it inside a table, cannot be searched over",
            source.offered_records_per_second
        )));
    }
    Ok(LoadedInputs {
        source_toml,
        source,
        subjects: inputs.subjects.clone(),
        cluster: inputs.cluster.clone(),
    })
}

/// Rewrites the top-level `offered_records_per_second` key of a scenario.
///
/// Only the region before the first `[table]` header is touched, because that is
/// where a scenario's top-level keys live; a key of the same name inside a table
/// would belong to that table and is none of this function's business.
///
/// # The line-oriented limitation
///
/// This works on lines, not on a parsed document, because the patched text is
/// what each probe *seals* — round-tripping the operator's scenario through a
/// TOML writer would mean the sealed input is no longer the file they wrote.
/// The price is that "before the first table header" is decided by looking for
/// a line whose first non-space character is `[`, and a line inside a
/// *multi-line value* can look exactly like that. A scenario carrying a
/// multi-line string with a bracketed line in it ends the head region early;
/// the original assignment then lands in the tail, is kept, and the new one is
/// inserted above it — inside the string, where it means nothing.
///
/// The result still parses, and still carries the original rate, which is the
/// worst possible outcome: a whole ladder of probes offering one rate while
/// every sealed document agrees they were varying it. The probe builder above
/// therefore re-parses the patched text and refuses when the rate is not the one
/// asked for, which turns that shape into a message before any topic is created.
#[must_use]
pub fn with_offered_rate(source_toml: &str, rate: u64) -> String {
    const KEY: &str = "offered_records_per_second";
    let mut head = Vec::new();
    let mut tail = Vec::new();
    let mut in_tables = false;
    for line in source_toml.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with('[') {
            in_tables = true;
        }
        if in_tables {
            tail.push(line);
        } else if !starts_with_key(trimmed, KEY) {
            head.push(line);
        }
    }
    head.push("# rewritten by benchctl capacity for this probe");
    let assignment = format!("{KEY} = {rate}");
    head.push(&assignment);
    let mut lines = head;
    lines.extend(tail);
    let mut text = lines.join("\n");
    text.push('\n');
    text
}

/// Reports whether a line assigns `key` at the top level.
fn starts_with_key(line: &str, key: &str) -> bool {
    line.strip_prefix(key)
        .is_some_and(|rest| rest.trim_start().starts_with('='))
}
