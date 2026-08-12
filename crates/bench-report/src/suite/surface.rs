//! Whether the two sides of a comparison were doing the same work.
//!
//! A ratio between two measurements is only a comparison when both sides
//! measured the same thing. `declared` in `kafkars.producer-benchmark.v2` is
//! the adapter's own statement of what was inside the measured path, and this
//! module is where a reader's obligation to check it — stated in
//! `docs/EVIDENCE.md` under the declared-execution vocabulary — becomes a gate
//! instead of advice.
//!
//! # Two failure axes, two severities
//!
//! **`payload_construction` and `serialization` fail the gate.** These decide
//! what work is being timed at all. A subject that builds its payload inside
//! the measured interval, or that serializes there while the other does not, is
//! not slower at producing — it is measuring more. A ratio across that
//! difference is not a weak comparison, it is not a comparison.
//!
//! **`ownership` and `completion_mode` are noted, never failed.** These are
//! real product-surface differences that a reader must be told about and must
//! *not* have erased: the kafkars adapter hands the client an owned buffer
//! because that is the public API it ships, and the librdkafka adapter asks for
//! a copy because that is the public API *it* ships. Refusing to compare them
//! would refuse to compare the two clients as they actually exist. So the note
//! carries the difference into every rendering, and the numbers stay.
//!
//! An observation with no declaration at all — an attempt whose result could
//! not be read, or a summary written before the field existed — is reported as
//! unknown rather than as matched. "We did not check" is not "it matched".

use std::collections::BTreeSet;

use bench_schema::{DeclaredExecution, GateOutcome, SuiteAttempt};

use super::comparison::Comparison;

/// The name every suite carries this gate under.
pub const GATE_NAME: &str = "matched-execution-surface";

/// What the declared execution surfaces said about every compared pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SurfaceReview {
    /// The gate, ready to be placed among the others.
    pub(super) gate: GateOutcome,
    /// Product-surface differences a reader must be told about.
    pub(super) notes: Vec<String>,
}

/// Reviews every compared pair's declared execution surface.
pub(super) fn review_execution_surface(
    comparisons: &[Comparison],
    valid: &[&SuiteAttempt],
) -> SurfaceReview {
    let mut blocking: BTreeSet<String> = BTreeSet::new();
    let mut notes: BTreeSet<String> = BTreeSet::new();
    let mut unknown: BTreeSet<String> = BTreeSet::new();
    let mut compared = 0_usize;

    for comparison in comparisons {
        for attempt in valid {
            let declared = |name: &str| {
                attempt
                    .subjects
                    .iter()
                    .find(|entry| entry.name == name)
                    .map(|entry| entry.declared.clone())
            };
            let (Some(numerator), Some(denominator)) = (
                declared(&comparison.numerator),
                declared(&comparison.denominator),
            ) else {
                continue;
            };
            let (Some(numerator), Some(denominator)) = (numerator, denominator) else {
                unknown.insert(format!(
                    "{} / {}",
                    comparison.numerator, comparison.denominator
                ));
                continue;
            };
            compared = compared.saturating_add(1);
            blocking.extend(differences(comparison, &numerator, &denominator, true));
            notes.extend(differences(comparison, &numerator, &denominator, false));
        }
    }

    let passed = blocking.is_empty() && unknown.is_empty() && compared > 0;
    SurfaceReview {
        gate: GateOutcome {
            name: GATE_NAME.to_owned(),
            description: "both sides of a comparison must declare the same payload construction \
                          and the same serialization placement, because a ratio across unlike \
                          work is not a comparison"
                .to_owned(),
            passed,
            detail: detail(compared, &blocking, &unknown),
        },
        notes: notes.into_iter().collect(),
    }
}

/// The differences between two declarations, on one of the two axes.
fn differences(
    comparison: &Comparison,
    numerator: &DeclaredExecution,
    denominator: &DeclaredExecution,
    blocking: bool,
) -> Vec<String> {
    let fields: &[(&str, &String, &String)] = if blocking {
        &[
            (
                "payload_construction",
                &numerator.payload_construction,
                &denominator.payload_construction,
            ),
            (
                "serialization",
                &numerator.serialization,
                &denominator.serialization,
            ),
        ]
    } else {
        &[
            ("ownership", &numerator.ownership, &denominator.ownership),
            (
                "completion_mode",
                &numerator.completion_mode,
                &denominator.completion_mode,
            ),
        ]
    };
    fields
        .iter()
        .filter(|(_, left, right)| left != right)
        .map(|(field, left, right)| {
            if blocking {
                format!(
                    "{} declares {field} {left:?} and {} declares {right:?}",
                    comparison.numerator, comparison.denominator
                )
            } else {
                format!(
                    "product-surface difference: {} declares {field} {left:?} while {} declares \
                     {right:?}; the comparison stands and the difference is reported rather than \
                     erased",
                    comparison.numerator, comparison.denominator
                )
            }
        })
        .collect()
}

/// What the gate observed, pass or fail.
fn detail(compared: usize, blocking: &BTreeSet<String>, unknown: &BTreeSet<String>) -> String {
    if compared == 0 && unknown.is_empty() {
        return "no compared pair declared an execution surface, so nothing was checked".to_owned();
    }
    let mut parts = Vec::new();
    if !blocking.is_empty() {
        parts.push(format!("unlike work: {}", to_list(blocking)));
    }
    if !unknown.is_empty() {
        parts.push(format!("no declaration to check for: {}", to_list(unknown)));
    }
    if parts.is_empty() {
        format!("{compared} compared pairs declare the same measured work")
    } else {
        parts.join("; ")
    }
}

/// Joins a set into one readable list.
fn to_list(entries: &BTreeSet<String>) -> String {
    entries.iter().cloned().collect::<Vec<_>>().join(", ")
}
