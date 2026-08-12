//! Line counts against category caps, and the baseline that buys an exception.
//!
//! Two thresholds do different jobs. `target` is where a file stops being
//! comfortable to read; crossing it prints an advisory and never fails, because
//! a design goal that fails a build stops being a goal and starts being a
//! number people route around. `soft` is the gate.
//!
//! Past the gate, the only way through is a `[budgets].baseline` entry naming
//! the file, its **exact** current length, and a reason. Exactness is what
//! makes it a ratchet rather than a budget line: a baselined file that grows by
//! one line fails, and a baselined file that shrinks fails too, which forces
//! the entry — and the justification attached to it — back in front of a
//! reviewer the moment the work that shrank it lands. An entry whose file now
//! fits under the gate, or whose file no longer exists, is a stale claim and
//! fails on its own.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path};

use crate::config::{Baseline, Budget, Budgets};
use crate::files::SourceFile;

/// The outcome of measuring every file against its category's budget.
#[derive(Debug, Default)]
pub struct BudgetReport {
    /// Gate violations and stale or malformed policy entries.
    pub failures: Vec<String>,
    /// Files above their design target but under the gate. Never fatal.
    pub advisories: Vec<String>,
}

/// Measure `files` against `budgets`, in the order `files` arrives.
#[must_use]
pub fn budget_findings(files: &[SourceFile], budgets: &Budgets) -> BudgetReport {
    let mut report = BudgetReport {
        failures: entry_shape_findings(&budgets.baseline),
        advisories: Vec::new(),
    };
    let index = budgets
        .baseline
        .iter()
        .map(|entry| (entry.path.as_str(), entry))
        .collect::<BTreeMap<_, _>>();
    let mut measured = BTreeSet::new();

    for file in files {
        measured.insert(file.relative.as_str());
        let budget = budgets.for_category(file.category);
        match measure(file, budget, index.get(file.relative.as_str()).copied()) {
            Some(Note::Failure(finding)) => report.failures.push(finding),
            Some(Note::Advisory(finding)) => report.advisories.push(finding),
            None => {}
        }
    }

    for entry in &budgets.baseline {
        if !measured.contains(entry.path.as_str()) {
            report.failures.push(format!(
                "{}: baseline entry names a file that no longer exists",
                entry.path
            ));
        }
    }
    report
}

/// What one file has to say about itself, if anything.
enum Note {
    Failure(String),
    Advisory(String),
}

/// Measure one file against its budget and its baseline entry, if it has one.
///
/// A file with an entry is judged only against that entry: it must still be
/// over the gate, and it must be exactly as long as the entry claims. A file
/// without one is judged against the gate, then against the target.
fn measure(file: &SourceFile, budget: Budget, entry: Option<&Baseline>) -> Option<Note> {
    let (relative, role, lines) = (&file.relative, file.category.label(), file.lines);
    let Some(entry) = entry else {
        if lines > budget.soft {
            return Some(Note::Failure(format!(
                "{relative}: {role}, {lines} lines, above the {}-line soft cap",
                budget.soft
            )));
        }
        if lines > budget.target {
            return Some(Note::Advisory(format!(
                "{relative}: {role}, {lines} lines, above the {}-line target",
                budget.target
            )));
        }
        return None;
    };
    if lines <= budget.soft {
        return Some(Note::Failure(format!(
            "{relative}: {role}, {lines} lines, within the {}-line soft cap — stale baseline entry",
            budget.soft
        )));
    }
    if lines == entry.lines {
        return None;
    }
    let direction = if lines > entry.lines {
        "grew beyond"
    } else {
        "shrank below"
    };
    Some(Note::Failure(format!(
        "{relative}: {role}, {lines} lines, {direction} its exact {}-line baseline",
        entry.lines
    )))
}

/// Findings about the baseline table itself, independent of any file on disk.
fn entry_shape_findings(baseline: &[Baseline]) -> Vec<String> {
    let mut findings = Vec::new();
    let mut seen = BTreeSet::new();
    for entry in baseline {
        if !is_canonical_relative(&entry.path) {
            findings.push(format!(
                "{}: baseline path is not repository-relative with forward slashes",
                entry.path
            ));
        }
        if entry.reason.trim().is_empty() {
            findings.push(format!("{}: baseline entry has a blank reason", entry.path));
        }
        if !seen.insert(entry.path.as_str()) {
            findings.push(format!("{}: duplicate baseline entry", entry.path));
        }
    }
    findings
}

/// Whether a policy path is exactly what traversal would produce for it.
fn is_canonical_relative(value: &str) -> bool {
    if value.is_empty() || value.contains('\\') {
        return false;
    }
    Path::new(value)
        .components()
        .map(|component| match component {
            Component::Normal(part) => part.to_str(),
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => None,
        })
        .collect::<Option<Vec<_>>>()
        .map(|parts| parts.join("/"))
        .as_deref()
        == Some(value)
}
