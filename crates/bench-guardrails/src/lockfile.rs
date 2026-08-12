//! No async runtime may enter the harness's own dependency graph.
//!
//! AGENTS.md states the rule and the workspace manifest repeats it in prose;
//! neither can notice a runtime that arrives four edges down through a
//! dev-dependency somebody added in a hurry. So the check reads the whole
//! resolved graph — the root `Cargo.lock`, not the manifests — because what
//! matters is what gets linked, not what was asked for.
//!
//! `adapters/kafkars/Cargo.lock` is deliberately not read. That adapter is a
//! separate workspace built against the client under test, and the client
//! brings whatever graph it brings; the rule has always been about what the
//! *harness* links, and reading the subject's lock file would turn a statement
//! about this repository into a statement about somebody else's.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use serde::Deserialize;

use crate::config::ForbiddenDependencies;

#[derive(Debug, Deserialize)]
struct Lockfile {
    #[serde(default)]
    package: Vec<LockedPackage>,
}

#[derive(Debug, Deserialize)]
struct LockedPackage {
    name: String,
    #[serde(default)]
    dependencies: Vec<String>,
}

/// Findings for every banned crate present in the root lock file of
/// `repository_root`.
///
/// The lock file is named by its repository-relative path in every finding, not
/// by wherever the checkout happens to live, so two machines produce the same
/// bytes.
pub fn lockfile_findings(
    repository_root: &Path,
    forbidden: &ForbiddenDependencies,
) -> Result<Vec<String>, String> {
    let path = repository_root.join(LOCKFILE);
    let source =
        fs::read_to_string(&path).map_err(|error| format!("read {}: {error}", path.display()))?;
    Ok(findings_for(&source, forbidden, LOCKFILE))
}

/// The one lock file this policy governs.
const LOCKFILE: &str = "Cargo.lock";

/// Validate lock-file text directly, so tests need no file on disk.
pub(crate) fn findings_for(
    source: &str,
    forbidden: &ForbiddenDependencies,
    label: &str,
) -> Vec<String> {
    let Ok(lockfile) = toml::from_str::<Lockfile>(source) else {
        return vec![format!("{label}: not a parseable Cargo lock file")];
    };
    let present = lockfile
        .package
        .iter()
        .map(|entry| entry.name.as_str())
        .collect::<BTreeSet<_>>();
    let mut findings = Vec::new();
    for banned in &forbidden.crates {
        if !present.contains(banned.as_str()) {
            continue;
        }
        let dependents = dependents_of(&lockfile, banned);
        findings.push(if dependents.is_empty() {
            format!("{label}: forbidden dependency `{banned}` is present")
        } else {
            format!(
                "{label}: forbidden dependency `{banned}` is present, required by {}",
                dependents.join(", ")
            )
        });
    }
    findings
}

/// Packages that name `banned` directly, so a failure points somewhere.
fn dependents_of(lockfile: &Lockfile, banned: &str) -> Vec<String> {
    lockfile
        .package
        .iter()
        .filter(|entry| {
            entry
                .dependencies
                .iter()
                // A lock entry is `name`, `name version`, or
                // `name version (source)`; only the first word identifies it.
                .any(|edge| edge.split_whitespace().next() == Some(banned))
        })
        .map(|entry| entry.name.clone())
        .collect()
}
