//! One ordered verdict over every check, for one repository root.
//!
//! The order is fixed and the contents are sorted, so two runs of the same tree
//! print the same bytes. That is what makes the output diffable, and it is what
//! lets a refactor in flight be tracked by the length of this list rather than
//! by re-reading it.
//!
//! Failures and advisories are kept apart all the way to the caller. Only
//! failures decide the exit status; advisories exist to be read.

use std::fmt::Write as _;
use std::path::Path;

use crate::budgets::budget_findings;
use crate::config::load_policy;
use crate::contract::contract_findings;
use crate::facade::facade_findings;
use crate::files::collect_sources;
use crate::lockfile::lockfile_findings;
use crate::siblings::sibling_findings;

/// Everything the guardrails have to say about one repository tree.
#[derive(Debug, Default)]
pub struct Verdict {
    /// Violations. A non-empty list fails the gate.
    pub failures: Vec<String>,
    /// Files above a design target but under its gate. Never fatal.
    pub advisories: Vec<String>,
    /// How many Rust files were inspected, so a silent pass over an empty tree
    /// cannot masquerade as a clean one.
    pub inspected: usize,
}

impl Verdict {
    /// A human-readable rendering, advisories first because they are context.
    #[must_use]
    pub fn render(&self) -> String {
        let mut text = String::new();
        let _ = writeln!(text, "guardrails: inspected {} Rust files", self.inspected);
        for advisory in &self.advisories {
            let _ = writeln!(text, "  advisory: {advisory}");
        }
        for failure in &self.failures {
            let _ = writeln!(text, "  violation: {failure}");
        }
        text
    }
}

/// Run every check against the tree rooted at `root`.
///
/// The `Err` arm is reserved for a policy or tree that cannot be read at all —
/// a missing `guardrails.toml`, an unreadable source root. A tree that reads
/// fine and breaks the rules is an `Ok` carrying failures.
pub fn inspect(root: &Path) -> Result<Verdict, String> {
    let policy = load_policy(root).map_err(|error| error.to_string())?;
    let files = collect_sources(root, &policy).map_err(|error| error.to_string())?;
    let budgets = budget_findings(&files, &policy.budgets);
    let mut failures = budgets.failures;
    failures.extend(facade_findings(&files));
    failures.extend(contract_findings(&files));
    failures.extend(sibling_findings(&files));
    failures.extend(lockfile_findings(
        root,
        &policy.forbidden_transitive_dependencies,
    )?);
    Ok(Verdict {
        failures,
        advisories: budgets.advisories,
        inspected: files.len(),
    })
}
