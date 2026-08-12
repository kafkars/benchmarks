//! The gate: this repository, measured against its own `guardrails.toml`.
//!
//! Everything else in this crate is proved against temp-directory fixtures.
//! This file is the one place the policy meets the real tree, and it is
//! deliberately the whole of the lane `scripts/check-guardrails` runs: a
//! guardrail that is only ever tested against fixtures proves the detector
//! works and says nothing about the repository.
//!
//! Advisories print on every run, pass or fail. They are the early warning —
//! files heading for a cap rather than over one — and they are useless if they
//! only appear once something is already broken.

use bench_guardrails::{inspect, repository_root};

/// Traversal must find a workspace-sized tree; a walk that silently found
/// nothing would otherwise report a clean bill of health.
const MINIMUM_FILES: usize = 100;

#[test]
fn the_repository_matches_its_own_guardrails() {
    let root = repository_root();
    let verdict = inspect(&root).unwrap_or_else(|error| panic!("guardrails: {error}"));
    print!("{}", verdict.render());

    assert!(
        verdict.inspected >= MINIMUM_FILES,
        "traversal inspected only {} Rust files under {}; expected at least {MINIMUM_FILES}",
        verdict.inspected,
        root.display()
    );
    assert!(
        verdict.failures.is_empty(),
        "{} guardrail violation(s):\n{}",
        verdict.failures.len(),
        verdict.failures.join("\n")
    );
}
