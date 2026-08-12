//! Executable enforcement of this repository's source shape and dependency
//! policy, read from the hand-authored `guardrails.toml` at the repository
//! root.
//!
//! These rules were review conventions before they were code, which is exactly
//! why they are code now: a convention enforced by review decays at the rate
//! reviewers get busy, and the two properties this repository most depends on —
//! that no async runtime is linked into the harness, and that a file which has
//! outgrown its budget did so by decision rather than by drift — are both
//! invisible in a diff. Written down as a test, they fail on the change that
//! breaks them instead of on the change months later that finally notices.
//!
//! # Two severities
//!
//! A file above its category's `soft` cap **fails**; the only way past is a
//! `[budgets].baseline` entry naming the file, its exact length, and a reason a
//! reviewer can weigh. A file above its `target` but under `soft` prints an
//! **advisory** and never fails, because a design goal that breaks a build
//! stops being a goal and becomes a number people route around. A baseline
//! entry whose file now fits under the gate, or whose file is gone, fails as a
//! stale claim: the ratchet only means something if it is released.
//!
//! # The category rule
//!
//! Every `.rs` file under `[paths].rust_roots` is classified by path and name,
//! in this precedence order:
//!
//! 1. **auxiliary** — path contains `/bin/` or `/tests/common/`, or the file is
//!    a `#[cfg(test)]`-declared support module named `fixture.rs`;
//! 2. **facade** — file name is `lib.rs` or `mod.rs`;
//! 3. **test** — file name ends `_test.rs`, or the path lies under `tests/`;
//! 4. **implementation** — `main.rs` and everything else.
//!
//! Auxiliary is tested before facade, and the case that settles is
//! `tests/common/mod.rs`: cargo mandates that exact filename for helpers shared
//! between integration test binaries, so that one file carries real code and is
//! not a declarative facade. Reading it as a facade would contradict AGENTS.md
//! and leave the `/tests/common/` clause with nothing it could ever match.
#![forbid(unsafe_code)]

mod budgets;
mod config;
mod contract;
mod facade;
mod files;
mod lockfile;
mod report;
mod siblings;

pub use budgets::{BudgetReport, budget_findings};
pub use config::{
    Baseline, Budget, Budgets, ForbiddenDependencies, Paths, Policy, PolicyError, load_policy,
    parse_policy,
};
pub use contract::contract_findings;
pub use facade::facade_findings;
pub use files::{Category, SourceFile, collect_sources, repository_root};
pub use lockfile::lockfile_findings;
pub use report::{Verdict, inspect};
pub use siblings::sibling_findings;

#[cfg(test)]
mod fixture;

#[cfg(test)]
mod budgets_test;
#[cfg(test)]
mod config_test;
#[cfg(test)]
mod contract_test;
#[cfg(test)]
mod facade_test;
#[cfg(test)]
mod files_test;
#[cfg(test)]
mod lockfile_test;
#[cfg(test)]
mod report_test;
#[cfg(test)]
mod siblings_test;
