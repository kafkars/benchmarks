//! Throwaway repository trees, built under the system temp directory.
//!
//! Every detector in this crate is proved against a tree written at test time
//! rather than against files checked into this repository. The reason is
//! circularity: a checked-in fixture that proves the oversized-file detector
//! would have to be an oversized file, and a checked-in fixture that proves the
//! module-contract detector would have to be a file with no module contract.
//! Both would be found by the live inspection of this very workspace, and the
//! only way out would be an exclusion list — which is a hole in the policy kept
//! open for the benefit of the thing testing the policy.
//!
//! So the trees live in `std::env::temp_dir()`, are named uniquely per test,
//! and delete themselves on drop.
#![expect(
    clippy::expect_used,
    reason = "a fixture that cannot be written has nothing left to assert"
)]

use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::report::{Verdict, inspect};

/// Distinguishes trees built within one process, including within one test.
static SEQUENCE: AtomicU32 = AtomicU32::new(0);

/// The policy every fixture starts from: the live caps, no baseline, and one
/// banned crate standing in for the async runtimes.
pub(crate) const POLICY: &str = r#"
schema = 1
[paths]
rust_roots = ["crates"]
[budgets.facade]
target = 80
soft = 120
[budgets.implementation]
target = 240
soft = 300
[budgets.test]
target = 300
soft = 500
[budgets.auxiliary]
target = 300
soft = 500
[budgets]
baseline = []
[forbidden_transitive_dependencies]
crates = ["tokio"]
"#;

/// A lock file resolving to nothing a policy could object to.
pub(crate) const CLEAN_LOCK: &str = r#"
version = 4
[[package]]
name = "demo"
version = "0.1.0"
dependencies = ["serde"]
[[package]]
name = "serde"
version = "1.0.0"
"#;

/// A repository-shaped directory that removes itself when it goes out of scope.
#[derive(Debug)]
pub(crate) struct Tree {
    root: PathBuf,
}

impl Tree {
    /// An empty tree carrying the default policy and a clean lock file.
    pub(crate) fn new(name: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |value| value.as_nanos());
        let ordinal = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "bench-guardrails-{name}-{}-{ordinal}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create fixture root");
        let tree = Self { root };
        tree.write("guardrails.toml", POLICY);
        tree.write("Cargo.lock", CLEAN_LOCK);
        tree
    }

    /// A tree that satisfies every check, one file per category.
    pub(crate) fn compliant(name: &str) -> Self {
        let tree = Self::new(name);
        tree.write("crates/demo/Cargo.toml", "[package]\nname = \"demo\"\n");
        tree.write("crates/demo/src/lib.rs", FACADE);
        tree.write(
            "crates/demo/src/engine.rs",
            module("The engine.", "pub fn run() {}"),
        );
        tree.write(
            "crates/demo/src/engine_test.rs",
            module("Engine tests.", ""),
        );
        tree.write("crates/demo/src/fixture.rs", module("Shared builders.", ""));
        tree.write(
            "crates/demo/src/bin/tool.rs",
            module("A binary.", "fn main() {}"),
        );
        tree.write(
            "crates/demo/tests/common/mod.rs",
            module("Shared helpers.", "pub fn help() {}"),
        );
        tree.write("crates/demo/tests/end_to_end.rs", module("End to end.", ""));
        tree
    }

    /// Write one file, creating parent directories as needed.
    pub(crate) fn write(&self, relative: &str, contents: impl AsRef<str>) -> &Self {
        let path = self.root.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create fixture directory");
        }
        fs::write(&path, contents.as_ref()).expect("write fixture file");
        self
    }

    /// Delete one file, for the cases that prove absence is detected.
    pub(crate) fn remove(&self, relative: &str) -> &Self {
        fs::remove_file(self.root.join(relative)).expect("remove fixture file");
        self
    }

    /// The tree's root directory.
    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    /// Run every check over the tree.
    pub(crate) fn inspect(&self) -> Verdict {
        inspect(&self.root).expect("fixture tree should be inspectable")
    }
}

impl Drop for Tree {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

/// A facade exercising every construct the purity classifier must accept.
const FACADE: &str = r"//! A compliant facade.
#![forbid(unsafe_code)]

/* a block comment
   spanning lines */
mod engine;

pub use engine::{
    // even with a comment inside the braces
    run,
};

#[cfg(test)]
mod fixture;
#[cfg(test)]
mod engine_test;
";

/// A file with a module contract, a blank line, and an optional body.
pub(crate) fn module(contract: &str, body: &str) -> String {
    format!("//! {contract}\n\n{body}\n")
}

/// A file with a module contract padded out to exactly `lines` lines.
pub(crate) fn sized(contract: &str, lines: usize) -> String {
    let mut text = format!("//! {contract}\n");
    for index in 1..lines {
        let _ = writeln!(text, "// filler {index}");
    }
    text
}
