//! The fixture matrix: one compliant tree, and one deliberate breakage per
//! detector, each proved end to end through [`crate::inspect`].
//!
//! Every tree here is written to a temp directory at test time. Nothing in this
//! matrix is a file in this repository, because a checked-in fixture proving
//! the oversized-file detector would itself be an oversized file that the live
//! inspection would then have to be taught to ignore.

use crate::fixture::{POLICY, Tree, module, sized};

/// A facade declaring everything the compliant tree contains.
const FACADE: &str =
    "//! A facade.\nmod engine;\n#[cfg(test)]\nmod fixture;\n#[cfg(test)]\nmod engine_test;\n";

#[test]
fn the_compliant_tree_passes_every_check() {
    let tree = Tree::compliant("compliant");
    let verdict = tree.inspect();

    assert!(verdict.failures.is_empty(), "{:?}", verdict.failures);
    assert!(verdict.advisories.is_empty(), "{:?}", verdict.advisories);
    assert_eq!(
        verdict.inspected, 7,
        "traversal must find every fixture file"
    );
}

#[test]
fn an_oversized_implementation_fails_naming_its_cap() {
    let tree = Tree::compliant("oversized");
    tree.write("crates/demo/src/engine.rs", sized("The engine.", 512));

    assert_eq!(
        tree.inspect().failures,
        vec!["crates/demo/src/engine.rs: implementation, 512 lines, above the 300-line soft cap"]
    );
}

#[test]
fn a_file_between_target_and_cap_advises_without_failing() {
    let tree = Tree::compliant("advisory");
    tree.write("crates/demo/src/engine.rs", sized("The engine.", 260));
    let verdict = tree.inspect();

    assert!(verdict.failures.is_empty(), "{:?}", verdict.failures);
    assert_eq!(
        verdict.advisories,
        vec!["crates/demo/src/engine.rs: implementation, 260 lines, above the 240-line target"]
    );
}

#[test]
fn an_impure_facade_fails() {
    let tree = Tree::compliant("impure-facade");
    tree.write(
        "crates/demo/src/lib.rs",
        format!("{FACADE}\npub fn leak() {{}}\n"),
    );

    assert_eq!(
        tree.inspect().failures,
        vec!["crates/demo/src/lib.rs:8: facade carries code, not declarations: pub fn leak() {}"]
    );
}

#[test]
fn a_missing_module_contract_fails() {
    let tree = Tree::compliant("no-contract");
    tree.write("crates/demo/src/engine.rs", "pub fn run() {}\n");

    assert_eq!(
        tree.inspect().failures,
        vec!["crates/demo/src/engine.rs: first non-empty line is not a `//!` module contract"]
    );
}

#[test]
fn an_orphan_unit_test_fails() {
    let tree = Tree::compliant("orphan-test");
    tree.remove("crates/demo/src/engine.rs");

    assert_eq!(
        tree.inspect().failures,
        vec![
            "crates/demo/src/engine_test.rs: orphan unit test, no sibling engine.rs or engine/ beside it"
        ]
    );
}

#[test]
fn an_undeclared_unit_test_fails() {
    let tree = Tree::compliant("undeclared-test");
    tree.write(
        "crates/demo/src/lib.rs",
        "//! A facade.\nmod engine;\n#[cfg(test)]\nmod fixture;\nmod engine_test;\n",
    );

    assert_eq!(
        tree.inspect().failures,
        vec![
            "crates/demo/src/engine_test.rs: nothing in crates/demo declares `#[cfg(test)] mod engine_test;`"
        ]
    );
}

#[test]
fn a_stale_baseline_entry_fails() {
    let tree = Tree::compliant("stale-baseline");
    tree.write(
        "guardrails.toml",
        POLICY.replace(
            "baseline = []",
            r#"baseline = [{ path = "crates/demo/src/engine.rs", lines = 3, reason = "reviewed" }]"#,
        ),
    );

    assert_eq!(
        tree.inspect().failures,
        vec![
            "crates/demo/src/engine.rs: implementation, 3 lines, within the 300-line soft cap — stale baseline entry"
        ]
    );
}

#[test]
fn a_justified_baseline_entry_passes() {
    let tree = Tree::compliant("justified-baseline");
    tree.write("crates/demo/src/engine.rs", sized("The engine.", 512));
    tree.write(
        "guardrails.toml",
        POLICY.replace(
            "baseline = []",
            r#"baseline = [{ path = "crates/demo/src/engine.rs", lines = 512, reason = "reviewed seam" }]"#,
        ),
    );

    assert!(tree.inspect().failures.is_empty());
}

#[test]
fn a_forbidden_dependency_in_the_lock_file_fails() {
    let tree = Tree::compliant("forbidden-dependency");
    tree.write(
        "Cargo.lock",
        concat!(
            "version = 4\n",
            "[[package]]\nname = \"demo\"\nversion = \"0.1.0\"\ndependencies = [\"tokio\"]\n",
            "[[package]]\nname = \"tokio\"\nversion = \"1.0.0\"\n",
        ),
    );

    assert_eq!(
        tree.inspect().failures,
        vec!["Cargo.lock: forbidden dependency `tokio` is present, required by demo"]
    );
}

#[test]
fn shared_integration_helpers_are_auxiliary_rather_than_impure_facades() {
    let tree = Tree::compliant("tests-common");
    tree.write(
        "crates/demo/tests/common/mod.rs",
        module("Shared helpers.", "pub struct Harness;\nimpl Harness {}\n"),
    );

    assert!(
        tree.inspect().failures.is_empty(),
        "cargo mandates this filename, so it carries code and is not judged as a facade"
    );
}

#[test]
fn a_missing_source_root_is_an_error_rather_than_a_silent_pass() {
    let tree = Tree::new("empty");

    assert!(crate::inspect(tree.root()).is_err());
}

#[test]
fn the_rendering_separates_advisories_from_violations() {
    let tree = Tree::compliant("render");
    tree.write("crates/demo/src/engine.rs", sized("The engine.", 512));
    let text = tree.inspect().render();

    assert!(text.contains("guardrails: inspected 7 Rust files"));
    assert!(text.contains("  violation: crates/demo/src/engine.rs: implementation, 512 lines"));
}
