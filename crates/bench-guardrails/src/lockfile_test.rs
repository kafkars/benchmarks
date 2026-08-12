//! The async-runtime ban, read off the resolved graph rather than the manifests.
#![expect(
    clippy::unwrap_used,
    reason = "a fixture policy that will not parse has nothing left to assert"
)]

use crate::config::ForbiddenDependencies;
use crate::fixture::{CLEAN_LOCK, POLICY};
use crate::lockfile::findings_for;

fn banned() -> ForbiddenDependencies {
    crate::parse_policy(POLICY)
        .unwrap()
        .forbidden_transitive_dependencies
}

#[test]
fn a_clean_graph_says_nothing() {
    assert!(findings_for(CLEAN_LOCK, &banned(), "Cargo.lock").is_empty());
}

#[test]
fn a_banned_crate_is_found_however_far_down_it_sits() {
    let source = concat!(
        "version = 4\n",
        "[[package]]\nname = \"demo\"\nversion = \"0.1.0\"\ndependencies = [\"middle\"]\n",
        "[[package]]\nname = \"middle\"\nversion = \"0.1.0\"\ndependencies = [\"tokio 1.0.0\"]\n",
        "[[package]]\nname = \"tokio\"\nversion = \"1.0.0\"\n",
    );

    assert_eq!(
        findings_for(source, &banned(), "Cargo.lock"),
        vec!["Cargo.lock: forbidden dependency `tokio` is present, required by middle"],
        "naming the dependent is what makes the failure actionable"
    );
}

#[test]
fn a_banned_crate_nobody_names_is_still_a_violation() {
    let source = "version = 4\n[[package]]\nname = \"tokio\"\nversion = \"1.0.0\"\n";

    assert_eq!(
        findings_for(source, &banned(), "Cargo.lock"),
        vec!["Cargo.lock: forbidden dependency `tokio` is present"]
    );
}

#[test]
fn a_crate_merely_named_like_a_banned_one_is_left_alone() {
    let source = concat!(
        "version = 4\n",
        "[[package]]\nname = \"tokio-console\"\nversion = \"1.0.0\"\n",
        "[[package]]\nname = \"futures-executor-shim\"\nversion = \"1.0.0\"\n",
    );

    assert!(
        findings_for(source, &banned(), "Cargo.lock").is_empty(),
        "the check matches package names exactly, not by prefix"
    );
}

#[test]
fn an_unreadable_lock_file_is_a_violation_rather_than_a_pass() {
    assert_eq!(
        findings_for("this is not toml{", &banned(), "Cargo.lock"),
        vec!["Cargo.lock: not a parseable Cargo lock file"]
    );
}

#[test]
fn the_live_root_lock_file_is_free_of_every_banned_runtime() {
    let root = crate::repository_root();
    let policy = crate::load_policy(&root).unwrap();
    let findings =
        crate::lockfile_findings(&root, &policy.forbidden_transitive_dependencies).unwrap();

    assert!(findings.is_empty(), "{findings:?}");
}
