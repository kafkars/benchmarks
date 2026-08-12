//! Both directions of the ratchet: what the gate refuses, and what a reviewed
//! baseline buys.
#![expect(
    clippy::unwrap_used,
    reason = "a fixture policy that will not parse has nothing left to assert"
)]

use std::path::PathBuf;

use crate::budgets::budget_findings;
use crate::config::Budgets;
use crate::files::{Category, SourceFile};
use crate::fixture::POLICY;

/// The default budgets with `[budgets].baseline` replaced wholesale.
fn budgets(baseline: &str) -> Budgets {
    let source = POLICY.replace("baseline = []", &format!("baseline = [{baseline}]"));
    crate::parse_policy(&source).unwrap().budgets
}

/// One measured file, without touching a disk.
fn file(relative: &str, category: Category, lines: usize) -> SourceFile {
    SourceFile {
        path: PathBuf::from(relative),
        relative: relative.to_owned(),
        package: "crates/demo".to_owned(),
        category,
        lines,
        text: String::new(),
    }
}

#[test]
fn a_file_under_its_target_says_nothing_at_all() {
    let files = [file(
        "crates/demo/src/engine.rs",
        Category::Implementation,
        200,
    )];
    let report = budget_findings(&files, &budgets(""));

    assert!(report.failures.is_empty());
    assert!(report.advisories.is_empty());
}

#[test]
fn a_file_over_its_target_advises_and_never_fails() {
    let files = [file(
        "crates/demo/src/engine.rs",
        Category::Implementation,
        260,
    )];
    let report = budget_findings(&files, &budgets(""));

    assert!(report.failures.is_empty(), "{:?}", report.failures);
    assert_eq!(
        report.advisories,
        vec!["crates/demo/src/engine.rs: implementation, 260 lines, above the 240-line target"]
    );
}

#[test]
fn a_file_over_its_soft_cap_fails_naming_file_category_count_and_cap() {
    let files = [file(
        "crates/demo/src/engine.rs",
        Category::Implementation,
        512,
    )];
    let report = budget_findings(&files, &budgets(""));

    assert_eq!(
        report.failures,
        vec!["crates/demo/src/engine.rs: implementation, 512 lines, above the 300-line soft cap"]
    );
    assert!(
        report.advisories.is_empty(),
        "a failure is not also an advisory"
    );
}

#[test]
fn each_category_is_measured_against_its_own_cap() {
    let files = [
        file("crates/demo/src/lib.rs", Category::Facade, 130),
        file("crates/demo/src/engine_test.rs", Category::Test, 130),
    ];
    let report = budget_findings(&files, &budgets(""));

    assert_eq!(
        report.failures,
        vec!["crates/demo/src/lib.rs: facade, 130 lines, above the 120-line soft cap"],
        "the same length is a violation for a facade and unremarkable for a test"
    );
}

#[test]
fn an_exact_justified_baseline_buys_the_exception() {
    let files = [file(
        "crates/demo/src/engine.rs",
        Category::Implementation,
        512,
    )];
    let report = budget_findings(
        &files,
        &budgets(
            r#"{ path = "crates/demo/src/engine.rs", lines = 512, reason = "reviewed seam" }"#,
        ),
    );

    assert!(report.failures.is_empty(), "{:?}", report.failures);
}

#[test]
fn a_baselined_file_may_neither_grow_nor_quietly_shrink() {
    let entry = r#"{ path = "crates/demo/src/engine.rs", lines = 512, reason = "reviewed seam" }"#;
    let grown = budget_findings(
        &[file(
            "crates/demo/src/engine.rs",
            Category::Implementation,
            513,
        )],
        &budgets(entry),
    );
    let shrunk = budget_findings(
        &[file(
            "crates/demo/src/engine.rs",
            Category::Implementation,
            480,
        )],
        &budgets(entry),
    );

    assert_eq!(
        grown.failures,
        vec![
            "crates/demo/src/engine.rs: implementation, 513 lines, grew beyond its exact 512-line baseline"
        ]
    );
    assert_eq!(
        shrunk.failures,
        vec![
            "crates/demo/src/engine.rs: implementation, 480 lines, shrank below its exact 512-line baseline"
        ]
    );
}

#[test]
fn a_baseline_whose_file_now_fits_is_a_stale_claim() {
    let files = [file(
        "crates/demo/src/engine.rs",
        Category::Implementation,
        240,
    )];
    let report = budget_findings(
        &files,
        &budgets(
            r#"{ path = "crates/demo/src/engine.rs", lines = 512, reason = "reviewed seam" }"#,
        ),
    );

    assert_eq!(
        report.failures,
        vec![
            "crates/demo/src/engine.rs: implementation, 240 lines, within the 300-line soft cap — stale baseline entry"
        ]
    );
}

#[test]
fn a_baseline_naming_a_vanished_file_is_a_stale_claim() {
    let report = budget_findings(
        &[],
        &budgets(r#"{ path = "crates/demo/src/gone.rs", lines = 512, reason = "reviewed seam" }"#),
    );

    assert_eq!(
        report.failures,
        vec!["crates/demo/src/gone.rs: baseline entry names a file that no longer exists"]
    );
}

#[test]
fn an_unjustified_or_malformed_entry_is_refused_before_any_file_is_read() {
    let report = budget_findings(
        &[],
        &budgets(concat!(
            r#"{ path = "./crates/demo/src/a.rs", lines = 1, reason = "x" },"#,
            r#"{ path = "crates/demo/src/b.rs", lines = 1, reason = "  " },"#,
            r#"{ path = "crates/demo/src/b.rs", lines = 1, reason = "y" }"#
        )),
    );

    assert!(report.failures.iter().any(|finding| finding
        == "./crates/demo/src/a.rs: baseline path is not repository-relative with forward slashes"));
    assert!(
        report
            .failures
            .iter()
            .any(|finding| finding == "crates/demo/src/b.rs: baseline entry has a blank reason")
    );
    assert!(
        report
            .failures
            .iter()
            .any(|finding| finding == "crates/demo/src/b.rs: duplicate baseline entry")
    );
}
