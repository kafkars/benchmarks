//! A file states what it is for before it does anything.

use std::path::PathBuf;

use crate::contract::contract_findings;
use crate::files::{Category, SourceFile};

/// One file of any category, without touching a disk.
fn source(text: &str) -> [SourceFile; 1] {
    [SourceFile {
        path: PathBuf::from("crates/demo/src/engine.rs"),
        relative: "crates/demo/src/engine.rs".to_owned(),
        package: "crates/demo".to_owned(),
        category: Category::Implementation,
        lines: text.lines().count(),
        text: text.to_owned(),
    }]
}

#[test]
fn a_module_contract_may_be_preceded_by_blank_lines_only() {
    assert!(contract_findings(&source("//! A contract.\n\nuse std::fs;\n")).is_empty());
    assert!(contract_findings(&source("\n   \n//! A contract.\n")).is_empty());
}

#[test]
fn a_file_that_opens_with_anything_else_is_reported() {
    let findings = contract_findings(&source("use std::fs;\n\n//! Too late.\n"));

    assert_eq!(
        findings,
        vec!["crates/demo/src/engine.rs: first non-empty line is not a `//!` module contract"]
    );
}

#[test]
fn an_item_doc_or_a_plain_comment_is_not_a_module_contract() {
    assert_eq!(
        contract_findings(&source("/// An item doc.\npub fn run() {}\n")).len(),
        1
    );
    assert_eq!(
        contract_findings(&source("// A remark.\n//! Too late.\n")).len(),
        1
    );
    assert_eq!(
        contract_findings(&source("#![forbid(unsafe_code)]\n//! Too late.\n")).len(),
        1
    );
}

#[test]
fn an_empty_file_has_no_contract() {
    assert_eq!(contract_findings(&source("")).len(), 1);
}
