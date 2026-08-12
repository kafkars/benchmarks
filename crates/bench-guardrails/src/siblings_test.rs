//! A unit test names a subject, and lives behind `#[cfg(test)]`.

use std::path::PathBuf;

use crate::files::{Category, SourceFile};
use crate::siblings::{cfg_test_modules, sibling_findings};

/// One file, without touching a disk.
fn source(relative: &str, text: &str) -> SourceFile {
    SourceFile {
        path: PathBuf::from(relative),
        relative: relative.to_owned(),
        package: "crates/demo".to_owned(),
        category: Category::Implementation,
        lines: text.lines().count(),
        text: text.to_owned(),
    }
}

/// A facade declaring `engine` and, behind `cfg(test)`, `engine_test`.
const FACADE: &str = "//! A contract.\nmod engine;\n#[cfg(test)]\nmod engine_test;\n";

#[test]
fn a_declared_test_beside_its_subject_is_silent() {
    let files = [
        source("crates/demo/src/lib.rs", FACADE),
        source("crates/demo/src/engine.rs", ""),
        source("crates/demo/src/engine_test.rs", ""),
    ];

    assert!(sibling_findings(&files).is_empty());
}

#[test]
fn a_subject_directory_counts_as_a_sibling() {
    let files = [
        source("crates/demo/src/lib.rs", FACADE),
        source("crates/demo/src/engine/inner.rs", ""),
        source("crates/demo/src/engine_test.rs", ""),
    ];

    assert!(sibling_findings(&files).is_empty());
}

#[test]
fn a_test_whose_subject_vanished_is_reported() {
    let files = [
        source("crates/demo/src/lib.rs", FACADE),
        source("crates/demo/src/engine_test.rs", ""),
    ];

    assert_eq!(
        sibling_findings(&files),
        vec![
            "crates/demo/src/engine_test.rs: orphan unit test, no sibling engine.rs or engine/ beside it"
        ]
    );
}

#[test]
fn a_test_inside_its_own_subjects_directory_is_a_sibling() {
    // a/b/b_test.rs is the unit test of a/b.rs: it lives inside the subject's
    // privacy boundary, so it needs no sibling b.rs in its own directory.
    let files = [
        source(
            "crates/demo/src/lib.rs",
            "//! c\n#[cfg(test)]\nmod phase_test;\n",
        ),
        source("crates/demo/src/phase.rs", ""),
        source("crates/demo/src/phase/phase_test.rs", ""),
    ];
    let findings = sibling_findings(&files);
    assert!(
        findings.iter().all(|finding| !finding.contains("orphan")),
        "accepted shape reported as orphan: {findings:?}"
    );
}

#[test]
fn a_mismatched_test_inside_a_subject_directory_is_still_an_orphan() {
    // Only a/b/b_test.rs is inside its own subject; a/b/inner_test.rs with no
    // inner.rs beside it names a subject that does not exist.
    let files = [
        source(
            "crates/demo/src/lib.rs",
            "//! c\n#[cfg(test)]\nmod inner_test;\n",
        ),
        source("crates/demo/src/phase.rs", ""),
        source("crates/demo/src/phase/inner_test.rs", ""),
    ];

    assert_eq!(
        sibling_findings(&files).len(),
        1,
        "the rule is same-directory, so a nested test names a subject that is not there"
    );
}

#[test]
fn a_test_nobody_declares_behind_cfg_test_is_reported() {
    let files = [
        source(
            "crates/demo/src/lib.rs",
            "//! A contract.\nmod engine;\nmod engine_test;\n",
        ),
        source("crates/demo/src/engine.rs", ""),
        source("crates/demo/src/engine_test.rs", ""),
    ];

    assert_eq!(
        sibling_findings(&files),
        vec![
            "crates/demo/src/engine_test.rs: nothing in crates/demo declares `#[cfg(test)] mod engine_test;`"
        ]
    );
}

#[test]
fn the_declaration_scan_reads_the_shapes_the_house_actually_writes() {
    let text = concat!(
        "//! A contract.\n",
        "#[cfg(test)]\nmod plain_test;\n",
        "#[cfg(test)] mod inline_test;\n",
        "#[cfg( test )]\npub mod spaced_test;\n",
        "#[cfg(test)]\n#[allow(dead_code)]\nmod stacked_test;\n",
        "mod ungated_test;\n",
        "#[cfg(feature = \"x\")]\nmod featured_test;\n",
    );
    let declared = cfg_test_modules(text);

    assert!(declared.contains("plain_test"));
    assert!(declared.contains("inline_test"));
    assert!(declared.contains("spaced_test"));
    assert!(declared.contains("stacked_test"));
    assert!(!declared.contains("ungated_test"));
    assert!(!declared.contains("featured_test"));
}
