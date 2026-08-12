//! What a facade may contain, and the constructs it may not.

use std::path::PathBuf;

use crate::facade::facade_findings;
use crate::files::{Category, SourceFile};

/// One facade file, without touching a disk.
fn facade(text: &str) -> [SourceFile; 1] {
    [SourceFile {
        path: PathBuf::from("crates/demo/src/lib.rs"),
        relative: "crates/demo/src/lib.rs".to_owned(),
        package: "crates/demo".to_owned(),
        category: Category::Facade,
        lines: text.lines().count(),
        text: text.to_owned(),
    }]
}

#[test]
fn declarations_re_exports_attributes_and_prose_are_all_permitted() {
    let text = r#"//! A contract.
//! Continued.
#![forbid(unsafe_code)]
#![expect(
    clippy::allow_attributes,
    reason = "a multi-line attribute is still an attribute"
)]

/* block
   comment */
/// An item doc, oddly placed.
mod engine;
pub(crate) mod shared;
pub mod public;

use engine::Inner;
pub use engine::{
    Outer,
    // a comment inside the braces
    run,
};
pub use shared::{a::{b, c}, d};

#[cfg(test)]
mod engine_test;
"#;

    assert!(facade_findings(&facade(text)).is_empty());
}

#[test]
fn a_function_definition_is_reported_with_its_line_and_text() {
    let text = "//! A contract.\n\nmod engine;\n\npub fn run() {}\n";

    assert_eq!(
        facade_findings(&facade(text)),
        vec!["crates/demo/src/lib.rs:5: facade carries code, not declarations: pub fn run() {}"]
    );
}

#[test]
fn types_impls_traits_and_constants_are_all_reported() {
    let text = "//! A contract.\nstruct Held;\nimpl Held {}\ntrait Shape {}\nconst N: u8 = 1;\n";

    assert_eq!(facade_findings(&facade(text)).len(), 4);
}

#[test]
fn an_inline_module_body_is_code_wearing_a_declaration_name() {
    let text = "//! A contract.\nmod engine { pub fn run() {} }\n";

    assert_eq!(
        facade_findings(&facade(text)),
        vec![
            "crates/demo/src/lib.rs:2: facade carries code, not declarations: mod engine { pub fn run() {} }"
        ]
    );
}

#[test]
fn a_multi_line_re_export_is_one_statement_not_many_lines_of_code() {
    let text = "//! A contract.\npub use engine::{\n    run,\n    struct_like_name,\n    fn_like_name,\n};\n";

    assert!(
        facade_findings(&facade(text)).is_empty(),
        "re-exported names that merely start with a keyword are not definitions"
    );
}

#[test]
fn code_hidden_inside_a_comment_or_a_string_is_not_code() {
    let text = "//! A contract.\n// pub fn commented() {}\n/* fn blocked() {} */\n#![doc = \"fn quoted() {}\"]\nmod engine;\n";

    assert!(facade_findings(&facade(text)).is_empty());
}

#[test]
fn code_after_a_closing_block_comment_is_still_found() {
    let text = "//! A contract.\n/* opened\n   still open */ pub fn run() {}\n";

    assert_eq!(
        facade_findings(&facade(text)),
        vec!["crates/demo/src/lib.rs:3: facade carries code, not declarations: pub fn run() {}"]
    );
}

#[test]
fn only_facades_are_judged_for_purity() {
    let mut files = facade("//! A contract.\npub fn run() {}\n");
    files[0].category = Category::Implementation;

    assert!(facade_findings(&files).is_empty());
}
