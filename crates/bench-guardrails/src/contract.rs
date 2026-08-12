//! Every Rust file opens with a `//!` module contract.
//!
//! The rule is cheap to check and expensive to skip. A file whose first line
//! states what it is responsible for is a file whose author had to decide that
//! before writing it, and a reviewer who disagrees with the split has something
//! concrete to disagree with. A file that opens with `use` invites the reader
//! to infer its purpose from its imports, which is how modules end up
//! accumulating responsibilities nobody chose.

use crate::files::SourceFile;

/// Findings for every file whose first non-empty line is not a `//!` contract.
#[must_use]
pub fn contract_findings(files: &[SourceFile]) -> Vec<String> {
    files
        .iter()
        .filter(|file| !opens_with_contract(&file.text))
        .map(|file| {
            format!(
                "{}: first non-empty line is not a `//!` module contract",
                file.relative
            )
        })
        .collect()
}

fn opens_with_contract(text: &str) -> bool {
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .is_some_and(|line| line.starts_with("//!"))
}
