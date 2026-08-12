//! Unit tests must name a subject, and must be compiled only under `cfg(test)`.
//!
//! Two failure modes, one check. A `*_test.rs` with no matching subject is
//! usually the residue of a rename: the tests keep passing while the thing they
//! describe no longer exists under that name, which is worse than having no
//! tests, because the file reads as coverage. A `*_test.rs` that nothing
//! declares behind `#[cfg(test)]` is worse still — either it is dead and
//! compiles nowhere, or it is live in release builds.
//!
//! The declaration scan is grep-level by design. Resolving Rust's real module
//! graph would need a parser and would still not answer the question any better
//! for a house style where the declaration is always a single line under a
//! single attribute.

use std::collections::BTreeSet;

use crate::files::{SourceFile, gated_modules};

/// Findings for every `*_test.rs` file among `files`.
#[must_use]
pub fn sibling_findings(files: &[SourceFile]) -> Vec<String> {
    let gated = gated_modules(files);
    let present = files
        .iter()
        .map(|file| file.relative.as_str())
        .collect::<BTreeSet<_>>();
    let mut findings = Vec::new();
    for file in files {
        let Some(stem) = file.relative.strip_suffix("_test.rs") else {
            continue;
        };
        let module = format!("{}_test", tail(stem));
        if !present.contains(format!("{stem}.rs").as_str())
            && !present
                .iter()
                .any(|path| path.starts_with(&format!("{stem}/")))
        {
            findings.push(format!(
                "{}: orphan unit test, no sibling {}.rs or {}/ beside it",
                file.relative,
                tail(stem),
                tail(stem)
            ));
        }
        if !gated.contains(&(file.package.clone(), module.clone())) {
            findings.push(format!(
                "{}: nothing in {} declares `#[cfg(test)] mod {module};`",
                file.relative, file.package
            ));
        }
    }
    findings
}

fn tail(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// Module stems declared behind `#[cfg(test)]` in one file's text.
///
/// Recognises the house shape — the attribute on its own line above the
/// declaration, or inline before it — and tolerates further attributes in
/// between, which is the only variation the tree actually contains.
pub(crate) fn cfg_test_modules(text: &str) -> BTreeSet<String> {
    let mut declared = BTreeSet::new();
    let mut armed = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("//") {
            continue;
        }
        let (is_cfg_test, rest) = split_leading_attribute(trimmed);
        let gated_here = armed || is_cfg_test;
        if rest.is_empty() || rest.starts_with('#') {
            armed = gated_here;
            continue;
        }
        armed = false;
        if let Some(stem) = module_declaration(rest) {
            if gated_here {
                declared.insert(stem);
            }
        }
    }
    declared
}

/// Split a leading outer attribute off a line, reporting whether it is
/// `#[cfg(test)]` with any interior spacing.
fn split_leading_attribute(line: &str) -> (bool, &str) {
    if !line.starts_with("#[") {
        return (false, line);
    }
    let Some(end) = line.find(']') else {
        return (false, line);
    };
    let (attribute, rest) = line.split_at(end + 1);
    let compact = attribute
        .chars()
        .filter(|value| !value.is_whitespace())
        .collect::<String>();
    (compact == "#[cfg(test)]", rest.trim_start())
}

/// The module stem of a bare `mod x;` declaration, whatever its visibility.
fn module_declaration(line: &str) -> Option<String> {
    let rest = strip_visibility(line);
    let name = rest.strip_prefix("mod ")?.trim();
    let name = name.strip_suffix(';')?.trim();
    let valid = !name.is_empty()
        && name
            .chars()
            .all(|value| value.is_ascii_alphanumeric() || value == '_');
    valid.then(|| name.to_owned())
}

/// Remove a leading `pub`, `pub(crate)`, `pub(super)`, or `pub(in ...)`.
pub(crate) fn strip_visibility(line: &str) -> &str {
    let Some(rest) = line.strip_prefix("pub") else {
        return line;
    };
    if let Some(open) = rest.strip_prefix('(') {
        return open
            .find(')')
            .map_or(line, |end| open[end + 1..].trim_start());
    }
    if rest.starts_with(char::is_whitespace) {
        return rest.trim_start();
    }
    line
}
