//! `lib.rs` and `mod.rs` stay declarative: a reader must be able to learn a
//! crate's shape from its facade without also reading its behaviour.
//!
//! A facade may contain doc comments, attributes, `mod` declarations, `use` and
//! `pub use` re-exports, and `#[cfg(test)] mod` declarations. Anything that
//! executes belongs in a named module, so the map never becomes the territory.
//!
//! This is a line classifier rather than a parser, and the case it is built to
//! get right is the multi-line re-export: a `pub use` whose braces span lines
//! is one statement, and every line of it is accepted without being re-read for
//! keywords, so a re-exported item named `format_line` never reads as a
//! definition. Comments and string literals are stripped for the same reason.

use crate::files::{Category, SourceFile};
use crate::siblings::strip_visibility;

/// How much of an offending line a finding quotes.
const SNIPPET: usize = 56;

/// Findings for every facade among `files`.
#[must_use]
pub fn facade_findings(files: &[SourceFile]) -> Vec<String> {
    files
        .iter()
        .filter(|file| file.category == Category::Facade)
        .flat_map(|file| {
            impurities(&file.text).into_iter().map(move |(line, code)| {
                format!(
                    "{}:{line}: facade carries code, not declarations: {}",
                    file.relative,
                    quote(&code)
                )
            })
        })
        .collect()
}

fn quote(code: &str) -> String {
    if code.chars().count() > SNIPPET {
        format!("{}…", code.chars().take(SNIPPET).collect::<String>())
    } else {
        code.to_owned()
    }
}

/// A statement that has opened but not yet closed.
struct Pending {
    depth: i64,
    needs_semicolon: bool,
}

/// Every line of `text` that is neither blank, a comment, an attribute, nor a
/// `mod` or `use` declaration, paired with its one-based line number.
fn impurities(text: &str) -> Vec<(usize, String)> {
    let mut findings = Vec::new();
    let mut comment = 0_i64;
    let mut pending: Option<Pending> = None;

    for (index, raw) in text.lines().enumerate() {
        let code = strip_comments(raw, &mut comment);
        let code = code.trim();
        if code.is_empty() {
            continue;
        }
        if let Some(open) = pending.take() {
            let depth = open.depth + depth_delta(code);
            let closed = depth <= 0 && (!open.needs_semicolon || code.contains(';'));
            if !closed {
                pending = Some(Pending {
                    depth,
                    needs_semicolon: open.needs_semicolon,
                });
            }
            continue;
        }
        match judge(code) {
            Judgement::Declaration => {}
            Judgement::Continues(open) => pending = Some(open),
            Judgement::Code => findings.push((index + 1, code.to_owned())),
        }
    }
    findings
}

/// The verdict for one statement-opening line.
enum Judgement {
    Declaration,
    Continues(Pending),
    Code,
}

fn judge(code: &str) -> Judgement {
    let Some(body) = shed_attributes(code) else {
        // An attribute whose brackets do not close on this line.
        return Judgement::Continues(Pending {
            depth: depth_delta(code),
            needs_semicolon: false,
        });
    };
    let body = body.trim();
    if body.is_empty() {
        return Judgement::Declaration;
    }
    let statement = strip_visibility(body);
    if let Some(rest) = statement.strip_prefix("mod ") {
        // A `mod x { .. }` body is code wearing a declaration's name.
        return if rest.contains('{') || !rest.trim_end().ends_with(';') {
            Judgement::Code
        } else {
            Judgement::Declaration
        };
    }
    if statement.starts_with("use ") {
        let depth = depth_delta(body);
        return if depth <= 0 && body.trim_end().ends_with(';') {
            Judgement::Declaration
        } else {
            Judgement::Continues(Pending {
                depth,
                needs_semicolon: true,
            })
        };
    }
    Judgement::Code
}

/// Remove every complete leading attribute, or `None` if one runs past the end.
fn shed_attributes(code: &str) -> Option<&str> {
    let mut rest = code;
    while rest.starts_with("#[") || rest.starts_with("#![") {
        let open = rest.find('[')?;
        let close = closing_bracket(rest, open)?;
        rest = rest[close + 1..].trim_start();
    }
    Some(rest)
}

/// Index of the `]` matching the `[` at `open`, skipping string literals.
fn closing_bracket(code: &str, open: usize) -> Option<usize> {
    let mut depth = 0_i64;
    let mut quoted = false;
    let mut escaped = false;
    for (index, value) in code.char_indices().skip(open) {
        if quoted {
            match value {
                _ if escaped => escaped = false,
                '\\' => escaped = true,
                '"' => quoted = false,
                _ => {}
            }
            continue;
        }
        match value {
            '"' => quoted = true,
            '[' => depth += 1,
            ']' => {
                depth -= 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

/// Net bracket depth a line adds, ignoring string literals.
fn depth_delta(code: &str) -> i64 {
    let mut depth = 0_i64;
    let mut quoted = false;
    let mut escaped = false;
    for value in code.chars() {
        if quoted {
            match value {
                _ if escaped => escaped = false,
                '\\' => escaped = true,
                '"' => quoted = false,
                _ => {}
            }
            continue;
        }
        match value {
            '"' => quoted = true,
            '{' | '(' | '[' => depth += 1,
            '}' | ')' | ']' => depth -= 1,
            _ => {}
        }
    }
    depth
}

/// Strip comments from one line, carrying nested block-comment depth across
/// lines. Doc comments vanish here like any other comment: a facade is allowed
/// as much prose as it likes.
fn strip_comments(raw: &str, depth: &mut i64) -> String {
    let mut code = String::with_capacity(raw.len());
    let mut chars = raw.chars().peekable();
    let mut quoted = false;
    let mut escaped = false;
    while let Some(value) = chars.next() {
        if *depth > 0 {
            match (value, chars.peek()) {
                ('*', Some('/')) => {
                    chars.next();
                    *depth -= 1;
                }
                ('/', Some('*')) => {
                    chars.next();
                    *depth += 1;
                }
                _ => {}
            }
            continue;
        }
        if quoted {
            match value {
                _ if escaped => escaped = false,
                '\\' => escaped = true,
                '"' => quoted = false,
                _ => {}
            }
            code.push(value);
            continue;
        }
        match (value, chars.peek()) {
            ('/', Some('/')) => break,
            ('/', Some('*')) => {
                chars.next();
                *depth += 1;
            }
            ('"', _) => {
                quoted = true;
                code.push(value);
            }
            _ => code.push(value),
        }
    }
    code
}
