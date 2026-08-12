//! Traversal of the configured Rust roots, and the category rule.
//!
//! Traversal is sorted and deduplicated so every finding this crate produces
//! comes out in the same order on every machine: a guardrail whose output moves
//! around is one nobody can diff.
//!
//! The category rule is documented in full on the crate root and in
//! `guardrails.toml`. The one subtlety worth repeating next to the code is the
//! precedence: auxiliary is tested before facade, so `tests/common/mod.rs` —
//! the filename cargo mandates for helpers shared between integration test
//! binaries, and therefore the one `mod.rs` in the house that carries real
//! code — is auxiliary rather than a facade that would immediately fail purity.

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::config::Policy;
use crate::siblings::cfg_test_modules;

/// Budget role of one Rust source file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Category {
    /// `lib.rs` and `mod.rs`: declarations and re-exports only.
    Facade,
    /// `main.rs` and every other ordinary module.
    Implementation,
    /// `*_test.rs` siblings and anything under `tests/`.
    Test,
    /// Binaries, shared integration helpers, and `cfg(test)` fixtures.
    Auxiliary,
}

impl Category {
    /// The name used in policy text and in findings.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Facade => "facade",
            Self::Implementation => "implementation",
            Self::Test => "test",
            Self::Auxiliary => "auxiliary",
        }
    }
}

/// One Rust file, read once and classified once.
#[derive(Debug)]
pub struct SourceFile {
    /// Absolute path on disk.
    pub path: PathBuf,
    /// Repository-relative path with forward slashes, as findings name it.
    pub relative: String,
    /// Budget role.
    pub category: Category,
    /// `wc -l`-style line count.
    pub lines: usize,
    /// Repository-relative root of the package owning the file.
    pub package: String,
    /// Full file text, so no check has to read the file a second time.
    pub text: String,
}

/// The repository root, derived from this crate's manifest directory.
#[must_use]
pub fn repository_root() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(Path::parent)
        .map_or_else(|| manifest.to_path_buf(), Path::to_path_buf)
}

/// Read and classify every `.rs` file under the policy's configured roots.
pub fn collect_sources(root: &Path, policy: &Policy) -> io::Result<Vec<SourceFile>> {
    let mut paths = Vec::new();
    for configured in &policy.paths.rust_roots {
        let start = root.join(configured);
        if !start.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("configured Rust root {} is missing", start.display()),
            ));
        }
        walk(&start, &mut paths)?;
    }
    paths.sort();
    paths.dedup();
    classify_all(root, paths)
}

fn walk(directory: &Path, found: &mut Vec<PathBuf>) -> io::Result<()> {
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.is_dir() {
            walk(&path, found)?;
        } else if path.extension().and_then(|value| value.to_str()) == Some("rs") {
            found.push(path);
        }
    }
    Ok(())
}

fn classify_all(root: &Path, paths: Vec<PathBuf>) -> io::Result<Vec<SourceFile>> {
    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
        let text = fs::read_to_string(&path)?;
        files.push(SourceFile {
            relative: relative_path(root, &path),
            package: relative_path(root, &package_root(root, &path)),
            category: Category::Implementation,
            lines: text.lines().count(),
            path,
            text,
        });
    }
    let gated = gated_modules(&files);
    for file in &mut files {
        let package = file.package.clone();
        let category = categorize(&file.relative, &|stem| {
            gated.contains(&(package.clone(), stem))
        });
        file.category = category;
    }
    // Sorted by the string every finding names, so ordering never depends on
    // how `Path` happens to compare components.
    files.sort_by(|left, right| left.relative.cmp(&right.relative));
    Ok(files)
}

/// Every `(package, module stem)` pair declared behind `#[cfg(test)]`.
pub(crate) fn gated_modules(files: &[SourceFile]) -> BTreeSet<(String, String)> {
    files
        .iter()
        .flat_map(|file| {
            cfg_test_modules(&file.text)
                .into_iter()
                .map(|stem| (file.package.clone(), stem))
        })
        .collect()
}

/// Repository-relative display path, always forward-slashed.
pub(crate) fn relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

/// The nearest ancestor directory holding a `Cargo.toml`, bounded by `root`.
fn package_root(root: &Path, path: &Path) -> PathBuf {
    let mut current = path.parent();
    while let Some(directory) = current {
        if directory.join("Cargo.toml").is_file() {
            return directory.to_path_buf();
        }
        if directory == root {
            break;
        }
        current = directory.parent();
    }
    root.to_path_buf()
}

/// The category rule, over a repository-relative path.
///
/// `is_gated` answers whether a module stem is declared behind `#[cfg(test)]`
/// somewhere in the file's own package; only the `fixture.rs` clause consults
/// it, and only after the cheap path tests have failed to match.
pub(crate) fn categorize(relative: &str, is_gated: &dyn Fn(String) -> bool) -> Category {
    let name = relative.rsplit('/').next().unwrap_or(relative);
    let padded = format!("/{relative}");
    if padded.contains("/bin/")
        || padded.contains("/tests/common/")
        || (name == "fixture.rs" && is_gated("fixture".to_owned()))
    {
        Category::Auxiliary
    } else if matches!(name, "lib.rs" | "mod.rs") {
        Category::Facade
    } else if name.ends_with("_test.rs") || padded.contains("/tests/") {
        Category::Test
    } else {
        Category::Implementation
    }
}
