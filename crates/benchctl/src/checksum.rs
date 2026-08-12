//! Bundle checksumming: a deterministic recursive walk producing the legacy-
//! compatible `checksums.txt` line set — relative paths, byte-order sorted,
//! streaming SHA-256 — excluding only the checksum file and bundle manifest.
//!
//! The property this module exists to guarantee is that
//! `cd <bundle> && shasum -a 256 -c checksums.txt` passes. That is not a nicety:
//! it means anyone holding a bundle can verify it with a tool they already have,
//! without this repository, without Rust, and without trusting either. The line
//! format therefore belongs to `shasum`, not to us; [`bench_schema::render_checksums`]
//! owns it and this module only supplies digests and paths.
//!
//! Two files are excluded, and only at the bundle root. `checksums.txt` cannot
//! list itself, and `bundle.json` is written *after* the manifest because it
//! contains the manifest's digest. A nested file that happens to share one of
//! those names — an adapter that writes its own `bundle.json` into its output
//! directory — is evidence like any other and is listed.
//!
//! Digests stream in fixed-size chunks rather than reading whole files. A
//! benchmark bundle can contain a latency CSV with ten million rows, and a seal
//! that needs the whole thing resident is a seal that fails exactly when the run
//! was most interesting.

use std::io::Read;
use std::path::{Path, PathBuf};

use bench_schema::{ChecksumEntry, render_checksums};
use sha2::{Digest, Sha256};

use crate::error::{CtlError, CtlResult};

/// Files at the bundle root that are never listed in `checksums.txt`.
pub const EXCLUDED_ROOT_FILES: [&str; 2] = ["checksums.txt", "bundle.json"];

/// Bytes read per digest chunk.
const CHUNK_BYTES: usize = 65_536;

const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

/// A bundle's checksum manifest: the exact bytes of `checksums.txt` and the
/// totals the bundle manifest reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BundleChecksums {
    /// The rendered `checksums.txt` content, newline terminated.
    pub text: String,
    /// How many files are listed.
    pub file_count: u64,
    /// How many bytes those files hold in total.
    pub total_bytes: u64,
}

/// Walks `root` and returns the checksum manifest for everything sealed in it.
///
/// # Errors
///
/// Returns an internal error when a directory cannot be read or a file cannot be
/// digested — the caller turns that into a seal failure, because a bundle whose
/// contents cannot be enumerated cannot be sealed honestly.
pub fn checksum_bundle(root: &Path) -> CtlResult<BundleChecksums> {
    let mut files = Vec::new();
    collect_files(root, root, &mut files)?;
    let mut entries = Vec::with_capacity(files.len());
    let mut total_bytes = 0u64;
    for path in files {
        let relative = relative_path(root, &path)?;
        let (digest, bytes) = digest_file(&path)?;
        total_bytes = total_bytes.saturating_add(bytes);
        entries.push(
            ChecksumEntry::new(digest, &relative)
                .map_err(|error| CtlError::seal(format!("checksum {relative}: {error}")))?,
        );
    }
    let file_count = u64::try_from(entries.len())
        .map_err(|_| CtlError::seal("the bundle holds more files than a count can hold"))?;
    Ok(BundleChecksums {
        text: render_checksums(&entries),
        file_count,
        total_bytes,
    })
}

/// Collects every regular file under `directory`, skipping the excluded root
/// files.
fn collect_files(root: &Path, directory: &Path, files: &mut Vec<PathBuf>) -> CtlResult<()> {
    let entries = std::fs::read_dir(directory)
        .map_err(|error| CtlError::seal(format!("read {}: {error}", directory.display())))?;
    for entry in entries {
        let entry = entry
            .map_err(|error| CtlError::seal(format!("read {}: {error}", directory.display())))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| CtlError::seal(format!("stat {}: {error}", path.display())))?;
        if file_type.is_dir() {
            collect_files(root, &path, files)?;
        } else if file_type.is_file() && !is_excluded(root, &path) {
            files.push(path);
        }
    }
    Ok(())
}

/// Reports whether `path` is one of the two files the manifest never lists.
fn is_excluded(root: &Path, path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    path.parent() == Some(root) && EXCLUDED_ROOT_FILES.contains(&name)
}

/// Returns the `/`-separated path of `path` relative to `root`.
fn relative_path(root: &Path, path: &Path) -> CtlResult<String> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| CtlError::seal(format!("{} is outside the bundle", path.display())))?;
    let mut rendered = String::new();
    for component in relative.components() {
        let text = component
            .as_os_str()
            .to_str()
            .ok_or_else(|| CtlError::seal(format!("{} is not valid UTF-8", path.display())))?;
        if !rendered.is_empty() {
            rendered.push('/');
        }
        rendered.push_str(text);
    }
    Ok(rendered)
}

/// Streams a file through SHA-256, returning its digest and its size.
fn digest_file(path: &Path) -> CtlResult<(String, u64)> {
    let mut file = std::fs::File::open(path)
        .map_err(|error| CtlError::seal(format!("open {}: {error}", path.display())))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; CHUNK_BYTES];
    let mut total = 0u64;
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| CtlError::seal(format!("read {}: {error}", path.display())))?;
        if read == 0 {
            break;
        }
        total = total.saturating_add(u64::try_from(read).unwrap_or(0));
        hasher.update(&buffer[..read]);
    }
    Ok((hex(&hasher.finalize()), total))
}

/// Renders bytes as lowercase hexadecimal.
fn hex(bytes: &[u8]) -> String {
    let mut rendered = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        rendered.push(char::from(HEX_DIGITS[usize::from(byte >> 4)]));
        rendered.push(char::from(HEX_DIGITS[usize::from(byte & 0x0f)]));
    }
    rendered
}
