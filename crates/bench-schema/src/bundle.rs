//! `kafkars.bundle.v1` and the checksum manifest: the second identity, the one
//! that names the evidence rather than the intent.
//!
//! A sealed bundle has two identities and they answer different questions. The
//! experiment id says *what was measured*; the bundle digest says *which bytes
//! were sealed*. Re-running the same experiment produces a new bundle digest
//! and the same experiment id, which is what makes repetitions aggregatable and
//! tampering visible.
//!
//! The circularity — a manifest cannot contain its own digest — is resolved by
//! two tiers. `checksums.txt` covers every file in the bundle except itself and
//! `bundle.json`; the bundle digest is then the sha-256 of `checksums.txt`'s
//! own bytes, and lives in `bundle.json`. Verifying a bundle is therefore two
//! steps that a person can run by hand: `shasum -a 256 -c checksums.txt`, then
//! `shasum -a 256 checksums.txt` compared against `bundle.json`.
//!
//! The line format is fixed at a 64-character lowercase hex digest, two spaces,
//! and a `/`-separated path relative to the bundle root, with lines sorted by
//! path bytes and terminated by a single newline including the last one. That
//! is byte-identical to what the legacy Node sealer wrote and to what `shasum
//! -a 256` prints in text mode, which is the entire reason it is that and not
//! something tidier.

use serde::{Deserialize, Serialize};

use crate::error::{SchemaError, SchemaResult};
use crate::identity::{is_digest_hex, sha256_hex};
use crate::schema_id::BUNDLE_V1;

/// Separator between the digest and the path in a checksum line.
pub const CHECKSUM_SEPARATOR: &str = "  ";

/// One line of `checksums.txt`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ChecksumEntry {
    digest: String,
    path: String,
}

impl ChecksumEntry {
    /// Creates an entry, rejecting anything that would not round-trip.
    ///
    /// The path must be relative, `/`-separated, and free of the characters
    /// that would make the line ambiguous — a leading space would be eaten by
    /// the separator, and a newline would split one entry into two.
    pub fn new(digest: impl Into<String>, path: impl Into<String>) -> SchemaResult<Self> {
        let digest = digest.into();
        let path = path.into();
        if !is_digest_hex(&digest) {
            return Err(SchemaError::invalid_field(
                "checksums.txt digest",
                "must be sixty-four lowercase hexadecimal characters",
            ));
        }
        validate_relative_path(&path)?;
        Ok(Self { digest, path })
    }

    /// Returns the file's sha-256, as lowercase hex.
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// Returns the path relative to the bundle root.
    pub fn path(&self) -> &str {
        &self.path
    }
}

fn validate_relative_path(path: &str) -> SchemaResult<()> {
    let field = "checksums.txt path";
    if path.is_empty() {
        return Err(SchemaError::invalid_field(field, "must not be empty"));
    }
    if path.starts_with('/') {
        return Err(SchemaError::invalid_field(
            field,
            "must be relative to the bundle root",
        ));
    }
    if path.starts_with(' ') || path.starts_with('\t') {
        return Err(SchemaError::invalid_field(
            field,
            "must not start with whitespace, which the separator would absorb",
        ));
    }
    if path.contains('\n') || path.contains('\r') {
        return Err(SchemaError::invalid_field(
            field,
            "must not contain a line break",
        ));
    }
    if path.contains('\\') {
        return Err(SchemaError::invalid_field(
            field,
            "must use '/' as its separator on every platform",
        ));
    }
    if path.split('/').any(|component| component == "..") {
        return Err(SchemaError::invalid_field(
            field,
            "must not climb out of the bundle root",
        ));
    }
    Ok(())
}

/// Renders entries as the bytes of `checksums.txt`.
///
/// Entries are sorted by path bytes — not by locale, not by digest — so that
/// two machines sealing the same bundle produce the same file.
pub fn render_checksums(entries: &[ChecksumEntry]) -> String {
    let mut sorted: Vec<&ChecksumEntry> = entries.iter().collect();
    sorted.sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
    let mut rendered = String::new();
    for entry in sorted {
        rendered.push_str(&entry.digest);
        rendered.push_str(CHECKSUM_SEPARATOR);
        rendered.push_str(&entry.path);
        rendered.push('\n');
    }
    rendered
}

/// Parses the bytes of `checksums.txt` back into entries.
///
/// Every line must be well formed; a manifest that cannot be read exactly is
/// not a manifest. Duplicate paths are rejected because they make the file
/// ambiguous about which digest applies.
pub fn parse_checksums(text: &str) -> SchemaResult<Vec<ChecksumEntry>> {
    let mut entries: Vec<ChecksumEntry> = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if line.is_empty() {
            return Err(SchemaError::parse(format!(
                "checksums.txt line {}: blank lines are not part of the format",
                index + 1
            )));
        }
        let (digest, path) = line.split_once(CHECKSUM_SEPARATOR).ok_or_else(|| {
            SchemaError::parse(format!(
                "checksums.txt line {}: expected a digest and a path separated by two spaces",
                index + 1
            ))
        })?;
        let entry = ChecksumEntry::new(digest, path).map_err(|error| {
            SchemaError::parse(format!("checksums.txt line {}: {error}", index + 1))
        })?;
        if entries.iter().any(|other| other.path == entry.path) {
            return Err(SchemaError::parse(format!(
                "checksums.txt line {}: {} is listed twice",
                index + 1,
                entry.path
            )));
        }
        entries.push(entry);
    }
    Ok(entries)
}

/// `kafkars.bundle.v1`: the manifest that names a sealed bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BundleManifest {
    /// Schema id, always [`BundleManifest::SCHEMA`].
    pub schema: String,
    /// Sha-256 of the bytes of `checksums.txt`, as lowercase hex.
    pub bundle_digest: String,
    /// Files listed in `checksums.txt`.
    pub file_count: u64,
    /// Total bytes of those files.
    pub total_bytes: u64,
}

impl BundleManifest {
    /// Schema id this document declares.
    pub const SCHEMA: &'static str = BUNDLE_V1;

    /// Builds a manifest from the sealed `checksums.txt` bytes.
    ///
    /// The file count is derived from the manifest itself rather than passed
    /// in, so it cannot disagree with the file it describes. The total size is
    /// supplied by the caller, which is the only party that saw the files.
    pub fn from_checksums_bytes(checksums: &[u8], total_bytes: u64) -> SchemaResult<Self> {
        let text = core::str::from_utf8(checksums)
            .map_err(|error| SchemaError::parse(format!("checksums.txt is not UTF-8: {error}")))?;
        let entries = parse_checksums(text)?;
        let file_count = u64::try_from(entries.len()).map_err(|_| {
            SchemaError::parse("checksums.txt lists more files than a count can hold")
        })?;
        Ok(Self {
            schema: Self::SCHEMA.to_owned(),
            bundle_digest: sha256_hex(checksums),
            file_count,
            total_bytes,
        })
    }

    /// Reports whether the document declares the expected schema id.
    pub fn has_expected_schema(&self) -> bool {
        self.schema == Self::SCHEMA
    }
}
