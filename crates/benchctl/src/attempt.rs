//! Attempt identity and the on-disk layout of one evidence bundle.
//!
//! An attempt id is `<utc-compact-seconds>-<8 hex>`: sortable by wall-clock
//! start, made unique by hashing process id, nanosecond clock, a monotonic
//! counter, and an ASLR-derived address — entropy without an RNG dependency
//! and without `unsafe`. Uniqueness is ultimately enforced by directory
//! creation, not by the id: claiming an attempt directory that already exists
//! is a hard error, never a reuse.
//!
//! [`AttemptPaths`] is the single authority for every file name inside a
//! bundle. The workspace is born under `results/pending/<attempt-id>/` so that
//! failures before the experiment id exists still have a home to seal into,
//! and is renamed to `results/<experiment-short-id>/<attempt-id>/` once
//! resolution succeeds.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bench_schema::sha256_hex;

use crate::error::{CtlError, CtlResult};
use crate::time::utc_compact_seconds;

/// Directory under the results root that holds attempts not yet resolved.
pub const PENDING_DIR: &str = "pending";
/// Length of the compact UTC prefix of an attempt id.
const COMPACT_TIME_LENGTH: usize = 16;
/// Length of the entropy suffix of an attempt id.
const ENTROPY_HEX_LENGTH: usize = 8;

static ATTEMPT_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Identifier of one attempt: `20260812T140305Z-1a2b3c4d`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptId(String);

impl AttemptId {
    /// Generates a fresh id for an attempt starting at `now`.
    #[must_use]
    pub fn generate(now: SystemTime) -> Self {
        let counter = ATTEMPT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = now
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos();
        let marker = 0u8;
        let address = std::ptr::from_ref(&marker).addr();
        let seed = format!("{}|{nanos}|{counter}|{address}", std::process::id());
        let digest = sha256_hex(seed.as_bytes());
        let entropy = &digest[..ENTROPY_HEX_LENGTH];
        Self(format!("{}-{entropy}", utc_compact_seconds(now)))
    }

    /// Validates and wraps an attempt id in its canonical text form.
    ///
    /// # Errors
    ///
    /// Returns an invalid-experiment error when the text does not have the
    /// `<utc-compact-seconds>-<8 lowercase hex>` shape.
    pub fn parse(text: &str) -> CtlResult<Self> {
        let expected_length = COMPACT_TIME_LENGTH + 1 + ENTROPY_HEX_LENGTH;
        let bytes = text.as_bytes();
        let shape_ok = bytes.len() == expected_length
            && bytes.get(COMPACT_TIME_LENGTH) == Some(&b'-')
            && bytes[..COMPACT_TIME_LENGTH]
                .iter()
                .all(|b| b.is_ascii_digit() || *b == b'T' || *b == b'Z')
            && bytes[COMPACT_TIME_LENGTH + 1..]
                .iter()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b));
        if shape_ok {
            Ok(Self(text.to_owned()))
        } else {
            Err(CtlError::invalid(format!("malformed attempt id: {text:?}")))
        }
    }

    /// The id's canonical text form.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for AttemptId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The file layout of one attempt's evidence bundle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptPaths {
    root: PathBuf,
}

impl AttemptPaths {
    /// Wraps an existing attempt root without creating anything.
    #[must_use]
    pub fn at(root: PathBuf) -> Self {
        Self { root }
    }

    /// Creates the pending workspace `results/pending/<attempt-id>/` plus its
    /// `adapters/` and `verification/` subdirectories.
    ///
    /// # Errors
    ///
    /// Returns an attempt-exists error when the directory is already claimed,
    /// and an internal error for any other filesystem failure.
    pub fn create_pending(results_root: &Path, id: &AttemptId) -> CtlResult<Self> {
        let pending = results_root.join(PENDING_DIR);
        std::fs::create_dir_all(&pending)
            .map_err(|e| CtlError::internal(format!("create {}: {e}", pending.display())))?;
        let root = pending.join(id.as_str());
        match std::fs::create_dir(&root) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(CtlError::attempt_exists(format!(
                    "attempt directory already exists: {}",
                    root.display()
                )));
            }
            Err(e) => {
                return Err(CtlError::internal(format!(
                    "create {}: {e}",
                    root.display()
                )));
            }
        }
        let paths = Self { root };
        for dir in [paths.adapters_dir(), paths.verification_dir()] {
            std::fs::create_dir_all(&dir)
                .map_err(|e| CtlError::internal(format!("create {}: {e}", dir.display())))?;
        }
        Ok(paths)
    }

    /// Renames the pending workspace to `results/<experiment-short>/<attempt-id>/`.
    ///
    /// # Errors
    ///
    /// Returns an attempt-exists error when the target is already claimed, and
    /// an internal error when the rename fails; on error the workspace remains
    /// at its current location and stays sealable in place.
    pub fn finalize(self, results_root: &Path, experiment_short: &str) -> CtlResult<Self> {
        let attempt_dir_name = self
            .root
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .ok_or_else(|| CtlError::internal("attempt root has no directory name"))?;
        let target_parent = results_root.join(experiment_short);
        std::fs::create_dir_all(&target_parent)
            .map_err(|e| CtlError::internal(format!("create {}: {e}", target_parent.display())))?;
        let target = target_parent.join(attempt_dir_name);
        if target.exists() {
            return Err(CtlError::attempt_exists(format!(
                "finalized attempt directory already exists: {}",
                target.display()
            )));
        }
        std::fs::rename(&self.root, &target).map_err(|e| {
            CtlError::internal(format!(
                "rename {} -> {}: {e}",
                self.root.display(),
                target.display()
            ))
        })?;
        Ok(Self { root: target })
    }

    /// The bundle's root directory.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// `status.json` — how the attempt ended.
    #[must_use]
    pub fn status_json(&self) -> PathBuf {
        self.root.join("status.json")
    }

    /// `experiment.source.toml` — the human-authored input, sealed verbatim.
    #[must_use]
    pub fn experiment_source_toml(&self) -> PathBuf {
        self.root.join("experiment.source.toml")
    }

    /// `experiment.resolved.json` — the canonical resolved experiment.
    #[must_use]
    pub fn experiment_resolved_json(&self) -> PathBuf {
        self.root.join("experiment.resolved.json")
    }

    /// `subjects.lock.json` — what each subject was at probe time.
    #[must_use]
    pub fn subjects_lock_json(&self) -> PathBuf {
        self.root.join("subjects.lock.json")
    }

    /// `environment.json` — the machine the attempt ran on.
    #[must_use]
    pub fn environment_json(&self) -> PathBuf {
        self.root.join("environment.json")
    }

    /// `execution-order.json` — what actually ran, in order.
    #[must_use]
    pub fn execution_order_json(&self) -> PathBuf {
        self.root.join("execution-order.json")
    }

    /// `classification.json` — validity, separate from execution status.
    #[must_use]
    pub fn classification_json(&self) -> PathBuf {
        self.root.join("classification.json")
    }

    /// `comparison.json` — cross-subject ratios when comparable.
    #[must_use]
    pub fn comparison_json(&self) -> PathBuf {
        self.root.join("comparison.json")
    }

    /// `checksums.txt` — digest of every sealed file except itself and the
    /// bundle manifest.
    #[must_use]
    pub fn checksums_txt(&self) -> PathBuf {
        self.root.join("checksums.txt")
    }

    /// `bundle.json` — the manifest sealing `checksums.txt`.
    #[must_use]
    pub fn bundle_json(&self) -> PathBuf {
        self.root.join("bundle.json")
    }

    /// `adapters/` — one subdirectory per subject.
    #[must_use]
    pub fn adapters_dir(&self) -> PathBuf {
        self.root.join("adapters")
    }

    /// `adapters/<subject>/` — one subject's output directory.
    #[must_use]
    pub fn adapter_dir(&self, subject: &str) -> PathBuf {
        self.adapters_dir().join(subject)
    }

    /// `adapters/<subject>/status.json` — the adapter's own terminal status.
    #[must_use]
    pub fn adapter_status_json(&self, subject: &str) -> PathBuf {
        self.adapter_dir(subject).join("status.json")
    }

    /// `adapters/<subject>/result.json` — the adapter's measurement document.
    #[must_use]
    pub fn adapter_result_json(&self, subject: &str) -> PathBuf {
        self.adapter_dir(subject).join("result.json")
    }

    /// `adapters/<subject>/stdout.log` — captured standard output.
    #[must_use]
    pub fn adapter_stdout_log(&self, subject: &str) -> PathBuf {
        self.adapter_dir(subject).join("stdout.log")
    }

    /// `adapters/<subject>/stderr.log` — captured standard error.
    #[must_use]
    pub fn adapter_stderr_log(&self, subject: &str) -> PathBuf {
        self.adapter_dir(subject).join("stderr.log")
    }

    /// `verification/` — broker-visible verifier output per subject and phase.
    #[must_use]
    pub fn verification_dir(&self) -> PathBuf {
        self.root.join("verification")
    }

    /// `verification/<subject>-<phase>.json`, `phase` being `measured` or
    /// `warmup`.
    #[must_use]
    pub fn verification_json(&self, subject: &str, phase: &str) -> PathBuf {
        self.verification_dir()
            .join(format!("{subject}-{phase}.json"))
    }
}
