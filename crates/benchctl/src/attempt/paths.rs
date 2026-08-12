//! The file layout of one evidence bundle.
//!
//! [`AttemptPaths`] is the single authority for every file name inside a
//! bundle. The workspace is born under `results/pending/<attempt-id>/` so that
//! failures before the experiment id exists still have a home to seal into,
//! and is renamed to `results/<experiment-short-id>/<attempt-id>/` once
//! resolution succeeds.

use std::path::{Path, PathBuf};

use crate::error::{CtlError, CtlResult};

use super::id::AttemptId;

/// Directory under the results root that holds attempts not yet resolved.
pub const PENDING_DIR: &str = "pending";

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

    /// `seal-failure.txt` — why sealing could not finish.
    ///
    /// Written only when a seal failed after the bundle already held evidence,
    /// which is the one case where the terminal documents may be absent from an
    /// otherwise populated bundle. A reader who finds a bundle without a
    /// `checksums.txt` needs to be told that in the bundle, not only on the
    /// stderr of a process that has since exited.
    #[must_use]
    pub fn seal_failure_txt(&self) -> PathBuf {
        self.root.join("seal-failure.txt")
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
