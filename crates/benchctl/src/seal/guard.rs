//! The last-resort seal: armed when an attempt starts, disarmed by a completed
//! seal, and otherwise writing the least a bundle can carry.
//!
//! A last-resort seal that failed loudly would be worse than one that fails
//! quietly: the original failure is the news.

use std::time::SystemTime;

use bench_schema::{ExecutionStatus, RunStatus};

use crate::attempt::AttemptPaths;
use crate::utc_rfc3339_millis;

use super::failure::write_missing_terminal_files;
use super::request::attempt_id_of;

/// The last-resort seal: armed when an attempt starts, disarmed by a completed
/// seal, and otherwise writing the least a bundle can carry.
#[derive(Debug)]
pub(super) struct SealOnDrop {
    paths: AttemptPaths,
    armed: bool,
}

impl SealOnDrop {
    /// Arms the guard for a bundle.
    pub(super) fn arm(paths: &AttemptPaths) -> Self {
        Self {
            paths: paths.clone(),
            armed: true,
        }
    }

    /// Disarms the guard: the funnel sealed the bundle itself.
    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SealOnDrop {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        eprintln!(
            "benchctl: sealing {} from the last-resort guard",
            self.paths.root().display()
        );
        if !self.paths.status_json().exists() {
            let status = RunStatus {
                schema: RunStatus::SCHEMA.to_owned(),
                experiment_id: None,
                attempt_id: attempt_id_of(&self.paths),
                execution_status: ExecutionStatus::Crashed,
                failure_reason: Some(format!(
                    "the control plane exited without sealing (guard ran at {})",
                    utc_rfc3339_millis(SystemTime::now())
                )),
                interrupted: false,
                subjects: Vec::new(),
                phases: Vec::new(),
            };
            if let Ok(bytes) = bench_schema::pretty_bytes(&status) {
                if let Err(error) = std::fs::write(self.paths.status_json(), &bytes) {
                    eprintln!("benchctl: the last-resort seal could not write a status: {error}");
                }
            }
        }
        write_missing_terminal_files(&self.paths);
    }
}
