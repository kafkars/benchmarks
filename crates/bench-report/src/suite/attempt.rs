//! One sealed bundle, read into everything this crate needs from it.
//!
//! # What this reads and what it refuses to invent
//!
//! `status.json` (how far the attempt got), `classification.json` (whether the
//! evidence may be believed), `experiment.resolved.json` (what was asked, and
//! which subject plays which role), `bundle.json` (the digest that lets a
//! reader fetch and check it), and one `kafkars.producer-benchmark.v2` document
//! per subject. Percentiles are derived from the sealed histograms here, never
//! read from a field, because no v2 document contains a percentile.
//!
//! An attempt that fails a validity check is still read in full and still
//! carries its reasons. Dropping it here would leave every downstream median
//! taken over a set nobody can reconstruct.

use std::path::{Path, PathBuf};

use bench_schema::{
    BundleManifest, Classification, ExperimentId, ProducerBenchmarkV2, ResolvedExperiment,
    RunStatus, SuiteAttempt, SuiteSubjectObservation, parse_json_slice,
};

use crate::economics::{RequestEconomics, STATISTICS_FILE_NAME, read_request_economics};
use crate::error::{ReportError, ReportResult};
use crate::stats::histogram_percentile;

use super::comparison::SubjectIdentity;

/// What a bundle's digest is recorded as when the bundle carries no
/// `bundle.json`.
///
/// A workspace that was never sealed is still readable, and reporting on it is
/// useful while a suite is being developed. The digest field says plainly that
/// there is nothing to verify rather than carrying a plausible-looking hex
/// string nobody can check.
pub const UNSEALED_DIGEST: &str = "unsealed";

/// Everything one bundle contributes, already parsed.
#[derive(Debug)]
pub(crate) struct LoadedAttempt {
    /// The bundle's root directory.
    pub(crate) root: PathBuf,
    /// How the attempt ended.
    pub(crate) status: RunStatus,
    /// Whether the attempt's evidence may be believed.
    pub(crate) classification: Classification,
    /// What the attempt was an attempt of.
    pub(crate) experiment: ResolvedExperiment,
    /// The sealed bundle digest, or [`UNSEALED_DIGEST`].
    pub(crate) bundle_digest: String,
    /// Each subject's measurement, in declaration order.
    pub(crate) results: Vec<(String, ProducerBenchmarkV2)>,
    /// Why the attempt is not usable, when it is not.
    pub(crate) invalid_reasons: Vec<String>,
}

impl LoadedAttempt {
    /// Reads every document this crate needs out of one bundle.
    pub(crate) fn read(root: &Path) -> ReportResult<Self> {
        let status: RunStatus = read_document(&root.join("status.json"))?;
        let classification: Classification = read_document(&root.join("classification.json"))?;
        let experiment: ResolvedExperiment = read_document(&root.join("experiment.resolved.json"))?;
        let manifest_path = root.join("bundle.json");
        let bundle_digest = if manifest_path.is_file() {
            read_document::<BundleManifest>(&manifest_path)?.bundle_digest
        } else {
            UNSEALED_DIGEST.to_owned()
        };

        let mut results = Vec::new();
        let mut invalid_reasons = Vec::new();
        if !classification.run_valid {
            let detail = if classification.reasons.is_empty() {
                "classification.json declares the run invalid".to_owned()
            } else {
                classification.reasons.join("; ")
            };
            invalid_reasons.push(detail);
        }
        for subject in &experiment.subjects {
            let path = root
                .join("adapters")
                .join(&subject.name)
                .join("result.json");
            if !path.is_file() {
                invalid_reasons.push(format!("subject {} produced no result", subject.name));
                continue;
            }
            let bytes = std::fs::read(&path).map_err(|error| ReportError::io(&path, &error))?;
            let result = ProducerBenchmarkV2::from_slice(&bytes)?;
            if !result.valid {
                let detail = result
                    .invalid_reason
                    .clone()
                    .unwrap_or_else(|| "no reason given".to_owned());
                invalid_reasons.push(format!(
                    "subject {} declared itself invalid: {detail}",
                    subject.name
                ));
            }
            if let Some(verdict) = classification
                .subjects
                .iter()
                .find(|entry| entry.name == subject.name)
                && !verdict.valid
            {
                invalid_reasons.push(format!(
                    "subject {} failed classification: {}",
                    subject.name,
                    verdict.reasons.join("; ")
                ));
            }
            results.push((subject.name.clone(), result));
        }
        Ok(Self {
            root: root.to_owned(),
            status,
            classification,
            experiment,
            bundle_digest,
            results,
            invalid_reasons,
        })
    }

    /// The attempt id, from the status document.
    pub(crate) fn attempt_id(&self) -> &str {
        &self.status.attempt_id
    }

    /// The experiment this attempt was of.
    ///
    /// Taken from `status.json` when the control plane recorded it, and
    /// recomputed from the resolved experiment when it did not, so that an
    /// attempt that died before resolution finished still lands in the right
    /// suite or is rejected from the wrong one.
    pub(super) fn experiment_id(&self) -> ReportResult<ExperimentId> {
        match &self.status.experiment_id {
            Some(id) => Ok(id.clone()),
            None => Ok(bench_schema::experiment_id(&self.experiment)?),
        }
    }

    /// Builds this attempt's entry, deriving every percentile from the sealed
    /// histograms.
    pub(super) fn observe(&self, subjects: &[SubjectIdentity]) -> ReportResult<SuiteAttempt> {
        let mut observations = Vec::new();
        for subject in subjects {
            let Some((_, result)) = self.results.iter().find(|(name, _)| *name == subject.name)
            else {
                continue;
            };
            let lateness = match &result.timing.intended_to_call_start {
                Some(histogram) => histogram_percentile(histogram, 0.99)?,
                None => None,
            };
            observations.push(SuiteSubjectObservation {
                name: subject.name.clone(),
                role: subject.role.clone(),
                declared: Some(result.declared.clone()),
                acknowledged_records_per_second: result.throughput.acknowledged_records_per_second,
                p50_intended_to_terminal_ns: percentile(result, 0.50)?,
                p99_intended_to_terminal_ns: percentile(result, 0.99)?,
                p999_intended_to_terminal_ns: percentile(result, 0.999)?,
                p99_admission_wait_ns: histogram_percentile(
                    &result.timing.call_start_to_accepted,
                    0.99,
                )?
                .unwrap_or(0),
                p99_intended_to_call_start_ns: lateness,
                p99_accepted_to_terminal_ns: histogram_percentile(
                    &result.timing.accepted_to_terminal,
                    0.99,
                )?
                .unwrap_or(0),
                max_rss_bytes: result.resources.map(|resources| resources.max_rss_bytes),
                cpu_core_seconds: result.resources.map(core_seconds),
                cpu_core_seconds_per_million_acknowledged: result
                    .resources
                    .map(core_seconds)
                    .and_then(|seconds| per_million(seconds, result.outcomes.acknowledged)),
            });
        }
        Ok(SuiteAttempt {
            attempt_id: self.attempt_id().to_owned(),
            bundle_digest: self.bundle_digest.clone(),
            execution_status: self.status.execution_status,
            run_valid: self.invalid_reasons.is_empty() && self.classification.run_valid,
            subjects: observations,
        })
    }

    /// Reads one subject's request economics, when its client emitted any.
    pub(crate) fn economics(&self, subject: &str) -> Option<(u64, RequestEconomics)> {
        let (_, result) = self.results.iter().find(|(name, _)| name == subject)?;
        let path = match &result.native_metrics_path {
            Some(relative) => self.root.join(relative),
            None => self
                .root
                .join("adapters")
                .join(subject)
                .join(STATISTICS_FILE_NAME),
        };
        if !path.is_file() {
            return None;
        }
        let topic = self
            .experiment
            .runtime
            .as_ref()
            .and_then(|runtime| runtime.topics.get(subject))
            .map(|topics| topics.measured.clone());
        let economics =
            read_request_economics(&path, result.outcomes.acknowledged, topic.as_deref()).ok()?;
        Some((result.outcomes.acknowledged, economics))
    }
}

/// Reads and parses one JSON document out of a bundle.
fn read_document<T: serde::de::DeserializeOwned>(path: &Path) -> ReportResult<T> {
    let bytes = std::fs::read(path).map_err(|error| ReportError::io(path, &error))?;
    parse_json_slice(&bytes)
        .map_err(|error| ReportError::document(format!("{}: {error}", path.display())))
}

/// The offer-to-terminal percentile, in nanoseconds.
///
/// An attempt where nothing reached a terminal reports zero, which the validity
/// checks have already disqualified: an attempt with no terminals is never
/// valid, so the zero can never reach a median.
fn percentile(result: &ProducerBenchmarkV2, quantile: f64) -> ReportResult<u64> {
    Ok(histogram_percentile(&result.timing.intended_to_terminal, quantile)?.unwrap_or(0))
}

/// CPU core-seconds from the two nanosecond counters.
#[expect(
    clippy::cast_precision_loss,
    reason = "reporting statistic over nanosecond counters, not identity arithmetic"
)]
fn core_seconds(resources: bench_schema::ProcessResources) -> f64 {
    resources
        .user_cpu_ns
        .saturating_add(resources.system_cpu_ns) as f64
        / 1_000_000_000.0
}

/// CPU core-seconds normalized to a million acknowledged records.
///
/// Total CPU is not comparable between subjects that moved different amounts of
/// traffic — the subject that acknowledged more records should have spent more
/// CPU, and reading the raw totals side by side rewards the one that did less
/// work. Absent, never zero, when there is nothing to divide by: a run that
/// acknowledged nothing has no per-record cost, which is a different statement
/// from a cost of nothing.
#[expect(
    clippy::cast_precision_loss,
    reason = "reporting statistic over a record count, not identity arithmetic"
)]
fn per_million(core_seconds: f64, acknowledged: u64) -> Option<f64> {
    if acknowledged == 0 || !core_seconds.is_finite() {
        return None;
    }
    Some(core_seconds * 1_000_000.0 / acknowledged as f64)
}
