//! Aggregation across repetitions: the step where a set of anecdotes becomes a
//! result, or fails to.
//!
//! # What this reads and what it refuses to invent
//!
//! One sealed bundle per attempt. From each it takes `status.json` (how far the
//! attempt got), `classification.json` (whether the evidence may be believed),
//! `experiment.resolved.json` (what was asked, and which subject plays which
//! role), `bundle.json` (the digest that lets a reader fetch and check it), and
//! one `kafkars.producer-benchmark.v2` document per subject. Percentiles are
//! derived from the sealed histograms here, never read from a field, because no
//! v2 document contains a percentile.
//!
//! Every attempt appears in the output. Attempts that failed a validity check
//! are carried with `run_valid: false`, excluded from every median, ratio, and
//! interval, and named in `notes` with the reason. A median over a silently
//! filtered set is a number nobody can reconstruct.
//!
//! # Pairing
//!
//! When the experiment labels subjects with roles, the comparisons are
//! `head/base` and `head/anchor`: the thing being tested over the thing it is
//! being tested against, and over the fixed reference that says whether the
//! machine itself moved. When no roles are declared, every subject is compared
//! against the first one in declaration order, because a ratio needs a named
//! denominator and declaration order is the only order the experiment states.
//!
//! Ratios are always *numerator over denominator of the medians*, and the
//! interval beside them is the paired-block bootstrap over the per-attempt
//! pairs — the same attempts, in the same order, for both subjects.
//!
//! # Gates
//!
//! A gate is not "the point estimate moved". A metric gate passes only when the
//! **entire** confidence interval clears the practical threshold on the
//! favorable side:
//!
//! - goodput, where larger is better, passes when `ci_low > 1 + threshold`;
//! - latency, CPU, and memory, where smaller is better, pass when
//!   `ci_high < 1 - threshold`.
//!
//! An interval that straddles the threshold is not a small effect, it is an
//! unresolved one, and the gate says so rather than rounding it to a verdict.
//! Three gates are not about any pair: at least one attempt has to be valid, a
//! comparison needs [`MINIMUM_PAIRED_REPETITIONS`] valid attempts, and every
//! compared pair's goodput and p99 *ratio* has to vary by no more than
//! [`COEFFICIENT_OF_VARIATION_BUDGET`].
//!
//! # Which dispersion the budget is about
//!
//! [`COEFFICIENT_OF_VARIATION_BUDGET`] is calibrated for the dispersion of the
//! **paired ratio series** — one ratio per attempt, the same numbers the
//! bootstrap resamples — because that is what `statistics.mjs` measured it on:
//! the legacy control plane fed `summarizeRatios` the per-pair
//! `head/base` ratios and asked `assessStatisticalCredibility` about *those*.
//!
//! The distinction is not academic, and it cuts both ways.
//!
//! - A machine that drifts — a thermal ramp, a noisy neighbour — moves both
//!   subjects together. Every subject's raw values are then far outside the
//!   budget while every ratio is rock steady, and pairing is precisely the
//!   technique that makes such a run usable. Gating on raw values throws it away.
//! - Two subjects that move in *opposite* directions produce raw values that
//!   each look quiet and a ratio that swings twice as far as either. Gating on
//!   raw values passes the run whose comparison is least trustworthy.
//!
//! The per-subject raw coefficients are still reported, and are still worth
//! reading — they are how a reader tells "the machine was noisy" from "the
//! subjects disagreed". They are informational rows in the dispersion table,
//! named after the subject; the gated rows are named `numerator/denominator`.
//!
//! `claim_eligible` is false, always, for every summary this milestone can
//! produce.

use std::path::{Path, PathBuf};

use bench_schema::{
    BundleManifest, Classification, ExperimentId, GateOutcome, PairedRatio, ProducerBenchmarkV2,
    ResolvedExperiment, RunStatus, SUBJECT_ROLE_ANCHOR, SUBJECT_ROLE_BASE, SUBJECT_ROLE_HEAD,
    SubjectDispersion, SubjectMedians, SuiteAttempt, SuiteSubjectObservation, SuiteSummary,
    parse_json_slice,
};

use crate::bootstrap::{BootstrapOptions, DEFAULT_BOOTSTRAP_RESAMPLES, PairedObservation};
use crate::economics::{RequestEconomics, STATISTICS_FILE_NAME, read_request_economics};
use crate::error::{ReportError, ReportResult};
use crate::stats::{histogram_percentile, median};
use crate::summary::{
    COEFFICIENT_OF_VARIATION_BUDGET, MINIMUM_PAIRED_REPETITIONS, summarize_positive_values,
};

/// What a bundle's digest is recorded as when the bundle carries no
/// `bundle.json`.
///
/// A workspace that was never sealed is still readable, and reporting on it is
/// useful while a suite is being developed. The digest field says plainly that
/// there is nothing to verify rather than carrying a plausible-looking hex
/// string nobody can check.
pub const UNSEALED_DIGEST: &str = "unsealed";

/// The default practical threshold: a five percent relative difference.
///
/// Chosen to match [`COEFFICIENT_OF_VARIATION_BUDGET`], because a difference
/// smaller than the run-to-run noise of the machine is not a difference this
/// harness can speak about.
pub const DEFAULT_PRACTICAL_THRESHOLD: f64 = 0.05;

/// How a suite is to be summarized.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SuiteOptions {
    /// Seed behind every deterministic choice, including the resampling.
    pub seed: u64,
    /// Bootstrap resamples behind every interval.
    pub resamples: u32,
    /// The relative difference the suite was set up to care about.
    pub practical_threshold: f64,
}

impl Default for SuiteOptions {
    fn default() -> Self {
        Self {
            seed: 0x6B61_666B,
            resamples: DEFAULT_BOOTSTRAP_RESAMPLES,
            practical_threshold: DEFAULT_PRACTICAL_THRESHOLD,
        }
    }
}

/// One subject's native request economics, totalled over the valid attempts.
///
/// Separate from [`SuiteSummary`] because `kafkars.suite-summary.v1` has no
/// field for it: the schema carries latency, goodput, and resources, and
/// request economics are only available for subjects whose client emits native
/// statistics. Carrying them beside the summary keeps the sealed document
/// honest about what it contains, and keeps the asymmetry visible instead of
/// hiding it in a half-empty column.
#[derive(Debug, Clone, PartialEq)]
pub struct SubjectEconomics {
    /// Subject name, matching the resolved experiment.
    pub subject: String,
    /// The subject's role in this comparison, when it declared one.
    pub role: Option<String>,
    /// Valid attempts that contributed a native statistics stream.
    pub attempts_reported: usize,
    /// The totals, and the normalizations derived from them.
    pub totals: RequestEconomics,
}

/// A suite summary together with everything the schema has no field for.
///
/// [`summarize_suite`] returns the sealed document alone; this is the same
/// analysis with the request economics still attached, for the renderers and
/// the analysis packet.
#[derive(Debug, Clone, PartialEq)]
pub struct SuiteReport {
    /// The sealed suite summary.
    pub summary: SuiteSummary,
    /// Per-subject request economics, in subject declaration order. Subjects
    /// whose client emits no native statistics are absent from this list
    /// entirely rather than present with zeroes.
    pub economics: Vec<SubjectEconomics>,
}

/// One metric a suite compares subjects on.
///
/// The order of the variants is the order every report lists them in, and the
/// order [`build_packet`](crate::build_packet) assigns metric ids in. It is
/// fixed on purpose: a reader who learns that `M003` is the p99 should not have
/// to re-learn it next release.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SuiteMetric {
    /// Acknowledged records per second; larger is better.
    Goodput,
    /// Median offer-to-terminal latency.
    P50Latency,
    /// 99th percentile offer-to-terminal latency.
    P99Latency,
    /// 99.9th percentile offer-to-terminal latency.
    P999Latency,
    /// 99th percentile admission wait.
    P99AdmissionWait,
    /// CPU core-seconds consumed.
    CpuCoreSeconds,
    /// Peak resident set size.
    MaxRssBytes,
}

impl SuiteMetric {
    /// Every metric, in the fixed reporting order.
    pub const ALL: [Self; 7] = [
        Self::Goodput,
        Self::P50Latency,
        Self::P99Latency,
        Self::P999Latency,
        Self::P99AdmissionWait,
        Self::CpuCoreSeconds,
        Self::MaxRssBytes,
    ];

    /// The `kafkars.suite-summary.v1` field name this metric is carried in.
    #[must_use]
    pub const fn field(self) -> &'static str {
        match self {
            Self::Goodput => "acknowledged_records_per_second",
            Self::P50Latency => "p50_intended_to_terminal_ns",
            Self::P99Latency => "p99_intended_to_terminal_ns",
            Self::P999Latency => "p999_intended_to_terminal_ns",
            Self::P99AdmissionWait => "p99_admission_wait_ns",
            Self::CpuCoreSeconds => "cpu_core_seconds",
            Self::MaxRssBytes => "max_rss_bytes",
        }
    }

    /// The human name a report prints.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Goodput => "acknowledged goodput",
            Self::P50Latency => "p50 offer-to-terminal",
            Self::P99Latency => "p99 offer-to-terminal",
            Self::P999Latency => "p99.9 offer-to-terminal",
            Self::P99AdmissionWait => "p99 admission wait",
            Self::CpuCoreSeconds => "cpu",
            Self::MaxRssBytes => "peak rss",
        }
    }

    /// The unit the metric's values are in.
    #[must_use]
    pub const fn unit(self) -> &'static str {
        match self {
            Self::Goodput => "records/s",
            Self::P50Latency | Self::P99Latency | Self::P999Latency | Self::P99AdmissionWait => {
                "ns"
            }
            Self::CpuCoreSeconds => "core-seconds",
            Self::MaxRssBytes => "bytes",
        }
    }

    /// Whether a larger value is the better outcome.
    ///
    /// Goodput is the only one. Everything else here is a cost.
    #[must_use]
    pub const fn higher_is_better(self) -> bool {
        matches!(self, Self::Goodput)
    }

    /// Reads this metric out of one attempt's observation.
    #[must_use]
    #[expect(
        clippy::cast_precision_loss,
        reason = "latency and byte counts are reporting statistics, not identity arithmetic"
    )]
    pub fn observed(self, observation: &SuiteSubjectObservation) -> Option<f64> {
        match self {
            Self::Goodput => Some(observation.acknowledged_records_per_second),
            Self::P50Latency => Some(observation.p50_intended_to_terminal_ns as f64),
            Self::P99Latency => Some(observation.p99_intended_to_terminal_ns as f64),
            Self::P999Latency => Some(observation.p999_intended_to_terminal_ns as f64),
            Self::P99AdmissionWait => Some(observation.p99_admission_wait_ns as f64),
            Self::CpuCoreSeconds => observation.cpu_core_seconds,
            Self::MaxRssBytes => observation.max_rss_bytes.map(|value| value as f64),
        }
    }

    /// Reads this metric out of one subject's medians.
    #[must_use]
    #[expect(
        clippy::cast_precision_loss,
        reason = "latency and byte counts are reporting statistics, not identity arithmetic"
    )]
    pub fn median_of(self, medians: &SubjectMedians) -> Option<f64> {
        match self {
            Self::Goodput => Some(medians.acknowledged_records_per_second),
            Self::P50Latency => Some(medians.p50_intended_to_terminal_ns as f64),
            Self::P99Latency => Some(medians.p99_intended_to_terminal_ns as f64),
            Self::P999Latency => Some(medians.p999_intended_to_terminal_ns as f64),
            Self::P99AdmissionWait => Some(medians.p99_admission_wait_ns as f64),
            Self::CpuCoreSeconds => medians.cpu_core_seconds,
            Self::MaxRssBytes => medians.max_rss_bytes.map(|value| value as f64),
        }
    }

    /// The sentence a gate over this metric enforces.
    #[must_use]
    pub fn gate_description(self, threshold: f64) -> String {
        if self.higher_is_better() {
            format!(
                "{} improves only when the whole confidence interval is above {:.3} \
                 (larger is better)",
                self.label(),
                1.0 + threshold
            )
        } else {
            format!(
                "{} improves only when the whole confidence interval is below {:.3} \
                 (smaller is better)",
                self.label(),
                1.0 - threshold
            )
        }
    }
}

/// Summarizes a set of sealed bundles into `kafkars.suite-summary.v1`.
///
/// The bundles are read in the order given, which is the order they were run
/// in, and that order is preserved in `attempts` and in every paired block.
///
/// # Errors
///
/// Returns an error when no bundle is given, when a bundle's documents cannot
/// be read, or when the bundles are not all attempts of the same experiment.
pub fn summarize_suite(
    bundle_roots: &[PathBuf],
    options: &SuiteOptions,
) -> ReportResult<SuiteSummary> {
    Ok(summarize_suite_report(bundle_roots, options)?.summary)
}

/// Summarizes a set of sealed bundles, keeping the request economics attached.
///
/// # Errors
///
/// The same errors as [`summarize_suite`].
pub fn summarize_suite_report(
    bundle_roots: &[PathBuf],
    options: &SuiteOptions,
) -> ReportResult<SuiteReport> {
    if bundle_roots.is_empty() {
        return Err(ReportError::sample(
            "a suite of no attempts summarizes nothing",
        ));
    }
    let loaded: Vec<LoadedAttempt> = bundle_roots
        .iter()
        .map(|root| LoadedAttempt::read(root))
        .collect::<ReportResult<_>>()?;
    let first = loaded
        .first()
        .ok_or_else(|| ReportError::sample("a suite of no attempts summarizes nothing"))?;
    let experiment_id = first.experiment_id()?;
    let scenario_name = first.experiment.name.clone();
    let subjects: Vec<SubjectIdentity> = first
        .experiment
        .subjects
        .iter()
        .map(|subject| SubjectIdentity {
            name: subject.name.clone(),
            role: subject.role.clone(),
        })
        .collect();
    let mut notes = Vec::new();
    for attempt in &loaded {
        if attempt.experiment_id()? != experiment_id {
            return Err(ReportError::document(format!(
                "attempt {} is of a different experiment than {}",
                attempt.attempt_id(),
                first.attempt_id()
            )));
        }
    }

    let attempts: Vec<SuiteAttempt> = loaded
        .iter()
        .map(|attempt| attempt.observe(&subjects))
        .collect::<ReportResult<_>>()?;
    for (attempt, loaded) in attempts.iter().zip(&loaded) {
        if !attempt.run_valid {
            notes.push(format!(
                "attempt {} is excluded from every statistic: {}",
                attempt.attempt_id,
                loaded.invalid_reasons.join("; ")
            ));
        }
        if attempt.subjects.len() != subjects.len() {
            notes.push(format!(
                "attempt {} measured {} of the {} declared subjects",
                attempt.attempt_id,
                attempt.subjects.len(),
                subjects.len()
            ));
        }
    }

    let valid: Vec<&SuiteAttempt> = attempts.iter().filter(|a| a.run_valid).collect();
    if valid.is_empty() {
        notes.push(
            "no attempt was valid, so this summary carries observations and nothing derived \
             from them"
                .to_owned(),
        );
    }
    let medians = subject_medians(&subjects, &valid);
    let comparisons = comparison_pairs(&subjects);
    let pair_dispersion = pair_dispersion(&comparisons, &valid);
    // Informational rows first, gated rows after, so the table reads from
    // "what each subject did" to "what the comparison did".
    let mut dispersion = subject_dispersion(&subjects, &valid);
    dispersion.extend(pair_dispersion.iter().cloned());
    let pairs = paired_ratios(&comparisons, &medians, &valid, options)?;
    let gates = gates(&comparisons, &pairs, &pair_dispersion, &valid, options);
    let economics = subject_economics(&subjects, &loaded, &attempts, &mut notes);

    let repetitions = u32::try_from(bundle_roots.len()).unwrap_or(u32::MAX);
    let summary = SuiteSummary {
        schema: SuiteSummary::SCHEMA.to_owned(),
        experiment_id,
        scenario_name,
        repetitions,
        seed: options.seed,
        resamples: options.resamples,
        practical_threshold: options.practical_threshold,
        attempts,
        medians,
        pairs,
        dispersion,
        gates,
        claim_eligible: false,
        notes,
    };
    summary.validate()?;
    Ok(SuiteReport { summary, economics })
}

/// A subject as the experiment declares it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SubjectIdentity {
    /// Subject name.
    name: String,
    /// Declared role, when there is one.
    role: Option<String>,
}

/// One directed comparison the suite will report.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Comparison {
    /// Subject on top of the ratio.
    numerator: String,
    /// Subject the ratio divides by.
    denominator: String,
}

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
    fn experiment_id(&self) -> ReportResult<ExperimentId> {
        match &self.status.experiment_id {
            Some(id) => Ok(id.clone()),
            None => Ok(bench_schema::experiment_id(&self.experiment)?),
        }
    }

    /// Builds this attempt's entry, deriving every percentile from the sealed
    /// histograms.
    fn observe(&self, subjects: &[SubjectIdentity]) -> ReportResult<SuiteAttempt> {
        let mut observations = Vec::new();
        for subject in subjects {
            let Some((_, result)) = self.results.iter().find(|(name, _)| *name == subject.name)
            else {
                continue;
            };
            observations.push(SuiteSubjectObservation {
                name: subject.name.clone(),
                role: subject.role.clone(),
                acknowledged_records_per_second: result.throughput.acknowledged_records_per_second,
                p50_intended_to_terminal_ns: percentile(result, 0.50)?,
                p99_intended_to_terminal_ns: percentile(result, 0.99)?,
                p999_intended_to_terminal_ns: percentile(result, 0.999)?,
                p99_admission_wait_ns: histogram_percentile(
                    &result.timing.call_start_to_accepted,
                    0.99,
                )?
                .unwrap_or(0),
                max_rss_bytes: result.resources.map(|resources| resources.max_rss_bytes),
                cpu_core_seconds: result.resources.map(core_seconds),
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

/// Per-subject medians over the valid attempts.
fn subject_medians(subjects: &[SubjectIdentity], valid: &[&SuiteAttempt]) -> Vec<SubjectMedians> {
    subjects
        .iter()
        .filter_map(|subject| {
            let observations: Vec<&SuiteSubjectObservation> = valid
                .iter()
                .filter_map(|attempt| {
                    attempt
                        .subjects
                        .iter()
                        .find(|entry| entry.name == subject.name)
                })
                .collect();
            if observations.is_empty() {
                return None;
            }
            Some(SubjectMedians {
                name: subject.name.clone(),
                role: subject.role.clone(),
                acknowledged_records_per_second: median_of(&observations, SuiteMetric::Goodput)
                    .unwrap_or(0.0),
                p50_intended_to_terminal_ns: median_ns(&observations, SuiteMetric::P50Latency),
                p99_intended_to_terminal_ns: median_ns(&observations, SuiteMetric::P99Latency),
                p999_intended_to_terminal_ns: median_ns(&observations, SuiteMetric::P999Latency),
                p99_admission_wait_ns: median_ns(&observations, SuiteMetric::P99AdmissionWait),
                max_rss_bytes: complete_median(&observations, SuiteMetric::MaxRssBytes)
                    .map(round_to_u64),
                cpu_core_seconds: complete_median(&observations, SuiteMetric::CpuCoreSeconds),
            })
        })
        .collect()
}

/// The median of one metric over one subject's observations.
fn median_of(observations: &[&SuiteSubjectObservation], metric: SuiteMetric) -> Option<f64> {
    let values: Vec<f64> = observations
        .iter()
        .filter_map(|observation| metric.observed(observation))
        .collect();
    median(&values)
}

/// The median of one metric, present only when every observation reported it.
///
/// Absence is contagious on purpose: a median of the three attempts that
/// happened to report memory is not the run's memory.
fn complete_median(observations: &[&SuiteSubjectObservation], metric: SuiteMetric) -> Option<f64> {
    let values: Vec<f64> = observations
        .iter()
        .filter_map(|observation| metric.observed(observation))
        .collect();
    if values.len() != observations.len() {
        return None;
    }
    median(&values)
}

/// The median of one nanosecond metric, rounded back to whole nanoseconds.
fn median_ns(observations: &[&SuiteSubjectObservation], metric: SuiteMetric) -> u64 {
    median_of(observations, metric).map_or(0, round_to_u64)
}

/// Rounds a non-negative reporting statistic back to an integer.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "the value is a rounded non-negative median of counts this harness can record"
)]
fn round_to_u64(value: f64) -> u64 {
    if value.is_finite() && value > 0.0 {
        value.round() as u64
    } else {
        0
    }
}

/// How a paired ratio series is named in the dispersion table.
///
/// The slash is the label: a row named `head/base` is a ratio series, a row
/// named `head` is that subject's own values. Subject names cannot contain a
/// slash, so the two can never be confused.
fn ratio_label(comparison: &Comparison) -> String {
    format!("{}/{}", comparison.numerator, comparison.denominator)
}

/// One comparison's per-attempt ratios for one metric, in attempt order.
///
/// The same series [`paired_ratios`] hands the bootstrap: an attempt where
/// either subject is missing contributes nothing, because a ratio needs both
/// halves measured under the same conditions. A denominator of zero produces a
/// non-finite ratio, which is deliberately *kept* rather than filtered — it
/// makes the series unsummarizable, and an unsummarizable series fails the gate
/// instead of quietly shortening it.
fn ratio_series(comparison: &Comparison, metric: SuiteMetric, valid: &[&SuiteAttempt]) -> Vec<f64> {
    valid
        .iter()
        .filter_map(|attempt| {
            let observed = |name: &str| {
                attempt
                    .subjects
                    .iter()
                    .find(|entry| entry.name == name)
                    .and_then(|entry| metric.observed(entry))
            };
            match (
                observed(&comparison.numerator),
                observed(&comparison.denominator),
            ) {
                (Some(numerator), Some(denominator)) => Some(numerator / denominator),
                _ => None,
            }
        })
        .collect()
}

/// Per-pair, per-metric dispersion of the ratio series across valid attempts.
///
/// These are the rows the budget gate reads. See the module contract for why
/// the budget is about ratios rather than about either subject's raw values.
fn pair_dispersion(comparisons: &[Comparison], valid: &[&SuiteAttempt]) -> Vec<SubjectDispersion> {
    let mut dispersion = Vec::new();
    for comparison in comparisons {
        for metric in SuiteMetric::ALL {
            let ratios = ratio_series(comparison, metric, valid);
            if ratios.is_empty() {
                continue;
            }
            dispersion.push(SubjectDispersion {
                name: ratio_label(comparison),
                metric: metric.field().to_owned(),
                coefficient_of_variation: summarize_positive_values(&ratios)
                    .ok()
                    .and_then(|summary| summary.coefficient_of_variation),
            });
        }
    }
    dispersion
}

/// Per-subject, per-metric dispersion across the valid attempts.
///
/// Informational: these rows say whether the machine was steady, which is a
/// different question from whether the comparison was. Nothing gates on them.
fn subject_dispersion(
    subjects: &[SubjectIdentity],
    valid: &[&SuiteAttempt],
) -> Vec<SubjectDispersion> {
    let mut dispersion = Vec::new();
    for subject in subjects {
        for metric in SuiteMetric::ALL {
            let values: Vec<f64> = valid
                .iter()
                .filter_map(|attempt| {
                    attempt
                        .subjects
                        .iter()
                        .find(|entry| entry.name == subject.name)
                })
                .filter_map(|observation| metric.observed(observation))
                .collect();
            if values.is_empty() {
                continue;
            }
            dispersion.push(SubjectDispersion {
                name: subject.name.clone(),
                metric: metric.field().to_owned(),
                coefficient_of_variation: summarize_positive_values(&values)
                    .ok()
                    .and_then(|summary| summary.coefficient_of_variation),
            });
        }
    }
    dispersion
}

/// The comparisons this suite reports, role-aware when roles were declared.
fn comparison_pairs(subjects: &[SubjectIdentity]) -> Vec<Comparison> {
    let with_role = |role: &str| {
        subjects
            .iter()
            .find(|subject| subject.role.as_deref() == Some(role))
            .map(|subject| subject.name.clone())
    };
    if let Some(head) = with_role(SUBJECT_ROLE_HEAD) {
        let mut pairs = Vec::new();
        for role in [SUBJECT_ROLE_BASE, SUBJECT_ROLE_ANCHOR] {
            if let Some(denominator) = with_role(role) {
                pairs.push(Comparison {
                    numerator: head.clone(),
                    denominator,
                });
            }
        }
        if !pairs.is_empty() {
            return pairs;
        }
    }
    let Some(first) = subjects.first() else {
        return Vec::new();
    };
    subjects
        .iter()
        .skip(1)
        .map(|subject| Comparison {
            numerator: subject.name.clone(),
            denominator: first.name.clone(),
        })
        .collect()
}

/// Every paired ratio, with the bootstrap interval over the paired attempts.
fn paired_ratios(
    comparisons: &[Comparison],
    medians: &[SubjectMedians],
    valid: &[&SuiteAttempt],
    options: &SuiteOptions,
) -> ReportResult<Vec<PairedRatio>> {
    let bootstrap = BootstrapOptions {
        seed: options.seed,
        resamples: options.resamples,
    };
    let mut ratios = Vec::new();
    for comparison in comparisons {
        for metric in SuiteMetric::ALL {
            let (Some(numerator), Some(denominator)) = (
                medians
                    .iter()
                    .find(|entry| entry.name == comparison.numerator)
                    .and_then(|entry| metric.median_of(entry)),
                medians
                    .iter()
                    .find(|entry| entry.name == comparison.denominator)
                    .and_then(|entry| metric.median_of(entry)),
            ) else {
                continue;
            };
            if denominator <= 0.0 || numerator <= 0.0 {
                continue;
            }
            let blocks: Vec<PairedObservation> = valid
                .iter()
                .filter_map(|attempt| {
                    let find = |name: &str| {
                        attempt
                            .subjects
                            .iter()
                            .find(|entry| entry.name == name)
                            .and_then(|entry| metric.observed(entry))
                    };
                    match (find(&comparison.numerator), find(&comparison.denominator)) {
                        (Some(candidate), Some(baseline)) => Some(PairedObservation {
                            baseline,
                            candidate,
                        }),
                        _ => None,
                    }
                })
                .collect();
            let Ok(interval) = crate::bootstrap::paired_ratio_interval(&blocks, &bootstrap) else {
                continue;
            };
            ratios.push(PairedRatio {
                numerator_subject: comparison.numerator.clone(),
                denominator_subject: comparison.denominator.clone(),
                metric: metric.field().to_owned(),
                ratio_of_medians: numerator / denominator,
                ci_low: interval.lower,
                ci_high: interval.upper,
            });
        }
    }
    Ok(ratios)
}

/// Reports whether a pair cleared the practical threshold on the favorable
/// side, with the whole interval.
#[must_use]
pub fn pair_passes(pair: &PairedRatio, metric: SuiteMetric, threshold: f64) -> bool {
    if metric.higher_is_better() {
        pair.ci_low > 1.0 + threshold
    } else {
        pair.ci_high < 1.0 - threshold
    }
}

/// Reports whether a pair is a regression: the whole interval cleared the
/// threshold on the *unfavorable* side.
#[must_use]
pub fn pair_regresses(pair: &PairedRatio, metric: SuiteMetric, threshold: f64) -> bool {
    if metric.higher_is_better() {
        pair.ci_high < 1.0 - threshold
    } else {
        pair.ci_low > 1.0 + threshold
    }
}

/// Returns the metric a `kafkars.suite-summary.v1` field name refers to.
#[must_use]
pub fn metric_of_field(field: &str) -> Option<SuiteMetric> {
    SuiteMetric::ALL
        .into_iter()
        .find(|metric| metric.field() == field)
}

/// Every gate the suite checked, in the order it checked them.
fn gates(
    comparisons: &[Comparison],
    pairs: &[PairedRatio],
    pair_dispersion: &[SubjectDispersion],
    valid: &[&SuiteAttempt],
    options: &SuiteOptions,
) -> Vec<GateOutcome> {
    let mut gates = vec![
        GateOutcome {
            name: "attempts-valid".to_owned(),
            description: "at least one attempt has to be usable evidence".to_owned(),
            passed: !valid.is_empty(),
            detail: format!("{} valid attempts", valid.len()),
        },
        GateOutcome {
            name: "paired-repetitions".to_owned(),
            description: format!(
                "a comparison needs at least {MINIMUM_PAIRED_REPETITIONS} valid paired attempts"
            ),
            passed: valid.len() >= MINIMUM_PAIRED_REPETITIONS,
            detail: format!("{} of {MINIMUM_PAIRED_REPETITIONS} required", valid.len()),
        },
    ];

    // Goodput and p99 for every comparison: the series that must exist before
    // the budget can say anything.
    let expected_series = 2 * comparisons.len();
    let gated: Vec<&SubjectDispersion> = pair_dispersion
        .iter()
        .filter(|entry| {
            matches!(
                metric_of_field(&entry.metric),
                Some(SuiteMetric::Goodput | SuiteMetric::P99Latency)
            )
        })
        .collect();
    let noisy: Vec<String> = gated
        .iter()
        .filter(|entry| {
            !entry
                .coefficient_of_variation
                .is_some_and(|value| value <= COEFFICIENT_OF_VARIATION_BUDGET)
        })
        .map(|entry| {
            entry.coefficient_of_variation.map_or_else(
                || format!("{} {} (undefined)", entry.name, entry.metric),
                |value| format!("{} {} ({value:.4})", entry.name, entry.metric),
            )
        })
        .collect();
    gates.push(GateOutcome {
        name: "dispersion-within-budget".to_owned(),
        description: format!(
            "every compared pair's goodput and p99 ratio must vary by no more than \
             {COEFFICIENT_OF_VARIATION_BUDGET:.2} of its mean, over the same per-attempt \
             ratios the interval is drawn from"
        ),
        // A suite with no ratio series to judge does not pass this gate by
        // having nothing to fail. One subject, or no attempt where both subjects
        // reported, means the dispersion the budget is about was never measured,
        // and "not measured" is not "inside the budget".
        passed: expected_series > 0 && gated.len() == expected_series && noisy.is_empty(),
        detail: if expected_series == 0 {
            "there is no compared pair, so the dispersion this budget is about was never \
             measured"
                .to_owned()
        } else if gated.len() != expected_series {
            format!(
                "{} of the {expected_series} gated ratio series could be formed at all",
                gated.len()
            )
        } else if noisy.is_empty() {
            "every gated ratio dispersion is inside the budget".to_owned()
        } else {
            format!("outside the budget or undefined: {}", noisy.join(", "))
        },
    });

    for comparison in comparisons {
        for metric in SuiteMetric::ALL {
            let Some(pair) = pairs.iter().find(|pair| {
                pair.numerator_subject == comparison.numerator
                    && pair.denominator_subject == comparison.denominator
                    && pair.metric == metric.field()
            }) else {
                continue;
            };
            gates.push(GateOutcome {
                name: format!(
                    "{}-over-{}:{}",
                    comparison.numerator,
                    comparison.denominator,
                    metric.field()
                ),
                description: metric.gate_description(options.practical_threshold),
                passed: pair_passes(pair, metric, options.practical_threshold),
                detail: format!(
                    "ratio of medians {:.4}, interval [{:.4}, {:.4}]",
                    pair.ratio_of_medians, pair.ci_low, pair.ci_high
                ),
            });
        }
    }
    gates
}

/// Per-subject request economics totalled over the valid attempts.
fn subject_economics(
    subjects: &[SubjectIdentity],
    loaded: &[LoadedAttempt],
    attempts: &[SuiteAttempt],
    notes: &mut Vec<String>,
) -> Vec<SubjectEconomics> {
    let mut all = Vec::new();
    for subject in subjects {
        let mut parts = Vec::new();
        let mut acknowledged = 0u64;
        for (attempt, loaded) in attempts.iter().zip(loaded) {
            if !attempt.run_valid {
                continue;
            }
            if let Some((count, economics)) = loaded.economics(&subject.name) {
                acknowledged = acknowledged.saturating_add(count);
                parts.push(economics);
            }
        }
        if parts.is_empty() {
            notes.push(format!(
                "subject {} reports no native client statistics, so its request economics are \
                 absent rather than zero",
                subject.name
            ));
            continue;
        }
        all.push(SubjectEconomics {
            subject: subject.name.clone(),
            role: subject.role.clone(),
            attempts_reported: parts.len(),
            totals: RequestEconomics::total(&parts, acknowledged),
        });
    }
    all
}
