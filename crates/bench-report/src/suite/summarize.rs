//! The one pass that turns a set of sealed bundles into a suite summary.
//!
//! Every attempt appears in the output. Attempts that failed a validity check
//! are carried with `run_valid: false`, excluded from every median, ratio, and
//! interval, and named in `notes` with the reason. A median over a silently
//! filtered set is a number nobody can reconstruct.
//!
//! `claim_eligible` is false, always, for every summary this milestone can
//! produce.

use std::path::PathBuf;

use bench_schema::{SuiteAttempt, SuiteSummary};

use crate::economics::RequestEconomics;
use crate::error::{ReportError, ReportResult};

use super::attempt::LoadedAttempt;
use super::comparison::{SubjectIdentity, comparison_pairs};
use super::gates::gates;
use super::medians::{pair_dispersion, subject_dispersion, subject_medians};
use super::pairs::paired_ratios;
use super::report::{SubjectEconomics, SuiteOptions, SuiteReport};
use super::surface::review_execution_surface;

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
    // A product-surface difference is reported beside the numbers rather than
    // instead of them: the note travels into every rendering, and the ratio it
    // qualifies stays exactly where it was.
    let surface = review_execution_surface(&comparisons, &valid);
    notes.extend(surface.notes.iter().cloned());
    let gates = gates(
        &comparisons,
        &pairs,
        &pair_dispersion,
        &valid,
        &surface,
        options,
    );
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
