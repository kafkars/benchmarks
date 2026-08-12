//! The single-attempt Markdown view over one sealed bundle.
//!
//! A single attempt has no medians, no intervals, and no gates, and this view
//! says so. It exists to answer "what happened in this run", which is the
//! question an operator has in front of a bundle directory — and it is
//! deliberately the only rendering that reads a bundle from disk rather than a
//! summary that was already decided.

use std::fmt::Write as _;
use std::path::Path;

use bench_schema::ProducerBenchmarkV2;

use crate::error::ReportResult;
use crate::stats::histogram_percentile;
use crate::suite::{LoadedAttempt, SubjectEconomics};

use super::economics::write_markdown_economics;
use super::format::{
    SCORECARD_ALIGNMENT, SCORECARD_HEADER, format_mib, format_rate, format_seconds,
    nanos_to_seconds, optional_ms,
};
use super::{DIAGNOSTIC_BANNER, NOT_REPORTED};

/// Renders one sealed bundle as Markdown: the single-attempt view.
///
/// A single attempt has no medians, no intervals, and no gates, and this view
/// says so. It exists to answer "what happened in this run", which is the
/// question an operator has in front of a bundle directory.
///
/// # Errors
///
/// Returns an error when the bundle's documents cannot be read or parsed.
pub fn render_markdown_bundle(bundle_root: &Path) -> ReportResult<String> {
    let attempt = LoadedAttempt::read(bundle_root)?;
    let mut out = String::new();
    let _ = writeln!(out, "# Attempt {}\n", attempt.attempt_id());
    let _ = writeln!(out, "> {DIAGNOSTIC_BANNER}\n");
    let _ = writeln!(
        out,
        "- scenario: `{}`\n- execution status: `{}`\n- run valid: `{}`\n- bundle digest: `{}`\n",
        attempt.experiment.name,
        attempt.status.execution_status.as_str(),
        attempt.classification.run_valid,
        attempt.bundle_digest
    );
    write_bundle_scorecard(&mut out, &attempt);
    write_bundle_accounting(&mut out, &attempt);
    write_markdown_economics(&mut out, &bundle_economics(&attempt));
    write_bundle_caveats(&mut out, &attempt);
    Ok(out)
}

/// The single-attempt scorecard, with percentiles read from the histograms.
fn write_bundle_scorecard(out: &mut String, attempt: &LoadedAttempt) {
    let _ = writeln!(out, "## Subjects\n");
    let _ = writeln!(out, "{SCORECARD_HEADER}");
    let _ = writeln!(out, "{SCORECARD_ALIGNMENT}");
    for (name, result) in &attempt.results {
        let role = attempt
            .experiment
            .subject(name)
            .and_then(|subject| subject.role.clone())
            .unwrap_or_else(|| "-".to_owned());
        let cpu = result.resources.map(|resources| {
            nanos_to_seconds(
                resources
                    .user_cpu_ns
                    .saturating_add(resources.system_cpu_ns),
            )
        });
        let _ = writeln!(
            out,
            "| {name} | {role} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            format_rate(result.throughput.acknowledged_records_per_second),
            optional_ms(terminal_percentile(result, 0.50)),
            optional_ms(terminal_percentile(result, 0.99)),
            optional_ms(terminal_percentile(result, 0.999)),
            optional_ms(percentile_of(&result.timing.call_start_to_accepted)),
            optional_ms(
                result
                    .timing
                    .intended_to_call_start
                    .as_ref()
                    .and_then(percentile_of)
            ),
            optional_ms(percentile_of(&result.timing.accepted_to_terminal)),
            cpu.map_or_else(|| NOT_REPORTED.to_owned(), format_seconds),
            cpu.and_then(|seconds| per_million(seconds, result.outcomes.acknowledged))
                .map_or_else(|| NOT_REPORTED.to_owned(), format_seconds),
            result
                .resources
                .map_or_else(|| NOT_REPORTED.to_owned(), |r| format_mib(r.max_rss_bytes)),
        );
    }
}

/// One histogram's 99th percentile, absent when it cannot be read.
fn percentile_of(histogram: &bench_schema::EncodedHistogram) -> Option<u64> {
    histogram_percentile(histogram, 0.99).ok().flatten()
}

/// CPU core-seconds normalized to a million acknowledged records.
#[expect(
    clippy::cast_precision_loss,
    reason = "reporting statistic over a record count, not identity arithmetic"
)]
fn per_million(core_seconds: f64, acknowledged: u64) -> Option<f64> {
    (acknowledged > 0).then(|| core_seconds * 1_000_000.0 / acknowledged as f64)
}

/// Where every offer ended, for one attempt.
fn write_bundle_accounting(out: &mut String, attempt: &LoadedAttempt) {
    let _ = writeln!(out, "\n## Offer accounting\n");
    let _ = writeln!(
        out,
        "| Subject | Offered | Accepted | Acknowledged | Failed | Timed out | Unknown | \
         Final outstanding | Adapter valid |"
    );
    let _ = writeln!(
        out,
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |"
    );
    for (name, result) in &attempt.results {
        let outcomes = result.outcomes;
        let _ = writeln!(
            out,
            "| {name} | {} | {} | {} | {} | {} | {} | {} | {} |",
            outcomes.offered,
            outcomes.accepted,
            outcomes.acknowledged,
            outcomes.failed,
            outcomes.timed_out,
            outcomes.unknown,
            result.queue.final_outstanding,
            result.valid
        );
    }
}

/// Why an attempt is unusable, and what it did not check.
fn write_bundle_caveats(out: &mut String, attempt: &LoadedAttempt) {
    if !attempt.invalid_reasons.is_empty() {
        let _ = writeln!(out, "\n## Why this attempt is not usable evidence\n");
        for reason in &attempt.invalid_reasons {
            let _ = writeln!(out, "- {reason}");
        }
    }
    if !attempt.classification.deferred_checks.is_empty() {
        let _ = writeln!(out, "\n## Checks this attempt did not perform\n");
        for check in &attempt.classification.deferred_checks {
            let _ = writeln!(out, "- {check}");
        }
    }
}

/// One attempt's per-subject economics, for the single-attempt view.
fn bundle_economics(attempt: &LoadedAttempt) -> Vec<SubjectEconomics> {
    attempt
        .results
        .iter()
        .filter_map(|(name, _)| {
            attempt.economics(name).map(|(_, totals)| SubjectEconomics {
                subject: name.clone(),
                role: None,
                attempts_reported: 1,
                totals,
            })
        })
        .collect()
}

/// One offer-to-terminal percentile, absent when nothing was recorded.
fn terminal_percentile(result: &ProducerBenchmarkV2, quantile: f64) -> Option<u64> {
    histogram_percentile(&result.timing.intended_to_terminal, quantile)
        .ok()
        .flatten()
}
