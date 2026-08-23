//! The Markdown suite report.
//!
//! Section order is the reading order: comparison first, then the underlying
//! medians, checks, stability, traffic cost, and attempts. Nothing here
//! recomputes a statistic — every number was decided in `suite`.

use std::fmt::Write as _;

use bench_schema::{SubjectMedians, SuiteSummary};

use crate::suite::{SubjectEconomics, metric_of_field};

use super::economics::write_markdown_economics;
use super::format::{
    SCORECARD_ALIGNMENT, SCORECARD_HEADER, direction_word, dispersion_role, format_mib, format_ms,
    format_percent, format_rate, format_ratio, format_seconds, optional_ms, pass_word, valid_count,
    verdict_word, yes_no,
};
use super::{DIAGNOSTIC_BANNER, NOT_REPORTED};

/// Renders a suite summary as Markdown.
#[must_use]
pub fn render_markdown_suite(summary: &SuiteSummary) -> String {
    markdown_suite(summary, &[])
}

/// Renders a suite summary and its economics as Markdown.
pub(super) fn markdown_suite(summary: &SuiteSummary, economics: &[SubjectEconomics]) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "# {} suite\n", summary.scenario_name);
    let _ = writeln!(out, "> {DIAGNOSTIC_BANNER}\n");
    let _ = writeln!(
        out,
        "- evidence: {} of {} attempts valid\n- public claim allowed: {}\n- practical threshold: \
         {}\n- analysis: {} bootstrap resamples, seed `{}`\n- experiment: `{}`\n",
        valid_count(summary),
        summary.repetitions,
        yes_no(summary.claim_eligible),
        format_percent(summary.practical_threshold),
        summary.resamples,
        summary.seed,
        summary.experiment_id,
    );
    write_markdown_pairs(&mut out, summary);
    write_markdown_scorecard(&mut out, summary);
    write_markdown_gates(&mut out, summary);
    write_markdown_dispersion(&mut out, summary);
    write_markdown_economics(&mut out, economics);
    write_markdown_attempts(&mut out, summary);
    if !summary.notes.is_empty() {
        let _ = writeln!(out, "\n## Notes\n");
        for note in &summary.notes {
            let _ = writeln!(out, "- {note}");
        }
    }
    out
}

/// The medians table.
fn write_markdown_scorecard(out: &mut String, summary: &SuiteSummary) {
    let _ = writeln!(out, "## Scorecard\n");
    if summary.medians.is_empty() {
        let _ = writeln!(
            out,
            "No attempt was valid, so there is no median to report.\n"
        );
        return;
    }
    let _ = writeln!(
        out,
        "Medians from valid attempts. Latency runs from offer to terminal and includes \
         admission wait. Lateness and accepted-to-terminal only help locate a difference; \
         refusing work can improve both.\n"
    );
    let _ = writeln!(out, "{SCORECARD_HEADER}");
    let _ = writeln!(out, "{SCORECARD_ALIGNMENT}");
    for median in &summary.medians {
        let _ = writeln!(out, "{}", markdown_median_row(median));
    }
}

/// The paired-ratio table.
fn write_markdown_pairs(out: &mut String, summary: &SuiteSummary) {
    let _ = writeln!(out, "## Result\n");
    if summary.pairs.is_empty() {
        let _ = writeln!(out, "No pair had enough valid attempts to compare.\n");
        return;
    }
    let _ = writeln!(
        out,
        "Ratios are numerator / denominator. Direction says which way is better. A result \
         is favorable only when its full confidence interval clears the practical threshold.\n"
    );
    let _ = writeln!(
        out,
        "| Comparison | Metric | Ratio | CI low | CI high | Direction | Result |"
    );
    let _ = writeln!(out, "| --- | --- | ---: | ---: | ---: | --- | --- |");
    for pair in &summary.pairs {
        let Some(metric) = metric_of_field(&pair.metric) else {
            continue;
        };
        let _ = writeln!(
            out,
            "| {} / {} | {} | {} | {} | {} | {} | {} |",
            pair.numerator_subject,
            pair.denominator_subject,
            metric.label(),
            format_ratio(pair.ratio_of_medians),
            format_ratio(pair.ci_low),
            format_ratio(pair.ci_high),
            direction_word(metric),
            verdict_word(pair, metric, summary.practical_threshold)
        );
    }
    let _ = writeln!(out);
}

/// The gate table.
fn write_markdown_gates(out: &mut String, summary: &SuiteSummary) {
    let _ = writeln!(out, "\n## Checks\n");
    let _ = writeln!(out, "| Check | Result | Requirement | Observed |");
    let _ = writeln!(out, "| --- | --- | --- | --- |");
    for gate in &summary.gates {
        let _ = writeln!(
            out,
            "| `{}` | {} | {} | {} |",
            gate.name,
            pass_word(gate.passed),
            gate.description,
            gate.detail
        );
    }
}

/// The dispersion table. See [`dispersion_role`] for the two kinds of row and
/// why both are shown.
fn write_markdown_dispersion(out: &mut String, summary: &SuiteSummary) {
    let _ = writeln!(out, "\n## Run stability\n");
    let _ = writeln!(out, "| Series | Metric | Variation | Used by check |");
    let _ = writeln!(out, "| --- | --- | ---: | :---: |");
    for entry in &summary.dispersion {
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} |",
            entry.name,
            metric_of_field(&entry.metric).map_or(entry.metric.as_str(), |metric| metric.label()),
            entry
                .coefficient_of_variation
                .map_or_else(|| NOT_REPORTED.to_owned(), format_ratio),
            dispersion_role(entry)
        );
    }
    let _ = writeln!(
        out,
        "\nRatio variation is checked against the noise budget. Subject variation only shows \
         whether the machine was steady."
    );
}

/// The attempt roster, valid and invalid alike.
fn write_markdown_attempts(out: &mut String, summary: &SuiteSummary) {
    let _ = writeln!(out, "\n## Attempts\n");
    let _ = writeln!(
        out,
        "| Attempt | Execution status | Valid | Bundle digest |"
    );
    let _ = writeln!(out, "| --- | --- | --- | --- |");
    for attempt in &summary.attempts {
        let _ = writeln!(
            out,
            "| `{}` | {} | {} | `{}` |",
            attempt.attempt_id,
            attempt.execution_status.as_str(),
            attempt.run_valid,
            attempt.bundle_digest
        );
    }
}

/// One scorecard row.
fn markdown_median_row(median: &SubjectMedians) -> String {
    format!(
        "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
        median.name,
        median.role.clone().unwrap_or_else(|| "-".to_owned()),
        format_rate(median.acknowledged_records_per_second),
        format_ms(median.p50_intended_to_terminal_ns),
        format_ms(median.p99_intended_to_terminal_ns),
        format_ms(median.p999_intended_to_terminal_ns),
        format_ms(median.p99_admission_wait_ns),
        optional_ms(median.p99_intended_to_call_start_ns),
        format_ms(median.p99_accepted_to_terminal_ns),
        median
            .cpu_core_seconds
            .map_or_else(|| NOT_REPORTED.to_owned(), format_seconds),
        median
            .cpu_core_seconds_per_million_acknowledged
            .map_or_else(|| NOT_REPORTED.to_owned(), format_seconds),
        median
            .max_rss_bytes
            .map_or_else(|| NOT_REPORTED.to_owned(), format_mib)
    )
}
