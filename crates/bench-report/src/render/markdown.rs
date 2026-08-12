//! The Markdown suite report.
//!
//! Section order is the reading order: what each subject did, what the
//! comparison did, what the gates concluded, how steady the run was, what the
//! traffic cost, and which attempts it was all drawn from. Nothing here
//! recomputes a statistic — every number was decided in `suite`.

use std::fmt::Write as _;

use bench_schema::{SubjectMedians, SuiteSummary};

use crate::suite::{SubjectEconomics, metric_of_field};

use super::economics::write_markdown_economics;
use super::format::{
    SCORECARD_ALIGNMENT, SCORECARD_HEADER, direction_word, dispersion_role, format_mib, format_ms,
    format_rate, format_ratio, format_seconds, pass_word, valid_count, verdict_word,
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
        "- experiment: `{}`\n- attempts: {} requested, {} valid\n- practical threshold: \
         {}\n- bootstrap: {} resamples, seed `{}`\n- claim eligible: `{}`\n",
        summary.experiment_id,
        summary.repetitions,
        valid_count(summary),
        format_ratio(summary.practical_threshold),
        summary.resamples,
        summary.seed,
        summary.claim_eligible
    );
    write_markdown_scorecard(&mut out, summary);
    write_markdown_pairs(&mut out, summary);
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
        "Medians over the valid attempts. Latency is offer-to-terminal, so it includes \
         admission wait.\n"
    );
    let _ = writeln!(out, "{SCORECARD_HEADER}");
    let _ = writeln!(out, "{SCORECARD_ALIGNMENT}");
    for median in &summary.medians {
        let _ = writeln!(out, "{}", markdown_median_row(median));
    }
}

/// The paired-ratio table.
fn write_markdown_pairs(out: &mut String, summary: &SuiteSummary) {
    let _ = writeln!(out, "\n## Paired comparisons\n");
    if summary.pairs.is_empty() {
        let _ = writeln!(out, "No pair had enough valid attempts to compare.\n");
        return;
    }
    let _ = writeln!(
        out,
        "Ratios are numerator over denominator of the medians. The interval is the \
         paired-block percentile bootstrap over the per-attempt pairs.\n"
    );
    let _ = writeln!(
        out,
        "| Comparison | Metric | Ratio | CI low | CI high | Direction | Clears threshold |"
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
}

/// The gate table.
fn write_markdown_gates(out: &mut String, summary: &SuiteSummary) {
    let _ = writeln!(out, "\n## Gates\n");
    let _ = writeln!(out, "| Gate | Result | Rule | Observed |");
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
    let _ = writeln!(out, "\n## Dispersion\n");
    let _ = writeln!(
        out,
        "| Series | Metric | Coefficient of variation | Gated |"
    );
    let _ = writeln!(out, "| --- | --- | ---: | :---: |");
    for entry in &summary.dispersion {
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} |",
            entry.name,
            entry.metric,
            entry
                .coefficient_of_variation
                .map_or_else(|| NOT_REPORTED.to_owned(), format_ratio),
            dispersion_role(entry)
        );
    }
    let _ = writeln!(
        out,
        "\nRatio series are gated against the noise budget. Per-subject rows are \
         informational: they say whether the machine was steady, which is a different \
         question from whether the comparison was."
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
        "| {} | {} | {} | {} | {} | {} | {} | {} | {} |",
        median.name,
        median.role.clone().unwrap_or_else(|| "-".to_owned()),
        format_rate(median.acknowledged_records_per_second),
        format_ms(median.p50_intended_to_terminal_ns),
        format_ms(median.p99_intended_to_terminal_ns),
        format_ms(median.p999_intended_to_terminal_ns),
        format_ms(median.p99_admission_wait_ns),
        median
            .cpu_core_seconds
            .map_or_else(|| NOT_REPORTED.to_owned(), format_seconds),
        median
            .max_rss_bytes
            .map_or_else(|| NOT_REPORTED.to_owned(), format_mib)
    )
}
