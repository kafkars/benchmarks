//! The self-contained HTML suite report.
//!
//! No stylesheet, no script, no font, no image request: the page is a single
//! self-contained document, so it can be attached to an issue or opened from a
//! sealed bundle on a machine with no network. The interval bars are inline SVG
//! for the same reason.
//!
//! The section order is the Markdown report's, and the numbers are formatted by
//! the same code, so the two renderings can only differ in markup.

use std::fmt::Write as _;

use bench_schema::SuiteSummary;

use crate::suite::{SubjectEconomics, metric_of_field};

use super::bar::ratio_bar;
use super::economics::html_economics;
use super::format::{
    dispersion_role, escape, format_mib, format_ms, format_rate, format_ratio, format_seconds,
    pass_word, valid_count, verdict_word,
};
use super::style::STYLE;
use super::{DIAGNOSTIC_BANNER, NOT_REPORTED};

/// Renders a suite summary as a self-contained HTML page.
#[must_use]
pub fn render_html_suite(summary: &SuiteSummary) -> String {
    html_suite(summary, &[])
}

/// Renders a suite summary and its economics as a self-contained HTML page.
pub(super) fn html_suite(summary: &SuiteSummary, economics: &[SubjectEconomics]) -> String {
    let mut out = String::new();
    html_head(&mut out, summary);
    html_scorecard(&mut out, summary);
    html_pairs(&mut out, summary);
    html_gates(&mut out, summary);
    html_dispersion(&mut out, summary);
    html_economics(&mut out, economics);
    html_attempts(&mut out, summary);
    if !summary.notes.is_empty() {
        let _ = writeln!(out, "<h2>Notes</h2>\n<ul>");
        for note in &summary.notes {
            let _ = writeln!(out, "<li>{}</li>", escape(note));
        }
        let _ = writeln!(out, "</ul>");
    }
    let _ = writeln!(out, "</body>\n</html>");
    out
}

/// The document head, banner, and the facts a reader needs before the numbers.
fn html_head(out: &mut String, summary: &SuiteSummary) {
    let _ = writeln!(
        out,
        "<!doctype html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n\
         <title>{} suite</title>\n<style>{}</style>\n</head>\n<body>",
        escape(&summary.scenario_name),
        STYLE
    );
    let _ = writeln!(
        out,
        "<h1>{} suite</h1>\n<p class=\"banner\">{}</p>",
        escape(&summary.scenario_name),
        escape(DIAGNOSTIC_BANNER)
    );
    let _ = writeln!(
        out,
        "<dl class=\"facts\">\
         <dt>experiment</dt><dd><code>{}</code></dd>\
         <dt>attempts</dt><dd>{} requested, {} valid</dd>\
         <dt>practical threshold</dt><dd>{}</dd>\
         <dt>bootstrap</dt><dd>{} resamples, seed <code>{}</code></dd>\
         <dt>claim eligible</dt><dd>{}</dd>\
         </dl>",
        escape(summary.experiment_id.as_str()),
        summary.repetitions,
        valid_count(summary),
        format_ratio(summary.practical_threshold),
        summary.resamples,
        summary.seed,
        summary.claim_eligible
    );
}

/// The medians table.
fn html_scorecard(out: &mut String, summary: &SuiteSummary) {
    let _ = writeln!(out, "<h2>Scorecard</h2>");
    if summary.medians.is_empty() {
        let _ = writeln!(
            out,
            "<p>No attempt was valid, so there is no median to report.</p>"
        );
        return;
    }
    let _ = writeln!(
        out,
        "<table><thead><tr><th>Subject</th><th>Role</th>\
         <th class=\"n\">Goodput (records/s)</th><th class=\"n\">p50 (ms)</th>\
         <th class=\"n\">p99 (ms)</th><th class=\"n\">p99.9 (ms)</th>\
         <th class=\"n\">Admission p99 (ms)</th><th class=\"n\">CPU (core-s)</th>\
         <th class=\"n\">Peak RSS (MiB)</th></tr></thead><tbody>"
    );
    for median in &summary.medians {
        let _ = writeln!(
            out,
            "<tr><td>{}</td><td>{}</td><td class=\"n\">{}</td><td class=\"n\">{}</td>\
             <td class=\"n\">{}</td><td class=\"n\">{}</td><td class=\"n\">{}</td>\
             <td class=\"n\">{}</td><td class=\"n\">{}</td></tr>",
            escape(&median.name),
            escape(median.role.as_deref().unwrap_or("-")),
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
        );
    }
    let _ = writeln!(out, "</tbody></table>");
}

/// The paired-ratio table, each row carrying its inline interval bar.
fn html_pairs(out: &mut String, summary: &SuiteSummary) {
    let _ = writeln!(out, "<h2>Paired comparisons</h2>");
    if summary.pairs.is_empty() {
        let _ = writeln!(out, "<p>No pair had enough valid attempts to compare.</p>");
        return;
    }
    let _ = writeln!(
        out,
        "<p>Ratios are numerator over denominator of the medians; the bar shows the \
         confidence interval against parity and the practical threshold.</p>\n\
         <table><thead><tr><th>Comparison</th><th>Metric</th><th class=\"n\">Ratio</th>\
         <th class=\"n\">CI low</th><th class=\"n\">CI high</th><th>Interval</th>\
         <th>Clears threshold</th></tr></thead><tbody>"
    );
    for pair in &summary.pairs {
        let Some(metric) = metric_of_field(&pair.metric) else {
            continue;
        };
        let _ = writeln!(
            out,
            "<tr><td>{} / {}</td><td>{}</td><td class=\"n\">{}</td><td class=\"n\">{}</td>\
             <td class=\"n\">{}</td><td>{}</td><td>{}</td></tr>",
            escape(&pair.numerator_subject),
            escape(&pair.denominator_subject),
            escape(metric.label()),
            format_ratio(pair.ratio_of_medians),
            format_ratio(pair.ci_low),
            format_ratio(pair.ci_high),
            ratio_bar(pair, metric, summary.practical_threshold),
            verdict_word(pair, metric, summary.practical_threshold)
        );
    }
    let _ = writeln!(out, "</tbody></table>");
}

/// The gate table.
fn html_gates(out: &mut String, summary: &SuiteSummary) {
    let _ = writeln!(
        out,
        "<h2>Gates</h2>\n<table><thead><tr><th>Gate</th><th>Result</th><th>Rule</th>\
         <th>Observed</th></tr></thead><tbody>"
    );
    for gate in &summary.gates {
        let _ = writeln!(
            out,
            "<tr><td><code>{}</code></td><td class=\"{}\">{}</td><td>{}</td><td>{}</td></tr>",
            escape(&gate.name),
            pass_word(gate.passed),
            pass_word(gate.passed),
            escape(&gate.description),
            escape(&gate.detail)
        );
    }
    let _ = writeln!(out, "</tbody></table>");
}

/// The dispersion table. See [`dispersion_role`] for the two kinds of row and
/// why both are shown.
fn html_dispersion(out: &mut String, summary: &SuiteSummary) {
    let _ = writeln!(
        out,
        "<h2>Dispersion</h2>\n<table><thead><tr><th>Series</th><th>Metric</th>\
         <th class=\"n\">Coefficient of variation</th><th>Gated</th></tr></thead><tbody>"
    );
    for entry in &summary.dispersion {
        let _ = writeln!(
            out,
            "<tr><td>{}</td><td>{}</td><td class=\"n\">{}</td><td>{}</td></tr>",
            escape(&entry.name),
            escape(&entry.metric),
            entry
                .coefficient_of_variation
                .map_or_else(|| NOT_REPORTED.to_owned(), format_ratio),
            dispersion_role(entry)
        );
    }
    let _ = writeln!(
        out,
        "</tbody></table>\n<p>Ratio series are gated against the noise budget. \
         Per-subject rows are informational: they say whether the machine was steady, \
         which is a different question from whether the comparison was.</p>"
    );
}

/// The attempt roster, valid and invalid alike.
fn html_attempts(out: &mut String, summary: &SuiteSummary) {
    let _ = writeln!(
        out,
        "<h2>Attempts</h2>\n<table><thead><tr><th>Attempt</th><th>Execution status</th>\
         <th>Valid</th><th>Bundle digest</th></tr></thead><tbody>"
    );
    for attempt in &summary.attempts {
        let _ = writeln!(
            out,
            "<tr><td><code>{}</code></td><td>{}</td><td class=\"{}\">{}</td>\
             <td><code>{}</code></td></tr>",
            escape(&attempt.attempt_id),
            escape(attempt.execution_status.as_str()),
            pass_word(attempt.run_valid),
            attempt.run_valid,
            escape(&attempt.bundle_digest)
        );
    }
    let _ = writeln!(out, "</tbody></table>");
}
