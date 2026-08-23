//! The self-contained HTML suite report.
//!
//! Styles and interval bars are embedded for offline use. Section order and
//! number formatting match the Markdown report.

use std::fmt::Write as _;

use bench_schema::SuiteSummary;

use crate::suite::{SubjectEconomics, metric_of_field};

use super::bar::ratio_bar;
use super::economics::html_economics;
use super::format::{
    dispersion_role, escape, format_mib, format_ms, format_percent, format_rate, format_ratio,
    format_seconds, optional_ms, pass_word, valid_count, verdict_word, yes_no,
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
    html_pairs(&mut out, summary);
    html_scorecard(&mut out, summary);
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
         <dt>evidence</dt><dd>{} of {} attempts valid</dd>\
         <dt>public claim allowed</dt><dd>{}</dd>\
         <dt>practical threshold</dt><dd>{}</dd>\
         <dt>analysis</dt><dd>{} bootstrap resamples, seed <code>{}</code></dd>\
         <dt>experiment</dt><dd><code>{}</code></dd>\
         </dl>",
        valid_count(summary),
        summary.repetitions,
        yes_no(summary.claim_eligible),
        format_percent(summary.practical_threshold),
        summary.resamples,
        summary.seed,
        escape(summary.experiment_id.as_str()),
    );
}

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
        "<p>Medians from valid attempts. Latency runs from offer to terminal and includes \
         admission wait. Lateness and accepted-to-terminal only help locate a difference; \
         refusing work can improve both.</p>\n\
         <table><thead><tr><th>Subject</th><th>Role</th>\
         <th class=\"n\">Goodput (records/s)</th><th class=\"n\">p50 (ms)</th>\
         <th class=\"n\">p99 (ms)</th><th class=\"n\">p99.9 (ms)</th>\
         <th class=\"n\">Admission p99 (ms)</th><th class=\"n\">Lateness p99 (ms)</th>\
         <th class=\"n\">Accepted-to-terminal p99 (ms)</th><th class=\"n\">CPU (core-s)</th>\
         <th class=\"n\">CPU per 1M ack (core-s)</th>\
         <th class=\"n\">Peak RSS (MiB)</th></tr></thead><tbody>"
    );
    for median in &summary.medians {
        let _ = writeln!(
            out,
            "<tr><td>{}</td><td>{}</td><td class=\"n\">{}</td><td class=\"n\">{}</td>\
             <td class=\"n\">{}</td><td class=\"n\">{}</td><td class=\"n\">{}</td>\
             <td class=\"n\">{}</td><td class=\"n\">{}</td><td class=\"n\">{}</td>\
             <td class=\"n\">{}</td><td class=\"n\">{}</td></tr>",
            escape(&median.name),
            escape(median.role.as_deref().unwrap_or("-")),
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
        );
    }
    let _ = writeln!(out, "</tbody></table>");
}

fn html_pairs(out: &mut String, summary: &SuiteSummary) {
    let _ = writeln!(out, "<h2>Result</h2>");
    if summary.pairs.is_empty() {
        let _ = writeln!(out, "<p>No pair had enough valid attempts to compare.</p>");
        return;
    }
    let _ = writeln!(
        out,
        "<p>Ratios are numerator / denominator. A result is favorable only when its full \
         confidence interval clears the practical threshold. The bar shows that interval \
         against parity and the threshold.</p>\n\
         <table><thead><tr><th>Comparison</th><th>Metric</th><th class=\"n\">Ratio</th>\
         <th class=\"n\">CI low</th><th class=\"n\">CI high</th><th>Interval</th>\
         <th>Result</th></tr></thead><tbody>"
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

fn html_gates(out: &mut String, summary: &SuiteSummary) {
    let _ = writeln!(
        out,
        "<h2>Checks</h2>\n<table><thead><tr><th>Check</th><th>Result</th><th>Requirement</th>\
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

fn html_dispersion(out: &mut String, summary: &SuiteSummary) {
    let _ = writeln!(
        out,
        "<h2>Run stability</h2>\n<table><thead><tr><th>Series</th><th>Metric</th>\
         <th class=\"n\">Variation</th><th>Used by check</th></tr></thead><tbody>"
    );
    for entry in &summary.dispersion {
        let _ = writeln!(
            out,
            "<tr><td>{}</td><td>{}</td><td class=\"n\">{}</td><td>{}</td></tr>",
            escape(&entry.name),
            escape(
                metric_of_field(&entry.metric)
                    .map_or(entry.metric.as_str(), |metric| metric.label()),
            ),
            entry
                .coefficient_of_variation
                .map_or_else(|| NOT_REPORTED.to_owned(), format_ratio),
            dispersion_role(entry)
        );
    }
    let _ = writeln!(
        out,
        "</tbody></table>\n<p>Ratio variation is checked against the noise budget. \
         Subject variation only shows whether the machine was steady.</p>"
    );
}

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
