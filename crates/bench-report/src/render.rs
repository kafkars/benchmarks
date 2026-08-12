//! Human-readable renderings of evidence that has already been decided.
//!
//! # Rendering never decides anything
//!
//! Every verdict, ratio, interval, and gate outcome printed here was computed
//! in `suite`, and this module's only job is to lay it out. That separation is
//! what makes the reports safe to circulate: a renderer that could round a
//! failing gate into a passing sentence would be a way to launder a result.
//! Nothing here recomputes a statistic, and nothing here hides one.
//!
//! # Deterministic to the digit
//!
//! Every number is formatted with a fixed number of decimals, so the same
//! summary always renders the same bytes and two reports can be diffed. There
//! is no locale, no thousands separator, and no "about" — a reader comparing
//! two runs should see a diff exactly where the measurement moved.
//!
//! # Honest by construction
//!
//! Every report opens with the same banner: this evidence is diagnostic and
//! never claim-eligible. Absent measurements print as `not reported`, never as
//! `0`, and a subject whose client emits no native statistics is missing from
//! the economics table rather than present with a row of zeroes. Invalid
//! attempts are listed with the reason they were excluded, because the set a
//! median was taken over is part of the median.
//!
//! # The HTML is one file
//!
//! No stylesheet, no script, no font, no image request: the page is a single
//! self-contained document, so it can be attached to an issue or opened from a
//! sealed bundle on a machine with no network. The ratio bars are inline SVG
//! for the same reason. Colors come from CSS custom properties with a
//! `prefers-color-scheme` override, so the page is legible in either theme
//! without asking the reader to choose.

use std::fmt::Write as _;
use std::path::Path;

use bench_schema::{PairedRatio, ProducerBenchmarkV2, SubjectMedians, SuiteSummary};

use crate::error::ReportResult;
use crate::stats::histogram_percentile;
use crate::suite::{
    LoadedAttempt, SubjectEconomics, SuiteMetric, SuiteReport, metric_of_field, pair_passes,
    pair_regresses,
};

/// The sentence every report opens with.
///
/// Stated in the rendering rather than left to the reader's memory: a report
/// that circulates without its limits attached will eventually be quoted
/// without them.
pub const DIAGNOSTIC_BANNER: &str = "Diagnostic only — never claim-eligible. These numbers describe the machine and \
     configuration they were measured on, and support no published comparison between clients.";

/// What a number that was not measured prints as.
pub const NOT_REPORTED: &str = "not reported";

/// The scorecard's header row, shared by the suite and bundle views.
const SCORECARD_HEADER: &str = "| Subject | Role | Goodput (records/s) | p50 (ms) | p99 (ms) | \
                                p99.9 (ms) | Admission p99 (ms) | CPU (core-s) | Peak RSS (MiB) |";

/// The scorecard's alignment row: every numeric column is right-aligned.
const SCORECARD_ALIGNMENT: &str = "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |";

/// Renders a suite summary as Markdown.
#[must_use]
pub fn render_markdown_suite(summary: &SuiteSummary) -> String {
    markdown_suite(summary, &[])
}

/// Renders a suite summary as a self-contained HTML page.
#[must_use]
pub fn render_html_suite(summary: &SuiteSummary) -> String {
    html_suite(summary, &[])
}

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

impl SuiteReport {
    /// Renders this report as Markdown, including the request economics.
    #[must_use]
    pub fn markdown(&self) -> String {
        markdown_suite(&self.summary, &self.economics)
    }

    /// Renders this report as a self-contained HTML page, including the request
    /// economics.
    #[must_use]
    pub fn html(&self) -> String {
        html_suite(&self.summary, &self.economics)
    }
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
        let _ = writeln!(
            out,
            "| {name} | {role} | {} | {} | {} | {} | {} | {} | {} |",
            format_rate(result.throughput.acknowledged_records_per_second),
            optional_ms(terminal_percentile(result, 0.50)),
            optional_ms(terminal_percentile(result, 0.99)),
            optional_ms(terminal_percentile(result, 0.999)),
            optional_ms(
                histogram_percentile(&result.timing.call_start_to_accepted, 0.99)
                    .ok()
                    .flatten()
            ),
            result.resources.map_or_else(
                || NOT_REPORTED.to_owned(),
                |resources| format_seconds(nanos_to_seconds(
                    resources
                        .user_cpu_ns
                        .saturating_add(resources.system_cpu_ns)
                ))
            ),
            result
                .resources
                .map_or_else(|| NOT_REPORTED.to_owned(), |r| format_mib(r.max_rss_bytes)),
        );
    }
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

/// Renders a suite summary and its economics as Markdown.
fn markdown_suite(summary: &SuiteSummary, economics: &[SubjectEconomics]) -> String {
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

/// The dispersion table.
fn write_markdown_dispersion(out: &mut String, summary: &SuiteSummary) {
    let _ = writeln!(out, "\n## Dispersion\n");
    let _ = writeln!(out, "| Subject | Metric | Coefficient of variation |");
    let _ = writeln!(out, "| --- | --- | ---: |");
    for entry in &summary.dispersion {
        let _ = writeln!(
            out,
            "| {} | {} | {} |",
            entry.name,
            entry.metric,
            entry
                .coefficient_of_variation
                .map_or_else(|| NOT_REPORTED.to_owned(), format_ratio)
        );
    }
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

/// The request economics section, or the sentence that explains its absence.
fn write_markdown_economics(out: &mut String, economics: &[SubjectEconomics]) {
    let _ = writeln!(out, "\n## Request economics\n");
    if economics.is_empty() {
        let _ = writeln!(
            out,
            "No subject reported native client statistics, so there is nothing to report here. \
             An absent measurement is not a measurement of zero.\n"
        );
        return;
    }
    let _ = writeln!(
        out,
        "What each client spent in broker traffic for the records it delivered. Only subjects \
         whose client emits native statistics appear.\n"
    );
    let _ = writeln!(
        out,
        "| Subject | Produce requests | Per million acknowledged | Records per request | \
         Payload share of wire bytes | Records per batch | Retries | Timeouts |"
    );
    let _ = writeln!(
        out,
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    );
    for entry in economics {
        let totals = &entry.totals;
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} | {} | {} |",
            entry.subject,
            optional_count(totals.produce_requests),
            optional_rate(totals.produce_requests_per_million_acknowledged),
            optional_ratio(totals.records_per_produce_request),
            optional_ratio(totals.payload_bytes_per_transmitted_byte),
            optional_ratio(totals.batch_records.and_then(|window| window.mean())),
            optional_count(totals.retries),
            optional_count(totals.timeouts)
        );
    }
}

/// Renders a suite summary and its economics as a self-contained HTML page.
fn html_suite(summary: &SuiteSummary, economics: &[SubjectEconomics]) -> String {
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

/// The dispersion table.
fn html_dispersion(out: &mut String, summary: &SuiteSummary) {
    let _ = writeln!(
        out,
        "<h2>Dispersion</h2>\n<table><thead><tr><th>Subject</th><th>Metric</th>\
         <th class=\"n\">Coefficient of variation</th></tr></thead><tbody>"
    );
    for entry in &summary.dispersion {
        let _ = writeln!(
            out,
            "<tr><td>{}</td><td>{}</td><td class=\"n\">{}</td></tr>",
            escape(&entry.name),
            escape(&entry.metric),
            entry
                .coefficient_of_variation
                .map_or_else(|| NOT_REPORTED.to_owned(), format_ratio)
        );
    }
    let _ = writeln!(out, "</tbody></table>");
}

/// The request economics table, or the sentence that explains its absence.
fn html_economics(out: &mut String, economics: &[SubjectEconomics]) {
    let _ = writeln!(out, "<h2>Request economics</h2>");
    if economics.is_empty() {
        let _ = writeln!(
            out,
            "<p>No subject reported native client statistics, so there is nothing to report \
             here. An absent measurement is not a measurement of zero.</p>"
        );
        return;
    }
    let _ = writeln!(
        out,
        "<table><thead><tr><th>Subject</th><th class=\"n\">Produce requests</th>\
         <th class=\"n\">Per million acknowledged</th><th class=\"n\">Records per request</th>\
         <th class=\"n\">Payload share of wire bytes</th><th class=\"n\">Records per batch</th>\
         <th class=\"n\">Retries</th><th class=\"n\">Timeouts</th></tr></thead><tbody>"
    );
    for entry in economics {
        let totals = &entry.totals;
        let _ = writeln!(
            out,
            "<tr><td>{}</td><td class=\"n\">{}</td><td class=\"n\">{}</td>\
             <td class=\"n\">{}</td><td class=\"n\">{}</td><td class=\"n\">{}</td>\
             <td class=\"n\">{}</td><td class=\"n\">{}</td></tr>",
            escape(&entry.subject),
            optional_count(totals.produce_requests),
            optional_rate(totals.produce_requests_per_million_acknowledged),
            optional_ratio(totals.records_per_produce_request),
            optional_ratio(totals.payload_bytes_per_transmitted_byte),
            optional_ratio(totals.batch_records.and_then(|window| window.mean())),
            optional_count(totals.retries),
            optional_count(totals.timeouts)
        );
    }
    let _ = writeln!(out, "</tbody></table>");
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

/// Width of a ratio bar, in user units.
const BAR_WIDTH: f64 = 200.0;

/// Height of a ratio bar, in user units.
const BAR_HEIGHT: f64 = 24.0;

/// Lowest ratio a bar can show; anything smaller is clamped to the left edge.
const BAR_LOW: f64 = 0.5;

/// Highest ratio a bar can show; anything larger is clamped to the right edge.
const BAR_HIGH: f64 = 1.5;

/// An inline SVG bar showing one interval against parity and the threshold.
///
/// The domain is fixed at [`BAR_LOW`, `BAR_HIGH`] so that bars in different
/// rows are directly comparable; an interval that runs off the end is clamped
/// and the printed numbers beside it remain the authority.
fn ratio_bar(pair: &PairedRatio, metric: SuiteMetric, threshold: f64) -> String {
    let scale = |value: f64| {
        let clamped = value.clamp(BAR_LOW, BAR_HIGH);
        (clamped - BAR_LOW) / (BAR_HIGH - BAR_LOW) * BAR_WIDTH
    };
    let low = scale(pair.ci_low);
    let high = scale(pair.ci_high);
    let width = (high - low).max(1.0);
    let class = if pair_passes(pair, metric, threshold) {
        "good"
    } else if pair_regresses(pair, metric, threshold) {
        "bad"
    } else {
        "flat"
    };
    format!(
        "<svg class=\"bar\" viewBox=\"0 0 {BAR_WIDTH:.0} {BAR_HEIGHT:.0}\" \
         width=\"{BAR_WIDTH:.0}\" height=\"{BAR_HEIGHT:.0}\" role=\"img\" \
         aria-label=\"interval {} to {}\">\
         <line class=\"axis\" x1=\"0\" y1=\"{axis:.1}\" x2=\"{BAR_WIDTH:.0}\" y2=\"{axis:.1}\"/>\
         <line class=\"tick\" x1=\"{lower_tick:.1}\" y1=\"2\" x2=\"{lower_tick:.1}\" \
         y2=\"{BAR_HEIGHT:.0}\"/>\
         <line class=\"tick\" x1=\"{upper_tick:.1}\" y1=\"2\" x2=\"{upper_tick:.1}\" \
         y2=\"{BAR_HEIGHT:.0}\"/>\
         <line class=\"parity\" x1=\"{parity:.1}\" y1=\"0\" x2=\"{parity:.1}\" \
         y2=\"{BAR_HEIGHT:.0}\"/>\
         <rect class=\"interval {class}\" x=\"{low:.1}\" y=\"{top:.1}\" width=\"{width:.1}\" \
         height=\"8\" rx=\"2\"/>\
         <circle class=\"point {class}\" cx=\"{point:.1}\" cy=\"{axis:.1}\" r=\"3\"/>\
         </svg>",
        format_ratio(pair.ci_low),
        format_ratio(pair.ci_high),
        axis = BAR_HEIGHT / 2.0,
        lower_tick = scale(1.0 - threshold),
        upper_tick = scale(1.0 + threshold),
        parity = scale(1.0),
        top = BAR_HEIGHT / 2.0 - 4.0,
        point = scale(pair.ratio_of_medians),
    )
}

/// The page's only stylesheet, inlined.
const STYLE: &str = "\
:root{color-scheme:light dark;--ink:#16191d;--muted:#5b6470;--rule:#d6dae0;--bg:#ffffff;\
--panel:#f5f7f9;--good:#1a7f4b;--bad:#b3261e;--flat:#6b7280;}\
@media (prefers-color-scheme:dark){:root{--ink:#e6e9ee;--muted:#9aa4b2;--rule:#333a44;\
--bg:#14171c;--panel:#1c2028;--good:#4ade80;--bad:#f87171;--flat:#9aa4b2;}}\
body{margin:0 auto;padding:2rem 1.25rem;max-width:60rem;background:var(--bg);color:var(--ink);\
font:16px/1.55 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;}\
h1{font-size:1.6rem;margin:0 0 .5rem;}h2{font-size:1.15rem;margin:2rem 0 .5rem;}\
p{margin:.5rem 0;}code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:.9em;}\
.banner{border-left:4px solid var(--bad);background:var(--panel);padding:.6rem .8rem;\
color:var(--muted);}\
.facts{display:grid;grid-template-columns:max-content 1fr;gap:.15rem .75rem;margin:1rem 0;}\
.facts dt{color:var(--muted);}.facts dd{margin:0;}\
table{border-collapse:collapse;width:100%;margin:.5rem 0;font-size:.92rem;}\
th,td{border-bottom:1px solid var(--rule);padding:.35rem .5rem;text-align:left;\
vertical-align:middle;}\
th{color:var(--muted);font-weight:600;}\
th.n,td.n{text-align:right;font-variant-numeric:tabular-nums;}\
td.pass{color:var(--good);}td.fail{color:var(--bad);}\
ul{margin:.5rem 0;padding-left:1.2rem;}li{margin:.2rem 0;}\
.bar{display:block;}\
.bar .axis{stroke:var(--rule);stroke-width:1;}\
.bar .tick{stroke:var(--muted);stroke-width:1;stroke-dasharray:2 2;}\
.bar .parity{stroke:var(--ink);stroke-width:1;}\
.bar .interval.good{fill:var(--good);}.bar .interval.bad{fill:var(--bad);}\
.bar .interval.flat{fill:var(--flat);}\
.bar .point.good{fill:var(--good);}.bar .point.bad{fill:var(--bad);}\
.bar .point.flat{fill:var(--flat);}\
";

/// How many attempts a summary counts as valid.
fn valid_count(summary: &SuiteSummary) -> usize {
    summary
        .attempts
        .iter()
        .filter(|attempt| attempt.run_valid)
        .count()
}

/// The word for a boolean gate or validity outcome.
fn pass_word(passed: bool) -> &'static str {
    if passed { "pass" } else { "fail" }
}

/// The word describing which direction is better for a metric.
fn direction_word(metric: SuiteMetric) -> &'static str {
    if metric.higher_is_better() {
        "larger is better"
    } else {
        "smaller is better"
    }
}

/// What a pair's interval says about the threshold, in one word.
fn verdict_word(pair: &PairedRatio, metric: SuiteMetric, threshold: f64) -> &'static str {
    if pair_passes(pair, metric, threshold) {
        "yes"
    } else if pair_regresses(pair, metric, threshold) {
        "no, worse"
    } else {
        "unresolved"
    }
}

/// Escapes the five characters that can end an HTML context.
fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            other => out.push(other),
        }
    }
    out
}

/// Formats a ratio or a dimensionless fraction.
fn format_ratio(value: f64) -> String {
    format!("{value:.4}")
}

/// Formats a rate.
fn format_rate(value: f64) -> String {
    format!("{value:.1}")
}

/// Formats a duration in nanoseconds as milliseconds.
fn format_ms(nanoseconds: u64) -> String {
    #[expect(
        clippy::cast_precision_loss,
        reason = "presentation of a duration, not identity arithmetic"
    )]
    let milliseconds = nanoseconds as f64 / 1_000_000.0;
    format!("{milliseconds:.3}")
}

/// Formats a duration in seconds.
fn format_seconds(value: f64) -> String {
    format!("{value:.3}")
}

/// Formats a byte count as mebibytes.
fn format_mib(bytes: u64) -> String {
    #[expect(
        clippy::cast_precision_loss,
        reason = "presentation of a byte count, not identity arithmetic"
    )]
    let mebibytes = bytes as f64 / (1024.0 * 1024.0);
    format!("{mebibytes:.1}")
}

/// Converts nanoseconds to seconds for presentation.
#[expect(
    clippy::cast_precision_loss,
    reason = "presentation of a duration, not identity arithmetic"
)]
fn nanos_to_seconds(nanoseconds: u64) -> f64 {
    nanoseconds as f64 / 1_000_000_000.0
}

/// Formats an optional duration, absent as [`NOT_REPORTED`].
fn optional_ms(nanoseconds: Option<u64>) -> String {
    nanoseconds.map_or_else(|| NOT_REPORTED.to_owned(), format_ms)
}

/// Formats an optional count, absent as [`NOT_REPORTED`].
fn optional_count(count: Option<u64>) -> String {
    count.map_or_else(|| NOT_REPORTED.to_owned(), |value| value.to_string())
}

/// Formats an optional ratio, absent as [`NOT_REPORTED`].
fn optional_ratio(value: Option<f64>) -> String {
    value.map_or_else(|| NOT_REPORTED.to_owned(), format_ratio)
}

/// Formats an optional rate, absent as [`NOT_REPORTED`].
fn optional_rate(value: Option<f64>) -> String {
    value.map_or_else(|| NOT_REPORTED.to_owned(), format_rate)
}
