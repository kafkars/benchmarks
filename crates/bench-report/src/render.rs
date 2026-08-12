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
//! # Honest by construction
//!
//! Every report opens with the same banner: this evidence is diagnostic and
//! never claim-eligible. Absent measurements print as [`NOT_REPORTED`], never
//! as `0`, and a subject whose client emits no native statistics is missing
//! from the economics table rather than present with a row of zeroes. Invalid
//! attempts are listed with the reason they were excluded, because the set a
//! median was taken over is part of the median.
//!
//! # Layout
//!
//! - `format` — the digit rules and words both renderings share.
//! - `markdown` — the Markdown suite report.
//! - `html` — the self-contained HTML suite report.
//! - `style` / `bar` — the HTML page's inlined stylesheet and interval bars.
//! - `economics` — the one table that is rendered in both, from a list the
//!   sealed summary has no field for.
//! - `bundle` — the single-attempt view over one sealed bundle.
//!
//! [`DIAGNOSTIC_BANNER`] stays declared in this file rather than moving down
//! into a child: `scripts/benchmark-openai-summary-test` reads it out of
//! `crates/bench-report/src/render.rs` by path, to prove the summary script's
//! copy has not drifted from this one.

mod bar;
mod bundle;
mod economics;
mod format;
mod html;
mod markdown;
mod style;

pub use bundle::render_markdown_bundle;
pub use html::render_html_suite;
pub use markdown::render_markdown_suite;

use crate::suite::SuiteReport;

/// The sentence every report opens with.
///
/// Stated in the rendering rather than left to the reader's memory: a report
/// that circulates without its limits attached will eventually be quoted
/// without them.
pub const DIAGNOSTIC_BANNER: &str = "Diagnostic only — never claim-eligible. These numbers describe the machine and \
     configuration they were measured on, and support no published comparison between clients.";

/// What a number that was not measured prints as.
pub const NOT_REPORTED: &str = "not reported";

impl SuiteReport {
    /// Renders this report as Markdown, including the request economics.
    #[must_use]
    pub fn markdown(&self) -> String {
        markdown::markdown_suite(&self.summary, &self.economics)
    }

    /// Renders this report as a self-contained HTML page, including the request
    /// economics.
    #[must_use]
    pub fn html(&self) -> String {
        html::html_suite(&self.summary, &self.economics)
    }
}
