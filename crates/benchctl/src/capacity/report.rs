//! Where a search's evidence lands, and the small report beside it.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use bench_schema::{CapacitySearch, CapacityStatus, pretty_bytes};

use crate::error::{CtlError, CtlResult};
use crate::suite::UNRESOLVED_REPORT_DIR;
use crate::time::utc_compact_seconds;

use super::ladder::SearchState;

/// Directory name suffix distinguishing a capacity search's reports.
pub const CAPACITY_REPORT_SUFFIX: &str = "capacity";

impl SearchState<'_> {
    /// Creates `<reports>/<experiment-short>/<utc-compact>-capacity/`.
    ///
    /// The directory is named by the *first* probe's experiment id, which is the
    /// scenario resolved at the search's initial rate. Every later probe has its
    /// own id, because a different offered rate is a different experiment; naming
    /// the tree after one of them would make the search's home depend on where it
    /// happened to stop.
    pub(super) fn report_directory(&self) -> CtlResult<PathBuf> {
        let short = self
            .first
            .as_ref()
            .and_then(|attempt| attempt.experiment_id.as_ref())
            .map_or_else(
                || UNRESOLVED_REPORT_DIR.to_owned(),
                |identity| identity.short().to_owned(),
            );
        let directory = self.command.reports_root.join(short).join(format!(
            "{}-{CAPACITY_REPORT_SUFFIX}",
            utc_compact_seconds(SystemTime::now())
        ));
        std::fs::create_dir_all(&directory).map_err(|error| {
            CtlError::internal(format!("create {}: {error}", directory.display()))
        })?;
        Ok(directory)
    }

    /// Writes `capacity-search.json` and the small report beside it.
    pub(super) fn write(&self, directory: &Path, status: CapacityStatus) -> CtlResult<()> {
        let subject = self
            .first
            .as_ref()
            .map_or_else(|| "unknown".to_owned(), |sealed| self.target(sealed));
        let document = CapacitySearch {
            schema: CapacitySearch::SCHEMA.to_owned(),
            subject,
            slo: self.slo,
            probes: self.probes.clone(),
            bracket_low: self.low,
            bracket_high: self.high,
            resolution: self.bounds.resolution(self.high),
            confirmed_rate: match status {
                CapacityStatus::Converged => self.low,
                CapacityStatus::Unconverged | CapacityStatus::Invalid => None,
            },
            confirmation: self.confirmation.clone(),
            status,
        };
        document.validate()?;
        for (name, bytes) in [
            ("capacity-search.json", pretty_bytes(&document)?),
            ("report.md", render(&document, &self.notes).into_bytes()),
        ] {
            let path = directory.join(name);
            std::fs::write(&path, &bytes).map_err(|error| {
                CtlError::internal(format!("write {}: {error}", path.display()))
            })?;
            println!("{}", path.display());
        }
        Ok(())
    }
}

/// Renders the small markdown report that sits beside the document.
///
/// Deliberately small: the ladder is the story, and the document beside it holds
/// every number this restates.
fn render(document: &CapacitySearch, notes: &[String]) -> String {
    // Writing into a `String` cannot fail, so the results are discarded rather
    // than turning a report into a fallible operation.
    let mut text = String::new();
    let _ = writeln!(text, "# Capacity search\n");
    let _ = writeln!(text, "- subject: `{}`", document.subject);
    let _ = writeln!(text, "- status: **{}**", document.status.as_str());
    let _ = writeln!(
        text,
        "- confirmed rate: {}",
        document
            .confirmed_rate
            .map_or_else(|| "none".to_owned(), |rate| format!("{rate} records/s"))
    );
    let _ = writeln!(
        text,
        "- bracket: {} .. {} (resolution {} records/s)\n",
        rate_or_dash(document.bracket_low),
        rate_or_dash(document.bracket_high),
        document.resolution
    );
    let _ = writeln!(text, "## Probes\n");
    let _ = writeln!(
        text,
        "| offered records/s | satisfied | attempt | reasons |"
    );
    let _ = writeln!(text, "| ---: | :---: | --- | --- |");
    for probe in document.probes.iter().chain(&document.confirmation) {
        let _ = writeln!(
            text,
            "| {} | {} | `{}` | {} |",
            probe.offered_records_per_second,
            if probe.satisfied { "yes" } else { "no" },
            probe.attempt_id,
            if probe.reasons.is_empty() {
                "—".to_owned()
            } else {
                probe.reasons.join("; ")
            }
        );
    }
    if !notes.is_empty() {
        let _ = writeln!(text, "\n## Notes\n");
        for note in notes {
            let _ = writeln!(text, "- {note}");
        }
    }
    text
}

/// A rate, or a dash for a bracket end the search never found.
fn rate_or_dash(rate: Option<u64>) -> String {
    rate.map_or_else(|| "-".to_owned(), |value| value.to_string())
}
