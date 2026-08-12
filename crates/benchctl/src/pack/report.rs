//! The closing table: one row per entry, in the order they ran.

use std::fmt::Write as _;

use super::command::EntryOutcome;
use super::manifest::PackManifest;
use super::plan::EntryPlan;

/// Renders the closing table: one row per entry, in the order they ran.
#[must_use]
pub fn render_table(manifest: &PackManifest, outcomes: &[EntryOutcome]) -> String {
    // Writing into a `String` cannot fail, so the results are discarded rather
    // than turning a report into a fallible operation.
    let mut text = String::new();
    let failed = outcomes
        .iter()
        .filter(|outcome| outcome.exit_code != 0)
        .count();
    let _ = writeln!(text, "\n## Pack {} ({})\n", manifest.name, manifest.cadence);
    let _ = writeln!(text, "| # | scenario | verb | repetitions | exit |");
    let _ = writeln!(text, "| ---: | --- | --- | ---: | ---: |");
    for (index, outcome) in outcomes.iter().enumerate() {
        let _ = writeln!(
            text,
            "| {} | `{}` | {} | {} | {} |",
            index + 1,
            outcome.scenario,
            outcome.plan.map_or("—", EntryPlan::as_str),
            outcome.repetitions,
            outcome.exit_code
        );
    }
    let _ = writeln!(
        text,
        "\n{} of {} entries exited 0.",
        outcomes.len() - failed,
        outcomes.len()
    );
    text
}
