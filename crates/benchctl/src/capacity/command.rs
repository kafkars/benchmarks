//! `benchctl capacity`, end to end: read the scenario, refuse an unsearchable
//! one, walk the ladder, and write the two documents.

use std::path::PathBuf;

use bench_schema::{BudgetSpec, CapacityStatus};

use crate::error::{CtlError, CtlResult, EXIT_SEALED_COMPLETE, EXIT_SEALED_PARTIAL};
use crate::pipeline::{self, CommonArguments};

use super::bounds::SearchBounds;
use super::ladder::SearchState;

/// `benchctl capacity`: search for the highest satisfying offered rate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapacityCommand {
    /// Shared inputs.
    pub common: CommonArguments,
    /// Root of the evidence tree.
    pub results_root: PathBuf,
    /// Ceilings each probe declares for itself.
    pub budget: BudgetSpec,
    /// Root of the report tree.
    pub reports_root: PathBuf,
}

/// Runs the search and returns the process exit code.
#[must_use]
pub fn execute(command: &CapacityCommand) -> i32 {
    match search(command) {
        Ok(status) => match status {
            CapacityStatus::Converged => EXIT_SEALED_COMPLETE,
            CapacityStatus::Unconverged | CapacityStatus::Invalid => EXIT_SEALED_PARTIAL,
        },
        Err(error) => {
            eprintln!("benchctl: {error}");
            error.exit_code()
        }
    }
}

/// The whole search, from reading the scenario to writing the two documents.
fn search(command: &CapacityCommand) -> CtlResult<CapacityStatus> {
    let inputs = pipeline::load(&command.common)?;
    let slo = crate::resolve::slo_spec(&inputs.source);
    if slo.is_empty() {
        return Err(CtlError::invalid(
            "a capacity search needs an [slo] section: with no objective declared, \
             every rate satisfies and the search would report a ceiling nobody measured",
        ));
    }
    let bounds = SearchBounds::from_source(&inputs.source)?;
    let mut state = SearchState::new(command, &inputs, slo, bounds);
    let status = state.run()?;
    let directory = state.report_directory()?;
    state.write(&directory, status)?;
    Ok(status)
}
