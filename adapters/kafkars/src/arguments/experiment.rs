//! Flag parsing for the three adapter-protocol verbs.
//!
//! These take a resolved experiment document rather than a positional
//! workload, which is the whole difference between the two command surfaces:
//! the legacy commands carry the workload in their argv, and the protocol
//! verbs carry a path to the document that states it.

use std::{error::Error, path::PathBuf};

use super::parse::usage;

/// `validate --experiment <resolved.json>`.
pub(super) fn parse_experiment(values: &[String]) -> Result<PathBuf, Box<dyn Error>> {
    match values {
        [flag, path] if flag == "--experiment" && !path.is_empty() => Ok(PathBuf::from(path)),
        _ => Err(usage().into()),
    }
}

/// `run --experiment <resolved.json> --output <dir>`.
pub(super) fn parse_run(values: &[String]) -> Result<(PathBuf, PathBuf), Box<dyn Error>> {
    match values {
        [experiment_flag, experiment, output_flag, output]
            if experiment_flag == "--experiment"
                && output_flag == "--output"
                && !experiment.is_empty()
                && !output.is_empty() =>
        {
            Ok((PathBuf::from(experiment), PathBuf::from(output)))
        }
        _ => Err(usage().into()),
    }
}
