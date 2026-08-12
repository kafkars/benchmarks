//! The files this adapter reads and writes, and the one place each is named.
//!
//! A status document is the control plane's only way to tell "this run failed"
//! from "this process was killed", so the writing side lives here rather than
//! being spelled out at each exit.

use std::{
    error::Error,
    path::{Path, PathBuf},
};

use bench_schema::{AdapterStatus, ProducerBenchmarkV2, ResolvedExperiment, pretty_bytes};

/// Per-record latency evidence the legacy verbs write next to their result.
///
/// The protocol path writes no such file: run-sized per-record rows are
/// precisely the evidence v2's bounded histograms replace.
pub(crate) const LATENCY_FILE: &str = "latency.csv";

/// The measurement document the control plane seals.
pub(crate) const RESULT_FILE: &str = "result.json";

/// This adapter's own terminal status.
pub(crate) const STATUS_FILE: &str = "status.json";

/// Where the per-record latency evidence goes.
pub(crate) fn latency_path(output: &Path) -> PathBuf {
    output.join(LATENCY_FILE)
}

/// Writes the measurement as one JSON line, after it has checked itself.
///
/// The document validates before it is written rather than after it is read,
/// so a measurement whose own accounting does not add up never becomes a file
/// and can never be sealed into a bundle. A reader who finds a `result.json`
/// here is therefore reading something that already satisfied
/// [`ProducerBenchmarkV2::validate`] on the writing side.
pub(super) fn write_result(
    output: &Path,
    document: &ProducerBenchmarkV2,
) -> Result<(), Box<dyn Error>> {
    document.validate()?;
    let line = serde_json::to_string(document)?;
    std::fs::write(output.join(RESULT_FILE), format!("{line}\n"))?;
    Ok(())
}

/// Reads and parses a resolved experiment.
pub(super) fn read_experiment(path: &Path) -> Result<ResolvedExperiment, Box<dyn Error>> {
    let bytes = std::fs::read(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    Ok(bench_schema::parse_json_slice(&bytes)
        .map_err(|error| format!("{} is not a resolved experiment: {error}", path.display()))?)
}

/// Writes the adapter status document.
pub(super) fn write_status(output: &Path, status: &AdapterStatus) -> Result<(), Box<dyn Error>> {
    std::fs::write(output.join(STATUS_FILE), pretty_bytes(status)?)?;
    Ok(())
}

/// Prints a protocol document as pretty JSON on stdout.
pub(super) fn print_document<T: serde::Serialize>(document: &T) -> Result<(), Box<dyn Error>> {
    use std::io::Write;
    std::io::stdout().write_all(&pretty_bytes(document)?)?;
    Ok(())
}
