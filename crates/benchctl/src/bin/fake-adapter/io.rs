//! Reading the resolved experiment, writing the fixture's documents, and the
//! two-line argument reader the verbs share.

use std::path::Path;

use bench_schema::{AdapterStatus, ResolvedExperiment};

/// Reads and validates the resolved experiment.
pub(crate) fn read_experiment(path: &Path) -> Result<ResolvedExperiment, String> {
    let bytes = std::fs::read(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let experiment: ResolvedExperiment = bench_schema::parse_json_slice(&bytes)
        .map_err(|error| format!("parse {}: {error}", path.display()))?;
    experiment
        .validate()
        .map_err(|error| format!("validate {}: {error}", path.display()))?;
    Ok(experiment)
}

/// The subject name, taken from the output directory's last component.
pub(crate) fn subject_name(output: &Path) -> String {
    output
        .file_name()
        .map_or_else(String::new, |name| name.to_string_lossy().into_owned())
}

/// Writes an adapter status document, reporting but not failing on error.
pub(crate) fn write_status(output: &Path, status: &AdapterStatus) {
    let bytes = render(bench_schema::pretty_bytes(status));
    if let Err(error) = std::fs::write(output.join("status.json"), bytes.as_bytes()) {
        eprintln!("fake-adapter: could not write status.json: {error}");
    }
}

/// Turns rendered document bytes into text, substituting an empty document if
/// the render somehow failed.
///
/// The rendering is passed in rather than performed here because this binary
/// does not depend on `serde` directly.
pub(crate) fn render(rendered: bench_schema::SchemaResult<Vec<u8>>) -> String {
    rendered.map_or_else(
        |error| {
            eprintln!("fake-adapter: could not render a document: {error}");
            "{}\n".to_owned()
        },
        |bytes| String::from_utf8_lossy(&bytes).into_owned(),
    )
}

/// Prints a document to standard output.
pub(crate) fn print_document(rendered: bench_schema::SchemaResult<Vec<u8>>) -> i32 {
    print!("{}", render(rendered));
    0
}

/// Returns the value of a `--flag value` pair.
pub(crate) fn flag(arguments: &[String], name: &str) -> Option<String> {
    arguments
        .iter()
        .position(|argument| argument == name)
        .and_then(|index| arguments.get(index + 1))
        .cloned()
}
