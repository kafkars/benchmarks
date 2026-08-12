//! `benchctl report` and `benchctl packet`: reading sealed evidence back out,
//! and the guardrail on prose written over it.
//!
//! Neither verb runs anything. `report` renders one sealed bundle as markdown,
//! which is the one-attempt view a person wants when a suite says something
//! surprising and they need to look at a specific run. `packet` is the
//! enforcement point between the deterministic layer and the interpretive one.
//!
//! # Why the packet verb only validates
//!
//! The analysis packet is derived from a suite summary by code, so there is
//! nothing to ask a person about: `benchctl suite` already writes it. What needs
//! a command is the other direction — a summary somebody (or some model) wrote
//! *over* that packet, which must be checked before it is believed. The check is
//! mechanical and total: the verdict must be copied rather than concluded, every
//! quantitative claim must cite a metric the packet defines, and every citation
//! must name provenance the packet carries.
//!
//! Exit `0` when the summary is bound to its packet, `65` when it is not, in the
//! same slot every other malformed input uses. Rejecting a summary costs
//! nothing: the packet is still there, and the numbers in it never depended on
//! the prose.

use std::io::Write;
use std::path::{Path, PathBuf};

use bench_schema::{LlmSummary, SuiteSummary};

use crate::attempt::AttemptPaths;
use crate::error::{CtlError, CtlResult, EXIT_SEALED_COMPLETE};

/// `benchctl report`: render one sealed bundle as markdown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportCommand {
    /// Root of the sealed bundle to render.
    pub bundle: PathBuf,
    /// Where the markdown goes; `None` means stdout.
    pub out: Option<PathBuf>,
}

/// `benchctl packet`: check an LLM summary against its analysis packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PacketCommand {
    /// Path to a sealed `suite-summary.json`.
    pub suite: PathBuf,
    /// Path to the `kafkars.llm-summary.v1` document to check.
    pub llm_summary: PathBuf,
}

/// Renders a sealed bundle and returns the process exit code.
#[must_use]
pub fn execute_report(command: &ReportCommand) -> i32 {
    match render(command) {
        Ok(()) => EXIT_SEALED_COMPLETE,
        Err(error) => {
            eprintln!("benchctl: {error}");
            error.exit_code()
        }
    }
}

/// Renders one bundle to its destination.
fn render(command: &ReportCommand) -> CtlResult<()> {
    let paths = AttemptPaths::at(command.bundle.clone());
    if !paths.status_json().is_file() {
        return Err(CtlError::invalid(format!(
            "{} does not look like a sealed bundle: no status.json",
            command.bundle.display()
        )));
    }
    let markdown = bench_report::render_markdown_bundle(paths.root())
        .map_err(|error| CtlError::invalid(format!("render the bundle: {error}")))?;
    write_out(command.out.as_deref(), markdown.as_bytes())
}

/// Writes rendered text to a file or to standard output.
///
/// The destination's parent is created first, as `suite` and `capacity` do for
/// their own report trees. `reports/` is not checked in — it is the output of
/// running things — so on a fresh clone the obvious `--out reports/run.md` names
/// a directory that does not exist yet, and refusing that would be refusing the
/// first command a reader tries.
fn write_out(destination: Option<&Path>, bytes: &[u8]) -> CtlResult<()> {
    match destination {
        Some(path) => {
            if let Some(parent) = path.parent()
                && !parent.as_os_str().is_empty()
            {
                std::fs::create_dir_all(parent).map_err(|error| {
                    CtlError::internal(format!("create {}: {error}", parent.display()))
                })?;
            }
            std::fs::write(path, bytes)
                .map_err(|error| CtlError::internal(format!("write {}: {error}", path.display())))
        }
        None => std::io::stdout()
            .write_all(bytes)
            .map_err(|error| CtlError::internal(format!("write stdout: {error}"))),
    }
}

/// Validates an LLM summary against its packet and returns the exit code.
#[must_use]
pub fn execute_packet(command: &PacketCommand) -> i32 {
    match validate(command) {
        Ok(()) => {
            println!(
                "{}: bound to the packet derived from {}",
                command.llm_summary.display(),
                command.suite.display()
            );
            EXIT_SEALED_COMPLETE
        }
        Err(error) => {
            eprintln!("benchctl: {error}");
            error.exit_code()
        }
    }
}

/// The check itself: build the packet, then bind the prose to it.
fn validate(command: &PacketCommand) -> CtlResult<()> {
    let summary = SuiteSummary::from_slice(&read(&command.suite)?)?;
    let packet = bench_report::build_packet(&summary);
    packet.validate()?;
    let written = LlmSummary::from_slice(&read(&command.llm_summary)?)?;
    written.validate_against(&packet)?;
    Ok(())
}

/// Reads a document's bytes, naming the file in the error.
fn read(path: &Path) -> CtlResult<Vec<u8>> {
    std::fs::read(path)
        .map_err(|error| CtlError::invalid(format!("read {}: {error}", path.display())))
}
