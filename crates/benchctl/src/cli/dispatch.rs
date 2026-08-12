//! Verb to implementation, and the process exit code that comes back.

use std::io::Write;
use std::time::SystemTime;

use bench_schema::{BudgetSpec, pretty_bytes};

use crate::attempt::AttemptId;
use crate::error::{CtlError, CtlResult, EXIT_SEALED_COMPLETE};
use crate::pipeline::{self, AttemptEnd};
use crate::{capacity, pack, report, suite};

use super::command::{Command, ResolveCommand, RunCommand};
use super::parse::parse;
use super::usage::USAGE;

/// Runs `benchctl` and returns the process exit code.
///
/// `argv` is the full argument vector including the program name, so that
/// `main` can hand over `std::env::args()` unchanged.
#[must_use]
pub fn run(argv: &[String]) -> i32 {
    match parse(argv.get(1..).unwrap_or_default()) {
        Ok(Command::Help) => {
            println!("{USAGE}");
            EXIT_SEALED_COMPLETE
        }
        Ok(Command::Resolve(command)) => match execute_resolve(&command) {
            Ok(()) => EXIT_SEALED_COMPLETE,
            Err(error) => report_error(&error),
        },
        Ok(Command::Run(command)) => execute_run(&command),
        Ok(Command::Suite(command)) => suite::execute(&command),
        Ok(Command::Capacity(command)) => capacity::execute(&command),
        Ok(Command::Pack(command)) => pack::execute(&command),
        Ok(Command::Report(command)) => report::execute_report(&command),
        Ok(Command::Packet(command)) => report::execute_packet(&command),
        Err(error) => {
            eprintln!("{USAGE}");
            report_error(&error)
        }
    }
}

fn report_error(error: &CtlError) -> i32 {
    eprintln!("benchctl: {error}");
    error.exit_code()
}

fn execute_resolve(command: &ResolveCommand) -> CtlResult<()> {
    let inputs = pipeline::load(&command.common)?;
    let id = AttemptId::generate(SystemTime::now());
    let scratch = pipeline::scratch_directory(&id)?;
    let resolved = pipeline::probe_and_resolve(
        &inputs,
        &command.common,
        BudgetSpec::default(),
        &id,
        &scratch,
    )
    .map(|(resolved, _lock)| resolved);
    let _ = std::fs::remove_dir_all(&scratch);
    let resolved = resolved?;
    let bytes = pretty_bytes(&resolved)?;
    // The id goes to stderr whether or not `--out` was given. Without `--out`
    // the document is on stdout for a pipeline to consume, and a second thing
    // printed there would break it; with `--out` a reader still asked "under
    // what identity would this run", and the id is the answer to that question
    // rather than a decoration on the answer to another one.
    eprintln!("experiment_id: {}", bench_schema::experiment_id(&resolved)?);
    match &command.out {
        Some(path) => std::fs::write(path, bytes)
            .map_err(|error| CtlError::internal(format!("write {}: {error}", path.display()))),
        None => std::io::stdout()
            .write_all(&bytes)
            .map_err(|error| CtlError::internal(format!("write stdout: {error}"))),
    }
}

fn execute_run(command: &RunCommand) -> i32 {
    let inputs = match pipeline::load(&command.common) {
        Ok(inputs) => inputs,
        Err(error) => return report_error(&error),
    };
    match pipeline::attempt(
        &inputs,
        &command.common,
        &command.results_root,
        command.budget,
    ) {
        AttemptEnd::Sealed(sealed) => {
            // The last line of stdout is the bundle path, on every exit code a
            // sealed attempt can produce. A crashed attempt is exactly when a
            // reader most wants the directory, and a verb that made them go
            // looking for it with `ls -t` was asking them to guess which of
            // several trees the run actually landed in.
            println!("{}", sealed.paths.root().display());
            sealed.exit_code
        }
        AttemptEnd::Unsealable(error) => error.exit_code(),
    }
}
