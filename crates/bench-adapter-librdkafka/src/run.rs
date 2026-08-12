//! Driving the C binary: translate, spawn, capture, and always leave a status
//! document behind.
//!
//! # Where the output goes
//!
//! Under `--v2-output` the C program writes `result.json` and
//! `client-metrics.jsonl` into the output directory itself, so this shim never
//! touches the result document: it does not create it, redirect into it, or
//! re-serialize it. A measurement that is never re-rendered can never be
//! re-rounded, and a shim that does not open the file cannot truncate a
//! document the child is still writing. Both of the child's streams are
//! inherited, so the control plane's own capture is the single place a
//! subject's diagnostics land.
//!
//! # No timeout here
//!
//! The child is waited on with a blocking wait and no deadline. Deadlines
//! belong to the control plane, which owns the budget, can kill a process tree,
//! and has somewhere to record that it did. An adapter enforcing its own
//! timeout would race the supervisor and produce two different accounts of the
//! same run.
//!
//! # The status document is the point
//!
//! Every path out of [`execute`] writes `<output>/status.json` first, including
//! the paths where the experiment could not be read, the argument vector could
//! not be built, or the binary could not be spawned. A missing status document
//! therefore means something killed this process — which is exactly the
//! distinction the control plane needs and cannot make for itself.
//!
//! The same distinction applies one level down. A child that dies on a signal
//! has no exit code at all, and reporting it as a plain failure would file an
//! out-of-memory kill, a segmentation fault, and a supervisor's own `SIGKILL`
//! under the same sentence as a benchmark that ran and disagreed with itself.
//! The signal is named instead.

use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};
use std::time::SystemTime;

use bench_schema::{AdapterStatus, ResolvedExperiment, pretty_bytes};

use crate::arguments::{EXIT_FAILURE, EXIT_OK};
use crate::translate::{self, STATUS_FILE};
use crate::{time, validate};

/// Runs one experiment through the C binary and returns the process exit code.
pub(crate) fn execute(binary: &Path, experiment: &Path, output: &Path) -> i32 {
    let started_at = time::utc_rfc3339_millis(SystemTime::now());
    if let Err(error) = std::fs::create_dir_all(output) {
        // Without the output directory there is nowhere to write a status, so
        // this is the one failure that can only be reported on stderr.
        eprintln!(
            "bench-adapter-librdkafka: create {}: {error}",
            output.display()
        );
        return EXIT_FAILURE;
    }
    match attempt(binary, experiment, output) {
        Ok(exit) => {
            let status = if exit == EXIT_OK {
                AdapterStatus::succeeded(started_at, time::now())
            } else {
                AdapterStatus::failed(
                    "run",
                    format!("the librdkafka benchmark exited with {exit}"),
                    started_at,
                    time::now(),
                )
            };
            write_status(output, &status);
            exit
        }
        Err(failure) => {
            eprintln!("bench-adapter-librdkafka: {}", failure.reason);
            write_status(
                output,
                &AdapterStatus::failed(failure.stage, failure.reason, started_at, time::now()),
            );
            EXIT_FAILURE
        }
    }
}

/// A failure before the child could report for itself.
struct Failure {
    stage: &'static str,
    reason: String,
}

impl Failure {
    fn new(stage: &'static str, reason: impl Into<String>) -> Self {
        Self {
            stage,
            reason: reason.into(),
        }
    }
}

/// Everything that can fail before the child's own exit code becomes the
/// answer.
fn attempt(binary: &Path, experiment: &Path, output: &Path) -> Result<i32, Failure> {
    let document = read_experiment(experiment)?;
    let subject = translate::subject_of(&document, Some(output))
        .map_err(|reason| Failure::new("resolve-subject", reason))?
        .name
        .clone();
    let report = validate::report(&document, Some(&subject));
    if !report.supported {
        return Err(Failure::new(
            "validate",
            format!(
                "this adapter declined the experiment: {}",
                report.reasons.join("; ")
            ),
        ));
    }
    let arguments = translate::arguments(&document, &subject, output)
        .map_err(|reason| Failure::new("translate", reason))?;
    let mut child = Command::new(binary)
        .args(&arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|error| Failure::new("spawn", format!("spawn {}: {error}", binary.display())))?;
    let status = child
        .wait()
        .map_err(|error| Failure::new("wait", format!("wait for the C benchmark: {error}")))?;
    if let Some(signal) = signal_of(status) {
        return Err(Failure::new(
            "run",
            format!("the librdkafka benchmark died on signal {signal}"),
        ));
    }
    // A status with neither a code nor a signal cannot be produced by a
    // `wait` on this platform; if one ever is, it is a failure rather than a
    // success, which is the only safe reading of "the child ended somehow".
    Ok(status.code().unwrap_or(EXIT_FAILURE))
}

/// Returns the signal a child died on, when the platform reports one.
#[cfg(unix)]
fn signal_of(status: ExitStatus) -> Option<i32> {
    std::os::unix::process::ExitStatusExt::signal(&status)
}

/// Returns no signal: this platform does not have them.
#[cfg(not(unix))]
fn signal_of(_status: ExitStatus) -> Option<i32> {
    None
}

/// Reads and parses the resolved experiment.
fn read_experiment(path: &Path) -> Result<ResolvedExperiment, Failure> {
    let bytes = std::fs::read(path).map_err(|error| {
        Failure::new(
            "read-experiment",
            format!("read {}: {error}", path.display()),
        )
    })?;
    bench_schema::parse_json_slice(&bytes).map_err(|error| {
        Failure::new(
            "read-experiment",
            format!("{} is not a resolved experiment: {error}", path.display()),
        )
    })
}

/// Writes the adapter status, reporting on stderr if even that fails.
fn write_status(output: &Path, status: &AdapterStatus) {
    let path = output.join(STATUS_FILE);
    let bytes = match pretty_bytes(status) {
        Ok(bytes) => bytes,
        Err(error) => {
            eprintln!("bench-adapter-librdkafka: render status: {error}");
            return;
        }
    };
    if let Err(error) = std::fs::write(&path, bytes) {
        eprintln!(
            "bench-adapter-librdkafka: write {}: {error}",
            path.display()
        );
    }
}
