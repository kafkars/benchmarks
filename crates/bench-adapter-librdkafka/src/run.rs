//! Driving the C binary: translate, spawn, capture, and always leave a status
//! document behind.
//!
//! # Where the output goes
//!
//! The C program prints its result document on stdout and writes its two side
//! files to paths it is given. This shim redirects the child's stdout straight
//! into `<output>/result.json` rather than reading and re-emitting it: the
//! result carries floating-point measurements, and a document that is never
//! re-serialized can never be re-rounded. Stderr is inherited, so the control
//! plane's own capture is the single place a subject's diagnostics land.
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

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::SystemTime;

use bench_schema::{AdapterStatus, ResolvedExperiment, pretty_bytes};

use crate::arguments::{EXIT_FAILURE, EXIT_OK};
use crate::translate::{self, RESULT_FILE, STATUS_FILE};
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
    let result = std::fs::File::create(output.join(RESULT_FILE)).map_err(|error| {
        Failure::new(
            "spawn",
            format!("create {}: {error}", output.join(RESULT_FILE).display()),
        )
    })?;
    let mut child = Command::new(binary)
        .args(&arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::from(result))
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|error| Failure::new("spawn", format!("spawn {}: {error}", binary.display())))?;
    let status = child
        .wait()
        .map_err(|error| Failure::new("wait", format!("wait for the C benchmark: {error}")))?;
    Ok(status.code().unwrap_or(EXIT_FAILURE))
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
