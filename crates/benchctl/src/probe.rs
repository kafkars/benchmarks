//! Adapter probes: spawning `<subject> describe --json` and `<subject>
//! validate --experiment <path>` with piped, size-capped output and a probe
//! deadline, parsing the capability and validation documents.
//!
//! # Why probing is hostile
//!
//! A subject is an arbitrary program named by a configuration file. It may
//! print nothing, print a gigabyte, block forever, or answer in a dialect of
//! JSON this repository does not speak. None of those may hang the control
//! plane or exhaust its memory, so every probe is bounded three ways: a wall
//! deadline enforced by polling rather than by a blocking read, a byte cap
//! applied by `Read::take` on both streams, and a strict parse that refuses
//! unknown fields.
//!
//! Both streams are drained by reader threads while the main thread polls
//! `Child::try_wait`. Reading one stream inline would deadlock the moment the
//! child filled the other pipe, and a blocking read on a hung child would
//! outlive the deadline it is supposed to enforce.
//!
//! # What a failed probe means
//!
//! Every failure here is [`CtlErrorKind::InvalidExperiment`]: the subject list
//! named something that cannot act as a subject. The captured head of the
//! child's stderr is folded into the message, because the one thing a person
//! debugging "the adapter did not answer" needs is what the adapter said before
//! it stopped.
//!
//! [`CtlErrorKind::InvalidExperiment`]: crate::CtlErrorKind::InvalidExperiment

use std::io::Read;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};

use bench_schema::{AdapterDescription, ValidateReport, require_schema};

use crate::error::{CtlError, CtlResult};

/// How often the supervising thread asks whether the probe has exited.
const POLL_INTERVAL: Duration = Duration::from_millis(25);

/// How long a reader thread is given to hand over its buffer after the child
/// has been reaped, before the capture gives up and reports what it has.
///
/// A grandchild that inherited the pipe can hold it open after its parent is
/// gone; waiting forever for that case would defeat the deadline this module
/// exists to enforce.
const READER_GRACE: Duration = Duration::from_secs(2);

/// Characters of captured output quoted back in an error message.
const OUTPUT_HEAD_CHARS: usize = 400;

/// Asks a subject what it can do: `<command...> describe --json`.
///
/// # Errors
///
/// Returns an invalid-experiment error when the program cannot be spawned,
/// answers after `timeout`, writes more than `capture_cap` bytes, exits
/// non-zero, or prints something that is not a `kafkars.adapter.v1` document.
pub fn describe(
    command_prefix: &[String],
    timeout: Duration,
    capture_cap: u64,
) -> CtlResult<AdapterDescription> {
    let capture = spawn_probe(
        command_prefix,
        &["describe".to_owned(), "--json".to_owned()],
        timeout,
        capture_cap,
    )?;
    let stdout = capture.checked_stdout()?;
    let description: AdapterDescription =
        bench_schema::parse_json_slice(stdout).map_err(|error| capture.unreadable(&error))?;
    require_schema(&description.schema, AdapterDescription::SCHEMA)
        .map_err(|error| capture.invalid(&error.to_string()))?;
    Ok(description)
}

/// Asks a subject whether it will run this experiment:
/// `<command...> validate --experiment <path>`.
///
/// # Errors
///
/// Returns an invalid-experiment error under the same conditions as
/// [`describe`]. A subject that answers `supported: false` is *not* an error
/// here — declining is a normal answer, and [`crate::resolve`] is where a
/// declination stops the attempt.
pub fn validate(
    command_prefix: &[String],
    experiment_path: &Path,
    timeout: Duration,
    capture_cap: u64,
) -> CtlResult<ValidateReport> {
    let arguments = vec![
        "validate".to_owned(),
        "--experiment".to_owned(),
        experiment_path.to_string_lossy().into_owned(),
    ];
    let capture = spawn_probe(command_prefix, &arguments, timeout, capture_cap)?;
    let stdout = capture.checked_stdout()?;
    let report: ValidateReport =
        bench_schema::parse_json_slice(stdout).map_err(|error| capture.unreadable(&error))?;
    require_schema(&report.schema, ValidateReport::SCHEMA)
        .map_err(|error| capture.invalid(&error.to_string()))?;
    Ok(report)
}

/// Everything one probe produced: its two capped streams and how it ended.
#[derive(Debug)]
struct Capture {
    /// The program that was asked, for error messages.
    program: String,
    /// The verb it was asked, for error messages.
    verb: String,
    /// Captured standard output, truncated to the cap.
    stdout: Vec<u8>,
    /// Captured standard error, truncated to the cap.
    stderr: Vec<u8>,
    /// How the child exited, or `None` when the deadline ended it.
    status: Option<ExitStatus>,
    /// Whether either stream exceeded the cap.
    oversize: bool,
}

impl Capture {
    /// Builds the invalid-experiment error for this probe, quoting the head of
    /// whatever the subject managed to say on stderr.
    fn invalid(&self, reason: &str) -> CtlError {
        let Self {
            program,
            verb: what,
            ..
        } = self;
        let stderr = output_head(&self.stderr);
        if stderr.is_empty() {
            CtlError::invalid(format!("subject {program:?} {what}: {reason}"))
        } else {
            CtlError::invalid(format!(
                "subject {program:?} {what}: {reason}; stderr: {stderr}"
            ))
        }
    }

    /// Returns this probe's stdout, after checking the child actually
    /// succeeded — a non-zero exit means whatever it printed is not an answer.
    fn checked_stdout(&self) -> CtlResult<&[u8]> {
        if let Some(status) = self.status
            && !status.success()
        {
            let ending = status
                .code()
                .map_or_else(|| "a signal".to_owned(), |code| format!("code {code}"));
            return Err(self.invalid(&format!("exited with {ending}")));
        }
        Ok(&self.stdout)
    }

    /// Builds the error for stdout that is not the document it should be.
    fn unreadable(&self, error: &bench_schema::SchemaError) -> CtlError {
        let head = output_head(&self.stdout);
        self.invalid(&format!(
            "printed no readable document ({error}); stdout: {head}"
        ))
    }
}

/// Runs one probe verb to completion, or to its deadline.
fn spawn_probe(
    command_prefix: &[String],
    arguments: &[String],
    timeout: Duration,
    capture_cap: u64,
) -> CtlResult<Capture> {
    let Some((program, leading)) = command_prefix.split_first() else {
        return Err(CtlError::invalid(
            "a subject command must name the program to run",
        ));
    };
    let mut command = Command::new(program);
    command
        .args(leading)
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().map_err(|error| {
        CtlError::invalid(format!("subject {program:?} could not be spawned: {error}"))
    })?;

    let stdout = child
        .stdout
        .take()
        .map(|pipe| read_capped(pipe, capture_cap));
    let stderr = child
        .stderr
        .take()
        .map(|pipe| read_capped(pipe, capture_cap));
    let status = await_exit(&mut child, timeout);
    let stdout = stdout.as_ref().map(collect).unwrap_or_default();
    let stderr = stderr.as_ref().map(collect).unwrap_or_default();

    let capture = Capture {
        program: program.clone(),
        verb: arguments
            .first()
            .cloned()
            .unwrap_or_else(|| "probe".to_owned()),
        oversize: over_cap(&stdout, capture_cap) || over_cap(&stderr, capture_cap),
        stdout: truncate_to(stdout, capture_cap),
        stderr: truncate_to(stderr, capture_cap),
        status,
    };

    if capture.status.is_none() {
        return Err(capture.invalid(&format!(
            "did not answer within {} milliseconds and was killed",
            timeout.as_millis()
        )));
    }
    if capture.oversize {
        return Err(capture.invalid(&format!(
            "wrote more than the {capture_cap}-byte capture limit"
        )));
    }
    Ok(capture)
}

/// Polls the child against a monotonic deadline, killing and reaping it on
/// expiry. Returns `None` when the deadline, rather than the child, ended it.
fn await_exit(child: &mut Child, timeout: Duration) -> Option<ExitStatus> {
    let deadline = Instant::now().checked_add(timeout);
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Some(status),
            Ok(None) => {}
            Err(_) => {
                kill_and_reap(child);
                return None;
            }
        }
        if deadline.is_none_or(|deadline| Instant::now() >= deadline) {
            kill_and_reap(child);
            return None;
        }
        thread::sleep(POLL_INTERVAL);
    }
}

/// Kills a child and blocks until it is reaped, so no zombie outlives a probe.
fn kill_and_reap(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// Drains a stream on its own thread, reading at most `cap + 1` bytes so that
/// the caller can tell "exactly at the limit" from "over the limit".
fn read_capped<R: Read + Send + 'static>(reader: R, cap: u64) -> Receiver<Vec<u8>> {
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let mut buffer = Vec::new();
        let mut bounded = reader.take(cap.saturating_add(1));
        let _ = bounded.read_to_end(&mut buffer);
        let _ = sender.send(buffer);
    });
    receiver
}

/// Takes a reader thread's buffer, giving up after [`READER_GRACE`].
fn collect(receiver: &Receiver<Vec<u8>>) -> Vec<u8> {
    receiver.recv_timeout(READER_GRACE).unwrap_or_default()
}

/// Reports whether a capped read came back longer than the cap allows.
fn over_cap(bytes: &[u8], cap: u64) -> bool {
    u64::try_from(bytes.len()).is_ok_and(|length| length > cap)
}

/// Trims a captured stream to the cap before it is quoted back.
fn truncate_to(mut bytes: Vec<u8>, cap: u64) -> Vec<u8> {
    if let Ok(cap) = usize::try_from(cap) {
        bytes.truncate(cap);
    }
    bytes
}

/// Renders the head of a captured stream as one quotable line.
fn output_head(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() > OUTPUT_HEAD_CHARS {
        let head: String = collapsed.chars().take(OUTPUT_HEAD_CHARS).collect();
        format!("{head}…")
    } else {
        collapsed
    }
}
