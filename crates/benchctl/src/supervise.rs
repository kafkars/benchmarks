//! Child-process supervision without an async runtime: spawn with stdio
//! redirected to files, poll `try_wait` against monotonic deadlines, kill and
//! reap on expiry or interrupt, and capture exit codes and signals exactly.
//!
//! # Why files and not pipes
//!
//! A subject under measurement may write megabytes to stderr while the control
//! plane is asleep between polls. Reading that through a pipe means either a
//! reader thread — concurrency this crate does not want — or a filled pipe
//! buffer that blocks the child mid-run and silently changes the thing being
//! measured. Redirecting to a file makes the child's writes the operating
//! system's problem, and the bytes land in the bundle where a reader can see
//! them. Pipes are reserved for the small probe captures in `probe`.
//!
//! # Why polling and not `wait`
//!
//! A blocking `wait` cannot notice a deadline or an interrupt. Polling
//! [`try_wait`](std::process::Child::try_wait) every [`POLL_INTERVAL`] costs a
//! syscall per subject per 25 ms — nothing next to a benchmark — and buys a
//! control plane that can always stop, always kill, and always seal.
//!
//! # Why the latch is read twice
//!
//! Ctrl-C at a terminal goes to the whole foreground process group, not to this
//! process alone. The child therefore dies of the same `SIGINT` that raised this
//! process's latch, and it usually dies first: the very next
//! [`try_wait`](std::process::Child::try_wait) reaps it and breaks the poll loop
//! before the loop ever looks at the latch. Reading the latch only inside the
//! loop makes that ordering look like a child that died on a signal nobody sent
//! — which is the definition of a crash — so a plain Ctrl-C would seal a
//! `crashed` bundle.
//!
//! The latch is consulted once more after the loop, whichever branch ended it.
//! A latch that is set means this process was asked to stop, and every ending
//! that coincides with it is part of stopping. A signal death with the latch
//! *unset* is still a crash, which is the distinction that matters: it separates
//! "the operator stopped the run" from "something killed the subject".
//!
//! # Why the blocking reap matters
//!
//! Killing a child does not mean it is gone; it means a signal was delivered.
//! Until the child is reaped it may still hold its output files open and still
//! be writing to them. Every kill in this module is therefore followed by a
//! blocking [`wait`](std::process::Child::wait) before the run returns, so that
//! by the time the caller reads the log files nothing else can be appending to
//! them, and so that no zombie survives the attempt.

use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use bench_schema::ProcessExit;

use crate::error::{CtlError, CtlResult};
use crate::interrupt::InterruptFlag;

/// How long the supervisor sleeps between `try_wait` polls.
pub const POLL_INTERVAL: Duration = Duration::from_millis(25);

/// The signal [`kill_and_reap`] sends, and therefore the only signal an ending
/// this module *caused* can report.
const SIGKILL: i32 = 9;

/// Where one of a child's output streams goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StdioTarget {
    /// Discard the stream.
    Null,
    /// Truncate or create this file and write the stream to it.
    File(PathBuf),
}

impl StdioTarget {
    /// Opens the target as a child's stdio slot.
    fn open(&self) -> CtlResult<Stdio> {
        match self {
            Self::Null => Ok(Stdio::null()),
            Self::File(path) => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).map_err(|error| {
                        CtlError::internal(format!("create {}: {error}", parent.display()))
                    })?;
                }
                let file = std::fs::File::create(path).map_err(|error| {
                    CtlError::internal(format!("create {}: {error}", path.display()))
                })?;
                Ok(Stdio::from(file))
            }
        }
    }
}

/// One supervised child: what to run, where its output goes, and how long it
/// may take.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolSpec {
    /// Argument vector, program first.
    pub argv: Vec<String>,
    /// Where standard output goes.
    pub stdout: StdioTarget,
    /// Where standard error goes.
    pub stderr: StdioTarget,
    /// How long the child may run before the supervisor kills it.
    pub deadline: Duration,
}

impl ToolSpec {
    /// Creates a spec that discards both streams.
    #[must_use]
    pub fn new(argv: Vec<String>, deadline: Duration) -> Self {
        Self {
            argv,
            stdout: StdioTarget::Null,
            stderr: StdioTarget::Null,
            deadline,
        }
    }

    /// Sends standard output to `path`.
    #[must_use]
    pub fn with_stdout_file(mut self, path: PathBuf) -> Self {
        self.stdout = StdioTarget::File(path);
        self
    }

    /// Sends standard error to `path`.
    #[must_use]
    pub fn with_stderr_file(mut self, path: PathBuf) -> Self {
        self.stderr = StdioTarget::File(path);
        self
    }

    /// The program the spec runs, for messages.
    #[must_use]
    pub fn program(&self) -> &str {
        self.argv.first().map_or("<empty command>", String::as_str)
    }
}

/// How a supervised child ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupervisedRun {
    /// The exit, in the vocabulary the run status records.
    pub exit: ProcessExit,
    /// Whether this ending is attributable to an interrupt: either the
    /// supervisor killed the child because the latch was raised, or the child
    /// ended while it was raised. Both are the operator stopping the run, and
    /// distinguishing them from a signal nobody asked for is what keeps a
    /// Ctrl-C from being reported as a crash.
    pub interrupted: bool,
    /// The child's process id, kept for evidence and for tests that need to
    /// signal it from outside.
    pub pid: u32,
}

impl SupervisedRun {
    /// Reports whether the child exited zero on its own.
    #[must_use]
    pub fn succeeded(self) -> bool {
        self.exit.exit_code == Some(0) && self.exit.signal.is_none() && !self.exit.timed_out
    }

    /// Describes the ending in one line, for a phase record.
    #[must_use]
    pub fn describe(self) -> String {
        if self.exit.timed_out {
            return format!(
                "killed after exceeding its deadline ({} ms)",
                self.exit.duration_ms
            );
        }
        if self.interrupted {
            // The supervisor's own kill is always `SIGKILL`. Any other signal
            // means the child took the interrupt directly, which is worth
            // saying: the phase record should not claim a kill nobody made.
            return match self.exit.signal {
                Some(signal) if signal != SIGKILL => {
                    format!("died on signal {signal} while the run was interrupted")
                }
                _ => "killed because the run was interrupted".to_owned(),
            };
        }
        match (self.exit.exit_code, self.exit.signal) {
            (Some(code), _) => format!("exited with code {code}"),
            (None, Some(signal)) => format!("died on signal {signal}"),
            (None, None) => "ended without an exit code or a signal".to_owned(),
        }
    }
}

/// Runs `spec` to completion, killing and reaping it if its deadline expires or
/// `interrupt` is raised.
///
/// # Errors
///
/// Returns an error only when the child could not be spawned or could not be
/// waited on — that is, when there is no exit to report. Every way a child can
/// end, including badly, is a successful supervision with a [`SupervisedRun`]
/// that says so.
pub fn run(spec: &ToolSpec, interrupt: &InterruptFlag) -> CtlResult<SupervisedRun> {
    let (program, arguments) = spec
        .argv
        .split_first()
        .ok_or_else(|| CtlError::invalid("a tool command must name the program to run"))?;
    let mut command = Command::new(program);
    command
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(spec.stdout.open()?)
        .stderr(spec.stderr.open()?);
    let started = Instant::now();
    let mut child = command
        .spawn()
        .map_err(|error| CtlError::internal(format!("spawn {program}: {error}")))?;
    let pid = child.id();
    let mut timed_out = false;
    let mut interrupted = false;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {}
            Err(error) => {
                return Err(CtlError::internal(format!("wait for {program}: {error}")));
            }
        }
        if started.elapsed() >= spec.deadline {
            timed_out = true;
            break kill_and_reap(&mut child, program)?;
        }
        if interrupt.is_set() {
            interrupted = true;
            break kill_and_reap(&mut child, program)?;
        }
        std::thread::sleep(POLL_INTERVAL);
    };
    let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    // The second read of the latch, for the ordering the loop cannot see: the
    // child died of the interrupt the terminal sent the whole process group, and
    // `try_wait` reaped it before the loop got as far as the latch check above.
    // Without this the ending is indistinguishable from a signal nobody sent.
    let interrupted = interrupted || interrupt.is_set();
    Ok(SupervisedRun {
        exit: ProcessExit {
            exit_code: status.code(),
            signal: signal_of(status),
            timed_out,
            duration_ms,
        },
        interrupted,
        pid,
    })
}

/// Kills a child and blocks until it has been reaped.
fn kill_and_reap(child: &mut Child, program: &str) -> CtlResult<ExitStatus> {
    // A kill failure here means the child is already dead but unreaped, which
    // the wait below handles; there is nothing else to do about it and nothing
    // useful to say.
    drop(child.kill());
    child
        .wait()
        .map_err(|error| CtlError::internal(format!("reap {program}: {error}")))
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
