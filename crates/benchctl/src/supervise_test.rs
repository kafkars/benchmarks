//! Supervision against `/bin/sh` fixtures: clean exits, non-zero exits, signal
//! deaths, deadline expiry, and interrupt-driven kills.
//!
//! `/bin/sh` is the fixture rather than a Rust helper binary because these
//! tests are about the supervisor's handling of the operating system, and a
//! shell can produce every ending — including dying on a signal it sends
//! itself — in one line.
#![expect(
    clippy::unwrap_used,
    reason = "a supervision fixture that misbehaves must fail the test immediately"
)]

use std::time::{Duration, SystemTime};

use crate::attempt::AttemptId;
use crate::interrupt::InterruptFlag;
use crate::supervise::{StdioTarget, ToolSpec, run};

fn shell(script: &str, deadline: Duration) -> ToolSpec {
    ToolSpec::new(
        vec!["/bin/sh".to_owned(), "-c".to_owned(), script.to_owned()],
        deadline,
    )
}

fn scratch_file(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "benchctl-supervise-test-{}-{label}-{}",
        std::process::id(),
        AttemptId::generate(SystemTime::now()).as_str(),
    ))
}

#[test]
fn a_clean_exit_is_reported_as_code_zero() {
    let outcome = run(
        &shell("exit 0", Duration::from_secs(30)),
        &InterruptFlag::unarmed(),
    )
    .unwrap();
    assert_eq!(outcome.exit.exit_code, Some(0));
    assert_eq!(outcome.exit.signal, None);
    assert!(!outcome.exit.timed_out);
    assert!(!outcome.interrupted);
    assert!(outcome.succeeded());
    assert!(outcome.pid > 0);
}

#[test]
fn a_non_zero_exit_keeps_its_code() {
    let outcome = run(
        &shell("exit 3", Duration::from_secs(30)),
        &InterruptFlag::unarmed(),
    )
    .unwrap();
    assert_eq!(outcome.exit.exit_code, Some(3));
    assert!(!outcome.succeeded());
    assert_eq!(outcome.describe(), "exited with code 3");
}

#[test]
fn a_signal_death_is_captured_as_a_signal() {
    let outcome = run(
        &shell("kill -ABRT $$", Duration::from_secs(30)),
        &InterruptFlag::unarmed(),
    )
    .unwrap();
    assert_eq!(outcome.exit.exit_code, None, "a signal death has no code");
    assert_eq!(outcome.exit.signal, Some(6), "SIGABRT is signal six");
    assert!(!outcome.exit.timed_out);
    assert!(!outcome.interrupted);
    assert_eq!(outcome.describe(), "died on signal 6");
}

#[test]
fn a_child_past_its_deadline_is_killed_timed_out_and_reaped() {
    let outcome = run(
        &shell("sleep 30", Duration::from_millis(200)),
        &InterruptFlag::unarmed(),
    )
    .unwrap();
    assert!(outcome.exit.timed_out, "the deadline must be recorded");
    assert_eq!(outcome.exit.signal, Some(9), "the supervisor kills hard");
    assert!(
        outcome.exit.duration_ms < 30_000,
        "the supervisor must not have waited for the child"
    );
    assert!(!outcome.succeeded());
    // The reap is blocking, so by now the pid is gone rather than a zombie.
    let alive = std::process::Command::new("kill")
        .args(["-0", &outcome.pid.to_string()])
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(!alive.success(), "the killed child was not reaped");
}

#[test]
fn a_latched_interrupt_kills_the_child_without_calling_it_a_crash() {
    let interrupt = InterruptFlag::unarmed();
    interrupt.raise();
    let outcome = run(&shell("sleep 30", Duration::from_secs(30)), &interrupt).unwrap();
    assert!(outcome.interrupted);
    assert!(!outcome.exit.timed_out, "an interrupt is not a timeout");
    assert_eq!(outcome.exit.signal, Some(9));
    assert_eq!(outcome.describe(), "killed because the run was interrupted");
}

#[test]
fn output_streams_land_in_their_files() {
    let stdout = scratch_file("stdout");
    let stderr = scratch_file("stderr");
    let spec = shell("echo out; echo err >&2", Duration::from_secs(30))
        .with_stdout_file(stdout.clone())
        .with_stderr_file(stderr.clone());
    assert_eq!(spec.stdout, StdioTarget::File(stdout.clone()));
    let outcome = run(&spec, &InterruptFlag::unarmed()).unwrap();
    assert!(outcome.succeeded());
    assert_eq!(std::fs::read_to_string(&stdout).unwrap(), "out\n");
    assert_eq!(std::fs::read_to_string(&stderr).unwrap(), "err\n");
    std::fs::remove_file(&stdout).unwrap();
    std::fs::remove_file(&stderr).unwrap();
}

#[test]
fn an_empty_command_is_refused_rather_than_spawned() {
    let error = run(
        &ToolSpec::new(Vec::new(), Duration::from_secs(1)),
        &InterruptFlag::unarmed(),
    )
    .unwrap_err();
    assert_eq!(error.kind(), crate::error::CtlErrorKind::InvalidExperiment);
    assert_eq!(
        ToolSpec::new(Vec::new(), Duration::from_secs(1)).program(),
        "<empty command>"
    );
}

#[test]
fn a_missing_program_fails_to_spawn_rather_than_reporting_an_exit() {
    let spec = ToolSpec::new(
        vec![
            scratch_file("no-such-program").display().to_string(),
            "run".to_owned(),
        ],
        Duration::from_secs(1),
    );
    let error = run(&spec, &InterruptFlag::unarmed()).unwrap_err();
    assert!(error.message().starts_with("spawn "), "{error}");
}
