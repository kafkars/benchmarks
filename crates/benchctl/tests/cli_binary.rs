//! The binary-level checks the module tests cannot express: a whole
//! `benchctl run` through `main` sealing a complete bundle, and SIGINT sealing
//! a partial, interrupted one — both when it reaches only the control plane and
//! when it reaches the whole process group, which is what a terminal's Ctrl-C
//! actually does.
//!
//! Everything offline: the subjects and all three cluster tools are the
//! `fake-adapter` fixture, and the scenario is the migrated balanced-1k
//! diagnostic read from `scenarios/`, so this also proves the committed
//! scenario resolves through the real CLI path.
#![expect(
    clippy::unwrap_used,
    reason = "a fixture that cannot be built must fail the test immediately"
)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use bench_schema::ExecutionStatus;
use benchctl::AttemptPaths;

/// The control-plane binary under test.
const BENCHCTL: &str = env!("CARGO_BIN_EXE_benchctl");

/// Repository root, two levels above this crate's manifest.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

/// Writes the subjects and cluster fixtures for one test into `dir`,
/// with every subject running the fake adapter in `mode`.
fn write_fixtures(dir: &Path, mode: &str) -> (PathBuf, PathBuf) {
    let adapter = common::adapter(mode);
    let command_toml = |extra: Option<&str>| {
        let mut argv: Vec<String> = adapter.clone();
        if let Some(verb) = extra {
            argv.push(verb.to_owned());
        }
        let quoted: Vec<String> = argv.iter().map(|part| format!("{part:?}")).collect();
        format!("[{}]", quoted.join(", "))
    };
    let subjects_path = dir.join("subjects.toml");
    std::fs::write(
        &subjects_path,
        format!(
            "[[subjects]]\nname = \"kafkars\"\ncommand = {c}\n\n\
             [[subjects]]\nname = \"librdkafka-c\"\ncommand = {c}\n",
            c = command_toml(None),
        ),
    )
    .unwrap();
    let cluster_path = dir.join("cluster.toml");
    std::fs::write(
        &cluster_path,
        format!(
            "name = \"offline\"\n\
             bootstrap = \"127.0.0.1:39092,127.0.0.1:39093,127.0.0.1:39094\"\n\
             broker_version = \"4.3.1\"\nbrokers = 3\nsecurity = \"plaintext\"\n\
             lifecycle = \"externally managed by the caller\"\n\n\
             [tools]\ntopic_create = {create}\ntopic_delete = {delete}\nverify = {verify}\n",
            create = command_toml(Some("topics-create")),
            delete = command_toml(Some("topics-delete")),
            verify = command_toml(Some("verify")),
        ),
    )
    .unwrap();
    (subjects_path, cluster_path)
}

/// The argv for a `benchctl run` over the committed balanced-1k scenario.
fn run_arguments(subjects: &Path, cluster: &Path, results: &Path) -> Vec<String> {
    [
        "run",
        "--experiment",
        repo_root()
            .join("scenarios/producer/legacy-balanced-1k.toml")
            .to_str()
            .unwrap(),
        "--subjects",
        subjects.to_str().unwrap(),
        "--cluster",
        cluster.to_str().unwrap(),
        "--bootstrap",
        "127.0.0.1:39092,127.0.0.1:39093,127.0.0.1:39094",
        "--results",
        results.to_str().unwrap(),
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

/// Finds the single sealed bundle root under a results directory.
fn find_bundle(results: &Path) -> AttemptPaths {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut found = Vec::new();
        let mut stack = vec![results.to_path_buf()];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    if path.join("bundle.json").is_file() {
                        found.push(path);
                    } else {
                        stack.push(path);
                    }
                }
            }
        }
        if found.len() == 1 {
            return AttemptPaths::at(found.remove(0));
        }
        assert!(
            found.len() <= 1,
            "expected one sealed bundle, found {found:?}"
        );
        assert!(
            Instant::now() < deadline,
            "no sealed bundle appeared under {}",
            results.display()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// One scratch directory per test, removed on success.
fn scratch(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "benchctl-cli-binary-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn a_full_cli_run_seals_a_complete_valid_bundle() {
    let dir = scratch("complete");
    let (subjects, cluster) = write_fixtures(&dir, "ok");
    let results = dir.join("results");
    let output = Command::new(BENCHCTL)
        .args(run_arguments(&subjects, &cluster, &results))
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(0), "stderr:\n{stderr}");
    let paths = find_bundle(&results);
    let status = common::read_status(&paths);
    assert_eq!(status.execution_status, ExecutionStatus::Complete);
    assert!(!status.interrupted);
    assert!(common::read_classification(&paths).run_valid);
    common::assert_bundle_is_self_consistent(&paths);
    common::assert_system_checksum_check_passes(&paths);
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Blocks until an attempt workspace under `results` has sealed its inputs.
///
/// The resolved experiment is written immediately before subjects spawn, so its
/// presence is the signal that interrupting now interrupts the run phase rather
/// than the resolution.
fn wait_for_sealed_inputs(results: &Path) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut stack = vec![results.to_path_buf()];
        while let Some(entry) = stack.pop() {
            if entry.join("experiment.resolved.json").is_file() {
                return;
            }
            if let Ok(entries) = std::fs::read_dir(&entry) {
                stack.extend(entries.flatten().map(|e| e.path()).filter(|p| p.is_dir()));
            }
        }
        assert!(
            Instant::now() < deadline,
            "resolved experiment never sealed"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Waits for `child` to exit and returns its code.
fn wait_for_exit(child: &mut std::process::Child) -> Option<i32> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status.code();
        }
        assert!(Instant::now() < deadline, "benchctl ignored SIGINT");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Asserts the bundle under `results` is a partial, interrupted, self-consistent
/// one.
fn assert_interrupted_bundle(results: &Path) {
    let paths = find_bundle(results);
    let status = common::read_status(&paths);
    assert_eq!(
        status.execution_status,
        ExecutionStatus::Partial,
        "an interrupted attempt is partial, never crashed: {:?}",
        status.failure_reason
    );
    assert!(
        status.interrupted,
        "the sealed status must record the interrupt"
    );
    common::assert_bundle_is_self_consistent(&paths);
}

#[test]
fn sigint_mid_run_seals_a_partial_interrupted_bundle() {
    let dir = scratch("sigint");
    let (subjects, cluster) = write_fixtures(&dir, "run-hang");
    let results = dir.join("results");
    let mut child = Command::new(BENCHCTL)
        .args(run_arguments(&subjects, &cluster, &results))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    wait_for_sealed_inputs(&results);
    std::thread::sleep(Duration::from_millis(300));
    let interrupted = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(interrupted.success());

    assert_eq!(
        wait_for_exit(&mut child),
        Some(20),
        "an interrupted attempt seals partial"
    );
    assert_interrupted_bundle(&results);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_group_wide_sigint_is_an_interrupt_and_not_a_crash() {
    // What Ctrl-C at a terminal actually does: the signal reaches the control
    // plane *and* the subject it is supervising. The subject dies of SIGINT on
    // its own, so `try_wait` reports a signal death before the poll loop ever
    // reads the interrupt latch — and reading the latch only there used to make
    // a plain Ctrl-C seal `crashed`.
    //
    // `process_group(0)` puts benchctl in a group of its own so that signalling
    // the group cannot reach the test runner. The fake adapter inherits it,
    // which is the point.
    use std::os::unix::process::CommandExt as _;

    let dir = scratch("sigint-group");
    let (subjects, cluster) = write_fixtures(&dir, "run-hang");
    let results = dir.join("results");
    let mut child = Command::new(BENCHCTL)
        .args(run_arguments(&subjects, &cluster, &results))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()
        .unwrap();

    wait_for_sealed_inputs(&results);
    std::thread::sleep(Duration::from_millis(300));
    // A negative pid is the whole process group, as `kill(2)` defines it.
    let interrupted = Command::new("kill")
        .args(["-INT", &format!("-{}", child.id())])
        .status()
        .unwrap();
    assert!(interrupted.success());

    assert_eq!(
        wait_for_exit(&mut child),
        Some(20),
        "a group-wide interrupt seals partial, not crashed (21)"
    );
    assert_interrupted_bundle(&results);
    std::fs::remove_dir_all(&dir).unwrap();
}
