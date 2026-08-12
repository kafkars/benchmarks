//! Driving the real `benchctl` binary against the fixture adapter.
//!
//! The helpers here are what an end-to-end check of the verb surface needs and
//! the in-process helpers beside them do not: a scratch directory, the two
//! fixture TOML files, a way to run the binary and read its streams back, and
//! the report directory a looping verb leaves behind. Nothing here touches a
//! Kafka cluster or a network.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use bench_schema::SuiteSummary;

/// The control-plane binary under test.
pub(crate) const BENCHCTL: &str = env!("CARGO_BIN_EXE_benchctl");

/// The bootstrap every offline fixture binds to.
pub(crate) const BOOTSTRAP: &str = "127.0.0.1:39092,127.0.0.1:39093,127.0.0.1:39094";

/// Repository root, two levels above this crate's manifest.
pub(crate) fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

/// One scratch directory per test, removed on success.
pub(crate) fn scratch(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "benchctl-suite-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Renders an argv as a TOML array of strings.
pub(crate) fn command_toml(argv: &[String]) -> String {
    let quoted: Vec<String> = argv.iter().map(|part| format!("{part:?}")).collect();
    format!("[{}]", quoted.join(", "))
}

/// Writes the subjects and cluster fixtures, every subject in `mode`.
pub(crate) fn write_fixtures(dir: &Path, mode: &str, subjects: &[&str]) -> (PathBuf, PathBuf) {
    let adapter = super::adapter(mode);
    let tool = |verb: &str| {
        let mut argv = adapter.clone();
        argv.push(verb.to_owned());
        command_toml(&argv)
    };
    let mut subjects_toml = String::new();
    for name in subjects {
        let _ = writeln!(
            subjects_toml,
            "[[subjects]]\nname = \"{name}\"\ncommand = {}\n",
            command_toml(&adapter)
        );
    }
    let subjects_path = dir.join("subjects.toml");
    std::fs::write(&subjects_path, subjects_toml).unwrap();
    let cluster_path = dir.join("cluster.toml");
    std::fs::write(
        &cluster_path,
        format!(
            "name = \"offline\"\n\
             bootstrap = \"127.0.0.1:39092,127.0.0.1:39093,127.0.0.1:39094\"\n\
             broker_version = \"4.3.1\"\nbrokers = 3\nsecurity = \"plaintext\"\n\
             lifecycle = \"externally managed by the caller\"\n\n\
             [tools]\ntopic_create = {create}\ntopic_delete = {delete}\nverify = {verify}\n",
            create = tool("topics-create"),
            delete = tool("topics-delete"),
            verify = tool("verify"),
        ),
    )
    .unwrap();
    (subjects_path, cluster_path)
}

/// Runs `benchctl` with `arguments` and returns its exit code, stdout, stderr.
pub(crate) fn benchctl_output(arguments: &[String]) -> (i32, String, String) {
    let output = Command::new(BENCHCTL).args(arguments).output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    let code = output.status.code().unwrap_or(-1);
    assert!(
        code >= 0,
        "benchctl died on a signal; stderr:\n{stderr}\narguments: {arguments:?}"
    );
    (
        code,
        String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr,
    )
}

/// Runs `benchctl` with `arguments` and returns its exit code and stdout.
pub(crate) fn benchctl(arguments: &[String]) -> (i32, String) {
    let (code, stdout, _stderr) = benchctl_output(arguments);
    (code, stdout)
}

/// The argv shared by `suite` and `capacity`.
pub(crate) fn common_arguments(
    verb: &str,
    scenario: &Path,
    subjects: &Path,
    cluster: &Path,
    results: &Path,
    reports: &Path,
) -> Vec<String> {
    [
        verb,
        "--experiment",
        scenario.to_str().unwrap(),
        "--subjects",
        subjects.to_str().unwrap(),
        "--cluster",
        cluster.to_str().unwrap(),
        "--bootstrap",
        BOOTSTRAP,
        "--results",
        results.to_str().unwrap(),
        "--reports",
        reports.to_str().unwrap(),
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

/// The single directory a report root holds, whatever it is named.
pub(crate) fn only_report_directory(reports: &Path) -> PathBuf {
    let mut found = Vec::new();
    let mut stack = vec![reports.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                if std::fs::read_dir(&path)
                    .unwrap()
                    .flatten()
                    .any(|child| child.path().is_file())
                {
                    found.push(path);
                } else {
                    stack.push(path);
                }
            }
        }
    }
    assert_eq!(
        found.len(),
        1,
        "expected one report directory, got {found:?}"
    );
    found.remove(0)
}

/// Runs a three-repetition suite and returns its report directory and exit code.
pub(crate) fn run_suite(dir: &Path, label: &str) -> (i32, PathBuf) {
    let (subjects, cluster) = write_fixtures(dir, "ok", &["kafkars", "librdkafka-c"]);
    let results = dir.join(format!("results-{label}"));
    let reports = dir.join(format!("reports-{label}"));
    let mut arguments = common_arguments(
        "suite",
        &repo_root().join("scenarios/producer/legacy-balanced-1k.toml"),
        &subjects,
        &cluster,
        &results,
        &reports,
    );
    arguments.push("--repetitions".to_owned());
    arguments.push("3".to_owned());
    let (code, _stdout) = benchctl(&arguments);
    (code, only_report_directory(&reports))
}

/// A suite summary with the two per-attempt fields that cannot repeat blanked.
///
/// Attempt ids carry a timestamp and entropy, and a bundle digest covers those
/// ids, so neither can be equal across two independent suites. Everything else
/// in the document — every observation, every median, every interval, every gate
/// — is derived from the resolved experiments and must be identical.
pub(crate) fn without_attempt_identity(bytes: &[u8]) -> Vec<u8> {
    let mut summary = SuiteSummary::from_slice(bytes).unwrap();
    for attempt in &mut summary.attempts {
        "<attempt>".clone_into(&mut attempt.attempt_id);
        "<digest>".clone_into(&mut attempt.bundle_digest);
    }
    bench_schema::pretty_bytes(&summary).unwrap()
}

/// Finds the bundle directory an attempt id names, anywhere under `results`.
pub(crate) fn find_bundle(results: &Path, attempt_id: &str) -> PathBuf {
    let mut stack = vec![results.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|name| name == attempt_id) {
                    return path;
                }
                stack.push(path);
            }
        }
    }
    panic!("no bundle named {attempt_id} under {}", results.display());
}
