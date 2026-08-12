//! `benchctl pack`, offline: a two-entry manifest that dispatches one entry to
//! the suite path and one to a single run, and leaves exactly the evidence
//! those two verbs would have left on their own.
//!
//! The property this file exists for is that a pack adds nothing. Running the
//! manifest must produce the same tree as typing `benchctl suite` and then
//! `benchctl run` by hand — one suite report set, two sealed bundles for the
//! suite entry, one for the run entry — because a runner that aggregated,
//! reordered, or retried would make "what the nightly ran" a question only the
//! runner can answer.
//!
//! Everything runs against the `fake-adapter` fixture playing every subject and
//! all three cluster tools, so there is no Kafka, no network, and no clock
//! dependence.
#![expect(
    clippy::unwrap_used,
    reason = "a fixture that cannot be built must fail the test immediately"
)]

mod common;

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use bench_schema::SuiteSummary;

/// The control-plane binary under test.
const BENCHCTL: &str = env!("CARGO_BIN_EXE_benchctl");

/// The bootstrap every offline fixture binds to.
const BOOTSTRAP: &str = "127.0.0.1:39092,127.0.0.1:39093,127.0.0.1:39094";

/// Repository root, two levels above this crate's manifest.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

/// One scratch directory per test, removed on success.
fn scratch(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "benchctl-pack-{label}-{}-{}",
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
fn command_toml(argv: &[String]) -> String {
    let quoted: Vec<String> = argv.iter().map(|part| format!("{part:?}")).collect();
    format!("[{}]", quoted.join(", "))
}

/// Writes the subjects and cluster fixtures, every subject in `mode`.
fn write_fixtures(dir: &Path, mode: &str, subjects: &[&str]) -> (PathBuf, PathBuf) {
    let adapter = common::adapter(mode);
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
             bootstrap = \"{BOOTSTRAP}\"\n\
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

/// Runs `benchctl` from the repository root and returns its exit code, stdout,
/// and stderr.
///
/// The working directory matters here and nowhere else: a manifest writes
/// repository-root-relative scenario paths, so a pack is defined to run from
/// the root.
fn benchctl(arguments: &[String]) -> (i32, String, String) {
    let output = Command::new(BENCHCTL)
        .args(arguments)
        .current_dir(repo_root())
        .output()
        .unwrap();
    let code = output.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(code >= 0, "benchctl died on a signal; stderr:\n{stderr}");
    (code, stdout, stderr)
}

/// The pack argv, over `manifest`.
fn pack_arguments(
    manifest: &Path,
    subjects: &Path,
    cluster: &Path,
    results: &Path,
    reports: &Path,
) -> Vec<String> {
    [
        "pack",
        "--manifest",
        manifest.to_str().unwrap(),
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

/// A two-entry manifest: one two-repetition suite, then one single run.
fn manifest(dir: &Path, entries: &str) -> PathBuf {
    let path = dir.join("pack.toml");
    std::fs::write(
        &path,
        format!(
            "name = \"offline-fixture\"\n\
             cadence = \"test\"\n\
             description = \"Two entries, driven by the offline integration test.\"\n\n\
             {entries}"
        ),
    )
    .unwrap();
    path
}

/// Every directory under `results` that holds a sealed `status.json`.
fn sealed_bundles(results: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![results.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.join("status.json").is_file() {
                    found.push(path);
                } else {
                    stack.push(path);
                }
            }
        }
    }
    found.sort();
    found
}

/// Every directory under `reports` that holds a suite summary.
fn suite_reports(reports: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![reports.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.join("suite-summary.json").is_file() {
                    found.push(path);
                } else {
                    stack.push(path);
                }
            }
        }
    }
    found.sort();
    found
}

#[test]
fn a_two_entry_pack_runs_a_suite_then_a_single_run_and_exits_zero() {
    let dir = scratch("two-entries");
    let (subjects, cluster) = write_fixtures(&dir, "ok", &["kafkars", "librdkafka-c"]);
    let results = dir.join("results");
    let reports = dir.join("reports");
    let path = manifest(
        &dir,
        "[[entries]]\n\
         scenario = \"scenarios/producer/legacy-balanced-1k.toml\"\n\
         repetitions = 2\n\n\
         [[entries]]\n\
         scenario = \"scenarios/producer/legacy-balanced-1k.toml\"\n\
         repetitions = 1\n",
    );
    let (code, stdout, stderr) = benchctl(&pack_arguments(
        &path, &subjects, &cluster, &results, &reports,
    ));
    assert_eq!(code, 0, "every entry exited 0; stderr:\n{stderr}");

    // The per-entry lines say which verb each entry dispatched to, before it
    // runs and after it ends.
    assert!(
        stdout.contains("==> pack offline-fixture (test): 2 entries"),
        "{stdout}"
    );
    assert!(stdout.contains("entry 1 of 2"), "{stdout}");
    assert!(stdout.contains("via suite (2 repetitions)"), "{stdout}");
    assert!(stdout.contains("entry 2 of 2"), "{stdout}");
    assert!(stdout.contains("via run (1 repetition)"), "{stdout}");
    assert!(stdout.contains("exited 0"), "{stdout}");

    // The closing table is one row per entry, in the order they ran.
    assert!(
        stdout.contains("| # | scenario | verb | repetitions | exit |"),
        "{stdout}"
    );
    assert!(stdout.contains("| suite | 2 | 0 |"), "{stdout}");
    assert!(stdout.contains("| run | 1 | 0 |"), "{stdout}");
    assert!(stdout.contains("2 of 2 entries exited 0."), "{stdout}");

    // Three sealed bundles: two for the suite entry and one for the run entry.
    // A pack contributes no attempt of its own, so any other count would mean
    // the runner had invented or skipped one.
    let bundles = sealed_bundles(&results);
    assert_eq!(bundles.len(), 3, "{bundles:?}");

    // Exactly one suite report set, from the entry that asked for one. The run
    // entry writes no report, because `benchctl run` does not.
    let summaries = suite_reports(&reports);
    assert_eq!(summaries.len(), 1, "{summaries:?}");
    let summary =
        SuiteSummary::from_slice(&std::fs::read(summaries[0].join("suite-summary.json")).unwrap())
            .unwrap();
    assert_eq!(summary.repetitions, 2);
    assert_eq!(summary.attempts.len(), 2);
    assert!(!summary.claim_eligible, "never in this milestone");
    for name in ["report.md", "report.html", "analysis-packet.json"] {
        assert!(summaries[0].join(name).is_file(), "{name} is missing");
    }

    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn an_entry_that_cannot_be_planned_fails_that_entry_and_not_the_rest() {
    let dir = scratch("broken-entry");
    let (subjects, cluster) = write_fixtures(&dir, "ok", &["kafkars"]);
    let results = dir.join("results");
    let reports = dir.join("reports");
    let path = manifest(
        &dir,
        "[[entries]]\n\
         scenario = \"scenarios/producer/there-is-no-such-scenario.toml\"\n\
         repetitions = 1\n\n\
         [[entries]]\n\
         scenario = \"scenarios/producer/legacy-balanced-1k.toml\"\n\
         repetitions = 1\n",
    );
    let (code, stdout, _stderr) = benchctl(&pack_arguments(
        &path, &subjects, &cluster, &results, &reports,
    ));
    assert_eq!(
        code, 20,
        "one entry did not exit 0, and the pack says so without inventing a code of its own"
    );
    assert!(stdout.contains("could not be planned, exit 65"), "{stdout}");
    assert!(stdout.contains("1 of 2 entries exited 0."), "{stdout}");
    // The second entry still ran: one broken file in a nightly must not cost
    // the evidence from the others.
    assert_eq!(sealed_bundles(&results).len(), 1);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_manifest_that_is_not_a_manifest_is_refused_before_anything_runs() {
    let dir = scratch("bad-manifest");
    let (subjects, cluster) = write_fixtures(&dir, "ok", &["kafkars"]);
    let results = dir.join("results");
    let path = dir.join("pack.toml");
    std::fs::write(&path, "name = \"broken\"\n").unwrap();
    let (code, _stdout, stderr) = benchctl(&pack_arguments(
        &path,
        &subjects,
        &cluster,
        &results,
        &dir.join("reports"),
    ));
    assert_eq!(code, 65, "a manifest that cannot be read is invalid input");
    assert!(stderr.contains("pack manifest"), "{stderr}");
    assert!(!results.exists(), "nothing was run, so nothing was sealed");
    std::fs::remove_dir_all(&dir).unwrap();
}
