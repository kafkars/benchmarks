//! Driving a stand-in for the C binary: what lands in the output directory,
//! and what the status document says about each way a run can end.
//!
//! `/bin/echo` stands in for the benchmark: it accepts any arguments, exits
//! zero, and prints them — so a successful run's `result.json` is the argument
//! vector, which makes the translation observable end to end without a broker
//! or a compiled C program. `/bin/cat` stands in for a child that fails, since
//! it treats the vector as file names and exits non-zero, and a missing path
//! stands in for a binary that was never built. All three exist on both
//! platforms this repository supports, which `/bin/false` does not.
#![expect(clippy::unwrap_used, reason = "test fixtures are exact")]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use bench_schema::{AdapterOutcome, AdapterStatus, LoadMode, parse_json_slice, pretty_bytes};

use crate::arguments::{EXIT_FAILURE, EXIT_OK};
use crate::fixture::experiment;
use crate::run::execute;
use crate::translate::{RESULT_FILE, STATUS_FILE};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A bundle-shaped scratch directory: `<scratch>/adapters/librdkafka-c`.
fn scratch(label: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "bench-adapter-librdkafka-{}-{label}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let output = root.join("adapters").join("librdkafka-c");
    std::fs::create_dir_all(&output).unwrap();
    output
}

fn experiment_file(directory: &Path, load_mode: LoadMode) -> PathBuf {
    let path = directory.join("experiment.resolved.json");
    std::fs::write(&path, pretty_bytes(&experiment(load_mode)).unwrap()).unwrap();
    path
}

fn status_of(output: &Path) -> AdapterStatus {
    let bytes = std::fs::read(output.join(STATUS_FILE)).unwrap();
    parse_json_slice(&bytes).unwrap()
}

#[test]
fn a_successful_run_writes_the_result_and_a_succeeded_status() {
    let output = scratch("success");
    let experiment = experiment_file(&output, LoadMode::ClosedLoop);

    let code = execute(Path::new("/bin/echo"), &experiment, &output);

    assert_eq!(code, EXIT_OK);
    let result = std::fs::read_to_string(output.join(RESULT_FILE)).unwrap();
    assert!(
        result.contains("kfb-0123456789abcdef-librdkafka-c-warmup"),
        "the child was not given the translated vector: {result}"
    );
    assert!(result.contains("latency.csv"), "{result}");
    let status = status_of(&output);
    assert_eq!(status.outcome, AdapterOutcome::Succeeded);
    assert_eq!(status.failure, None);
    assert!(status.has_expected_schema());
    assert!(status.started_at.ends_with('Z'));
    assert!(status.finished_at.ends_with('Z'));
}

#[test]
fn the_fixed_rate_run_leads_with_the_flag() {
    let output = scratch("fixed");
    let experiment = experiment_file(&output, LoadMode::ScheduledOpenLoopFixedRate);

    let code = execute(Path::new("/bin/echo"), &experiment, &output);

    assert_eq!(code, EXIT_OK);
    let result = std::fs::read_to_string(output.join(RESULT_FILE)).unwrap();
    assert!(result.starts_with("--fixed-rate "), "{result}");
}

#[test]
fn a_child_that_fails_keeps_its_exit_code_and_says_so() {
    let output = scratch("child-failure");
    let experiment = experiment_file(&output, LoadMode::ClosedLoop);

    let code = execute(Path::new("/bin/cat"), &experiment, &output);

    assert_eq!(code, 1);
    let status = status_of(&output);
    assert_eq!(status.outcome, AdapterOutcome::Failed);
    let failure = status.failure.unwrap();
    assert_eq!(failure.stage, "run");
    assert!(failure.reason.contains("exited with 1"), "{failure:?}");
}

#[test]
fn a_binary_that_cannot_be_spawned_is_a_spawn_failure() {
    let output = scratch("spawn-failure");
    let experiment = experiment_file(&output, LoadMode::ClosedLoop);

    let code = execute(
        Path::new("/nonexistent/librdkafka-producer-benchmark"),
        &experiment,
        &output,
    );

    assert_eq!(code, EXIT_FAILURE);
    let failure = status_of(&output).failure.unwrap();
    assert_eq!(failure.stage, "spawn");
    assert!(failure.reason.contains("librdkafka-producer-benchmark"));
}

#[test]
fn an_unreadable_experiment_still_leaves_a_status() {
    let output = scratch("unreadable");

    let code = execute(
        Path::new("/bin/echo"),
        &output.join("does-not-exist.json"),
        &output,
    );

    assert_eq!(code, EXIT_FAILURE);
    let failure = status_of(&output).failure.unwrap();
    assert_eq!(failure.stage, "read-experiment");
}

#[test]
fn an_experiment_this_adapter_declines_is_never_spawned() {
    let output = scratch("declined");
    let mut document = experiment(LoadMode::ClosedLoop);
    document.payload.bytes = 32;
    let path = output.join("experiment.resolved.json");
    std::fs::write(&path, pretty_bytes(&document).unwrap()).unwrap();

    let code = execute(Path::new("/bin/echo"), &path, &output);

    assert_eq!(code, EXIT_FAILURE);
    let failure = status_of(&output).failure.unwrap();
    assert_eq!(failure.stage, "validate");
    assert!(failure.reason.contains("64 payload bytes"), "{failure:?}");
    assert!(
        !output.join(RESULT_FILE).exists(),
        "a declined experiment must not produce a result document"
    );
}

#[test]
fn the_output_directory_is_created_when_it_does_not_exist_yet() {
    let output = scratch("created").join("nested");
    let parent = output.parent().unwrap().to_path_buf();
    let experiment = experiment_file(&parent, LoadMode::ClosedLoop);

    let code = execute(Path::new("/bin/echo"), &experiment, &output);

    // The directory's name is not a subject, so the adapter falls back to the
    // only librdkafka-c subject in the experiment and runs anyway.
    assert_eq!(code, EXIT_OK);
    assert!(output.join(STATUS_FILE).exists());
}
