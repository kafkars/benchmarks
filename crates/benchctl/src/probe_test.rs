//! Probe behaviour against real child processes: a well-behaved subject, a
//! silent one, a chatty one, a slow one, and one that is not a subject at all.
//!
//! The fixtures are `/bin/sh`, `/bin/echo`, and `/bin/cat`, which exist on both
//! platforms this repository supports and need no build step. Note that the
//! probe *appends* its verb and flags to the configured command, so a fixture
//! has to tolerate trailing arguments: `sh -c <script>` puts them in `$0` and
//! `$@` where a script can ignore or inspect them, while `echo` and `cat` show
//! what a program that does not speak the protocol does with them.
#![expect(clippy::unwrap_used, reason = "test assertions may unwrap")]

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use bench_schema::{AdapterCapabilities, AdapterDescription, ValidateReport, pretty_bytes};

use crate::attempt::AttemptId;
use crate::error::CtlErrorKind;
use crate::probe::{describe, validate};

const CAP: u64 = 1_048_576;
const TIMEOUT: Duration = Duration::from_secs(10);

fn scratch(label: &str) -> PathBuf {
    let directory = std::env::temp_dir().join(format!(
        "benchctl-probe-test-{}-{label}-{}",
        std::process::id(),
        AttemptId::generate(SystemTime::now()).as_str()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    directory
}

/// A subject that runs a shell script; trailing protocol arguments land in
/// `$0` and `$@`.
fn shell(script: &str) -> Vec<String> {
    vec!["/bin/sh".to_owned(), "-c".to_owned(), script.to_owned()]
}

/// A subject that prints the contents of a file, whatever it is asked.
fn printing(path: &Path) -> Vec<String> {
    shell(&format!("cat {}", path.display()))
}

fn write(directory: &Path, name: &str, contents: &[u8]) -> PathBuf {
    let path = directory.join(name);
    std::fs::write(&path, contents).unwrap();
    path
}

fn description() -> AdapterDescription {
    AdapterDescription {
        schema: AdapterDescription::SCHEMA.to_owned(),
        name: "fixture".to_owned(),
        version: "1.2.3".to_owned(),
        capabilities: AdapterCapabilities {
            producer: true,
            compression: vec!["none".to_owned()],
            ..AdapterCapabilities::default()
        },
        result_schemas: None,
    }
}

#[test]
fn describe_reads_the_capability_document_a_subject_prints() {
    let directory = scratch("describe-ok");
    let document = write(
        &directory,
        "describe.json",
        &pretty_bytes(&description()).unwrap(),
    );

    let observed = describe(&printing(&document), TIMEOUT, CAP).unwrap();

    assert_eq!(observed, description());
}

#[test]
fn describe_refuses_a_subject_that_prints_something_else() {
    // `echo` prints its arguments, so the probe's own `describe --json` comes
    // back as the answer — which is exactly the kind of nonsense a
    // protocol-unaware program produces.
    let error = describe(&["/bin/echo".to_owned()], TIMEOUT, CAP).unwrap_err();

    assert_eq!(error.kind(), CtlErrorKind::InvalidExperiment);
    assert!(
        error.message().contains("no readable document"),
        "{}",
        error.message()
    );
    assert!(
        error.message().contains("describe --json"),
        "the error should quote what the subject actually printed: {}",
        error.message()
    );
}

#[test]
fn describe_refuses_a_document_that_declares_another_schema() {
    let directory = scratch("describe-schema");
    let mut wrong = description();
    wrong.schema = "kafkars.adapter.v99".to_owned();
    let document = write(&directory, "describe.json", &pretty_bytes(&wrong).unwrap());

    let error = describe(&printing(&document), TIMEOUT, CAP).unwrap_err();

    assert_eq!(error.kind(), CtlErrorKind::InvalidExperiment);
    assert!(error.message().contains("v99"), "{}", error.message());
}

#[test]
fn describe_refuses_a_subject_that_exits_non_zero_and_quotes_its_stderr() {
    let error = describe(
        &shell("echo 'no broker configured' >&2; exit 3"),
        TIMEOUT,
        CAP,
    )
    .unwrap_err();

    assert_eq!(error.kind(), CtlErrorKind::InvalidExperiment);
    assert!(error.message().contains("code 3"), "{}", error.message());
    assert!(
        error.message().contains("no broker configured"),
        "{}",
        error.message()
    );
}

#[test]
fn describe_kills_a_subject_that_never_answers() {
    let started = std::time::Instant::now();

    let error = describe(&shell("sleep 5"), Duration::from_millis(200), CAP).unwrap_err();

    assert_eq!(error.kind(), CtlErrorKind::InvalidExperiment);
    assert!(
        error.message().contains("did not answer"),
        "{}",
        error.message()
    );
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "the deadline did not end the probe: {:?}",
        started.elapsed()
    );
}

#[test]
fn describe_refuses_a_subject_that_writes_more_than_the_cap() {
    let directory = scratch("describe-oversize");
    let document = write(
        &directory,
        "describe.json",
        &pretty_bytes(&description()).unwrap(),
    );

    let error = describe(&printing(&document), TIMEOUT, 8).unwrap_err();

    assert_eq!(error.kind(), CtlErrorKind::InvalidExperiment);
    assert!(
        error.message().contains("capture limit"),
        "{}",
        error.message()
    );
}

#[test]
fn describe_refuses_an_empty_command() {
    let error = describe(&[], TIMEOUT, CAP).unwrap_err();

    assert_eq!(error.kind(), CtlErrorKind::InvalidExperiment);
    assert!(error.message().contains("program"), "{}", error.message());
}

#[test]
fn describe_refuses_a_program_that_does_not_exist() {
    let error = describe(&["/nonexistent/benchmark-adapter".to_owned()], TIMEOUT, CAP).unwrap_err();

    assert_eq!(error.kind(), CtlErrorKind::InvalidExperiment);
    assert!(error.message().contains("spawned"), "{}", error.message());
}

#[test]
fn validate_reads_the_report_and_hands_over_the_experiment_path() {
    let directory = scratch("validate-ok");
    let report = ValidateReport::unsupported(vec!["compression is not none".to_owned()]);
    let document = write(&directory, "validate.json", &pretty_bytes(&report).unwrap());
    let seen = directory.join("arguments.txt");
    let subject = shell(&format!(
        "printf '%s\\n' \"$@\" > {}; cat {}",
        seen.display(),
        document.display()
    ));
    let experiment = directory.join("experiment.resolved.json");

    let observed = validate(&subject, &experiment, TIMEOUT, CAP).unwrap();

    assert_eq!(observed, report);
    let arguments = std::fs::read_to_string(&seen).unwrap();
    assert!(
        arguments.contains(&experiment.display().to_string()),
        "the subject was not told which experiment to judge: {arguments}"
    );
}

#[test]
fn validate_refuses_a_program_that_does_not_speak_the_protocol() {
    // `cat` treats the probe's own arguments as file names and fails on them,
    // which is what a subject list pointing at the wrong binary looks like.
    let directory = scratch("validate-cat");
    let experiment = directory.join("experiment.resolved.json");
    std::fs::write(&experiment, b"{}").unwrap();

    let error = validate(&["/bin/cat".to_owned()], &experiment, TIMEOUT, CAP).unwrap_err();

    assert_eq!(error.kind(), CtlErrorKind::InvalidExperiment);
    assert!(error.message().contains("validate"), "{}", error.message());
}
