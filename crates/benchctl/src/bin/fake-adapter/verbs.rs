//! One function per verb the control plane can ask this fixture for.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use bench_schema::{
    AdapterCapabilities, AdapterDescription, AdapterStatus, LoadMode, ValidateReport,
};
use benchctl::utc_rfc3339_millis;
use serde_json::json;

use super::document::{ADAPTER_NAME, ADAPTER_VERSION, result_document};
use super::io::{flag, print_document, read_experiment, render, subject_name, write_status};
use super::mode::Mode;

/// Exit code for a usage error, matching the control plane's table.
const EXIT_USAGE: i32 = 64;
/// Exit code the `run-nonzero` mode exits with.
const EXIT_RUN_FAILED: i32 = 3;

/// `describe --json`.
pub(crate) fn describe(mode: Mode) -> i32 {
    if mode == Mode::DescribeGarbage {
        println!("this is not a capability document");
        return 0;
    }
    let mut result_schemas = BTreeMap::new();
    for load_mode in [LoadMode::ClosedLoop, LoadMode::ScheduledOpenLoopFixedRate] {
        result_schemas.insert(load_mode, bench_schema::PRODUCER_BENCHMARK_V2.to_owned());
    }
    let description = AdapterDescription {
        schema: AdapterDescription::SCHEMA.to_owned(),
        name: ADAPTER_NAME.to_owned(),
        version: ADAPTER_VERSION.to_owned(),
        capabilities: AdapterCapabilities {
            producer: true,
            consumer: false,
            idempotence: true,
            transactions: false,
            tls: false,
            compression: vec!["none".to_owned()],
            completion_modes: vec!["aggregate-batch-terminal".to_owned()],
            ownership_modes: vec!["public-batch".to_owned()],
            metric_families: vec!["producer_requests".to_owned()],
        },
        result_schemas: Some(result_schemas),
    };
    print_document(bench_schema::pretty_bytes(&description))
}

/// `validate --experiment <file>`.
pub(crate) fn validate() -> i32 {
    print_document(bench_schema::pretty_bytes(&ValidateReport::supported()))
}

/// `run --experiment <file> --output <dir>`.
pub(crate) fn run(mode: Mode, tail: &[String]) -> i32 {
    let Some(output) = flag(tail, "--output").map(PathBuf::from) else {
        eprintln!("fake-adapter: run needs --output");
        return EXIT_USAGE;
    };
    match mode {
        Mode::RunAbort => {
            eprintln!("fake-adapter: aborting on purpose");
            std::process::abort();
        }
        Mode::RunWritePidThenHang => {
            let pid_path = output.join("pid");
            if let Err(error) = std::fs::write(&pid_path, format!("{}\n", std::process::id())) {
                eprintln!(
                    "fake-adapter: could not write {}: {error}",
                    pid_path.display()
                );
                return EXIT_USAGE;
            }
            hang()
        }
        Mode::RunHang => hang(),
        _ => run_measuring(mode, tail, &output),
    }
}

/// Sleeps until something kills this process.
fn hang() -> i32 {
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// The `run` verb for the modes that actually write documents.
fn run_measuring(mode: Mode, tail: &[String], output: &Path) -> i32 {
    let started_at = utc_rfc3339_millis(SystemTime::now());
    let Some(experiment_path) = flag(tail, "--experiment") else {
        eprintln!("fake-adapter: run needs --experiment");
        return EXIT_USAGE;
    };
    let experiment = match read_experiment(Path::new(&experiment_path)) {
        Ok(experiment) => experiment,
        Err(reason) => {
            write_status(
                output,
                &AdapterStatus::failed(
                    "load",
                    reason.clone(),
                    started_at.as_str(),
                    utc_rfc3339_millis(SystemTime::now()).as_str(),
                ),
            );
            eprintln!("fake-adapter: {reason}");
            return EXIT_RUN_FAILED;
        }
    };
    let subject = subject_name(output);
    if mode == Mode::RunNonzero {
        write_status(
            output,
            &AdapterStatus::failed(
                "produce",
                "the fixture was asked to fail",
                started_at.as_str(),
                utc_rfc3339_millis(SystemTime::now()).as_str(),
            ),
        );
        eprintln!("fake-adapter: failing on purpose");
        return EXIT_RUN_FAILED;
    }
    if let Err(error) = std::fs::write(
        output.join("result.json"),
        render(bench_schema::pretty_bytes(&result_document(
            &experiment,
            &subject,
            mode,
        )))
        .as_bytes(),
    ) {
        eprintln!("fake-adapter: could not write result.json: {error}");
        return EXIT_RUN_FAILED;
    }
    write_status(
        output,
        &AdapterStatus::succeeded(
            started_at.as_str(),
            utc_rfc3339_millis(SystemTime::now()).as_str(),
        ),
    );
    0
}

/// `verify <bootstrap> <topic> <run-id> <records> <payload-bytes> <partitions>`.
pub(crate) fn verify(mode: Mode, tail: &[String]) -> i32 {
    if mode == Mode::VerifierFail {
        eprintln!("fake-adapter: the verifier was asked to fail");
        return 1;
    }
    let topic = tail.get(1).cloned().unwrap_or_default();
    let records: u64 = tail.get(3).and_then(|text| text.parse().ok()).unwrap_or(0);
    let partitions: u64 = tail.get(5).and_then(|text| text.parse().ok()).unwrap_or(0);
    let valid = mode != Mode::VerifierInvalid;
    let report = json!({
        "schema": bench_schema::PRODUCER_VERIFICATION_V1,
        "topic": topic,
        "expected_records": records,
        "verified_records": if valid { records } else { records.saturating_sub(1) },
        "duplicates": 0,
        "missing_records": u64::from(!valid),
        "corrupt": 0,
        "unexpected": 0,
        "eof_partitions": partitions,
        "valid": valid,
    });
    print!("{}", render(bench_schema::pretty_bytes(&report)));
    0
}

/// `topics-create <bootstrap> <partitions> <replication-factor> <topic...>`.
pub(crate) fn topics_create(mode: Mode, tail: &[String]) -> i32 {
    if mode == Mode::TopicsFail {
        eprintln!("fake-adapter: the topic tool was asked to fail");
        return 1;
    }
    println!("created {} topics", tail.len().saturating_sub(3));
    0
}
