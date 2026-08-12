//! A test fixture that speaks the whole adapter protocol, doubles as the topic
//! tool and the verifier, and can fail in every way the control plane has to
//! survive.
//!
//! The always-seal guarantee is a claim about behavior under failure, and a
//! claim about failure that is only tested against success is not tested. This
//! binary is how the integration tests produce a real non-zero exit, a real
//! `SIGABRT`, a real process that ignores its deadline, and a real verifier that
//! disagrees with its adapter — without a Kafka cluster, without a network, and
//! without the flakiness of arranging those conditions for real.
//!
//! # Verbs
//!
//! Protocol: `describe --json`, `validate --experiment <file>`,
//! `run --experiment <file> --output <dir>`.
//!
//! Configured tools, in the legacy positional shapes the control plane appends:
//! `topics-create <bootstrap> <partitions> <replication-factor> <topic...>`,
//! `topics-delete <bootstrap> <topic...>`, and
//! `verify <bootstrap> <topic> <run-id> <records> <payload-bytes> <partitions>`.
//!
//! # Modes
//!
//! The behavior is chosen by a leading `--mode <mode>` argument, falling back to
//! the `FAKE_ADAPTER_MODE` environment variable and then to `ok`. The argument
//! form exists because integration tests run in parallel threads of one process,
//! where a per-test environment variable would be a race.
//!
//! `rate-slo:<threshold>` is the capacity-search fixture: it behaves exactly like
//! `ok` except that its latencies jump above every objective the experiment
//! declares as soon as `offered_records_per_second` exceeds the threshold. The
//! jump is a step function of the resolved experiment alone, so a capacity search
//! over this fixture converges on the threshold with no clock, no load, and no
//! run-to-run variation to bracket around.
//!
//! # What `run` emits
//!
//! `kafkars.producer-benchmark.v2`, for both load modes, satisfying every
//! accounting invariant the schema states: every offer is accepted and
//! acknowledged, the three histograms carry exactly the totals the outcome
//! counts imply, scheduler lateness is present exactly for the scheduled
//! open-loop mode, and the run drains completely. Latencies are sixteen distinct
//! values cycled across the records, so the derived 99th percentile is the top of
//! the cycle regardless of how many records the scenario asks for.
//!
//! # Which subject am I?
//!
//! The resolved experiment names topics per subject, and a subject's adapter is
//! told where to write rather than who it is. This fixture takes its subject
//! name from the last component of `--output`, which is `adapters/<subject>/` by
//! the bundle layout. It is a fixture convention, documented here so that no
//! reader mistakes it for a protocol rule.
#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use bench_schema::{
    AdapterCapabilities, AdapterDescription, AdapterStatus, DeclaredExecution, EncodedHistogram,
    Histogram, LoadMode, MeasuredThroughput, OfferOutcomes, OfferTiming, ProcessResources,
    ProducerBenchmarkV2, QueueObservation, ResolvedExperiment, ValidateReport,
};
use benchctl::utc_rfc3339_millis;
use serde_json::json;

/// This fixture's adapter name.
const ADAPTER_NAME: &str = "fake-adapter";
/// This fixture's adapter version.
const ADAPTER_VERSION: &str = "0.1.0";
/// Exit code for a usage error, matching the control plane's table.
const EXIT_USAGE: i32 = 64;
/// Exit code the `run-nonzero` mode exits with.
const EXIT_RUN_FAILED: i32 = 3;

/// How this invocation should misbehave, if at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Everything succeeds.
    Ok,
    /// `run` writes a failed status and exits non-zero.
    RunNonzero,
    /// `run` never returns.
    RunHang,
    /// `run` dies on `SIGABRT`.
    RunAbort,
    /// `run` writes its process id to `<output>/pid`, then never returns.
    RunWritePidThenHang,
    /// `verify` reports an invalid topic and exits zero.
    VerifierInvalid,
    /// `verify` exits non-zero without a report.
    VerifierFail,
    /// `describe` prints something that is not JSON.
    DescribeGarbage,
    /// `topics-create` exits non-zero.
    TopicsFail,
    /// `run` succeeds, but its latencies exceed every declared objective as soon
    /// as the offered rate is above this threshold.
    RateSlo(u64),
}

impl Mode {
    /// The prefix of the parameterised capacity mode.
    const RATE_SLO_PREFIX: &'static str = "rate-slo:";

    /// Parses a mode name, defaulting to [`Mode::Ok`] for anything unknown.
    ///
    /// An unparseable threshold in `rate-slo:<n>` is a usage mistake in a test,
    /// and falling back to [`Mode::Ok`] would turn it into a capacity search that
    /// never saturates. A threshold of zero saturates at every rate instead,
    /// which fails loudly and immediately.
    fn parse(text: &str) -> Self {
        if let Some(threshold) = text.strip_prefix(Self::RATE_SLO_PREFIX) {
            return Self::RateSlo(threshold.parse().unwrap_or(0));
        }
        match text {
            "run-nonzero" => Self::RunNonzero,
            "run-hang" => Self::RunHang,
            "run-abort" => Self::RunAbort,
            "run-write-pid-then-hang" => Self::RunWritePidThenHang,
            "verifier-invalid" => Self::VerifierInvalid,
            "verifier-fail" => Self::VerifierFail,
            "describe-garbage" => Self::DescribeGarbage,
            "topics-fail" => Self::TopicsFail,
            _ => Self::Ok,
        }
    }
}

fn main() {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let (mode, rest) = take_mode(&arguments);
    let code = if let Some((verb, tail)) = rest.split_first() {
        dispatch(mode, verb, tail)
    } else {
        eprintln!("usage: fake-adapter [--mode <mode>] <verb> [arguments...]");
        EXIT_USAGE
    };
    std::process::exit(code);
}

/// Splits a leading `--mode <mode>` off the argument vector.
fn take_mode(arguments: &[String]) -> (Mode, Vec<String>) {
    let fallback = std::env::var("FAKE_ADAPTER_MODE").unwrap_or_default();
    if let Some(name) = arguments.first() {
        if name == "--mode" {
            let mode = arguments.get(1).map_or(Mode::Ok, |text| Mode::parse(text));
            return (mode, arguments.iter().skip(2).cloned().collect());
        }
    }
    (Mode::parse(&fallback), arguments.to_vec())
}

/// Routes one verb.
fn dispatch(mode: Mode, verb: &str, tail: &[String]) -> i32 {
    match verb {
        "describe" => describe(mode),
        "validate" => validate(),
        "run" => run(mode, tail),
        "topics-create" => topics_create(mode, tail),
        "topics-delete" => 0,
        "verify" => verify(mode, tail),
        other => {
            eprintln!("fake-adapter: unknown verb {other}");
            EXIT_USAGE
        }
    }
}

/// `describe --json`.
fn describe(mode: Mode) -> i32 {
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
fn validate() -> i32 {
    print_document(bench_schema::pretty_bytes(&ValidateReport::supported()))
}

/// `run --experiment <file> --output <dir>`.
fn run(mode: Mode, tail: &[String]) -> i32 {
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

/// Distinct latency values cycled across the records of one histogram.
///
/// Sixteen is enough that the derived 99th percentile is the top of the cycle
/// for every record count above a hundred, and small enough that the sparse
/// bucket list stays short whatever the scenario asks for.
const LATENCY_STEPS: u64 = 16;

/// The four latency knobs one run's histograms are built from, all nanoseconds.
#[derive(Debug, Clone, Copy)]
struct LatencyShape {
    /// Smallest `terminal - intended`.
    terminal_base: u64,
    /// Distance between consecutive `terminal - intended` values.
    terminal_step: u64,
    /// Smallest `call_start - intended`.
    lateness_base: u64,
    /// Distance between consecutive `call_start - intended` values.
    lateness_step: u64,
}

impl LatencyShape {
    /// The shape of a run that comfortably meets every objective.
    ///
    /// `spread` makes the numbers differ per subject, so a comparison between
    /// two fixtures is a ratio somebody has to look at rather than a constant
    /// one.
    fn healthy(spread: u64) -> Self {
        Self {
            terminal_base: 200_000 + spread * 1_000,
            terminal_step: 20_000,
            lateness_base: 5_000,
            lateness_step: 1_000,
        }
    }

    /// The shape of a run that misses every objective the experiment declared.
    ///
    /// The top of the cycle lands a full second above the largest declared
    /// ceiling, so no rounding, no bucket width, and no reader's choice of
    /// percentile can make this run look satisfied.
    fn saturated(ceiling_ms: u64) -> Self {
        let target = ceiling_ms.saturating_add(1_000).saturating_mul(1_000_000);
        let step = target / (LATENCY_STEPS * 2);
        Self {
            terminal_base: target - step * (LATENCY_STEPS - 1),
            terminal_step: step,
            lateness_base: target - step * (LATENCY_STEPS - 1),
            lateness_step: step,
        }
    }
}

/// Builds a valid v2 producer measurement for one subject.
///
/// Every accounting invariant the schema states holds by construction: nothing
/// is refused admission, nothing fails, nothing is left unknown, and the three
/// histograms carry exactly the totals the outcome counts imply.
fn result_document(
    experiment: &ResolvedExperiment,
    subject: &str,
    mode: Mode,
) -> ProducerBenchmarkV2 {
    let run_id = experiment
        .runtime
        .as_ref()
        .map_or_else(String::new, |runtime| runtime.run_id.clone());
    let spread = u64::from(subject.bytes().fold(0u8, u8::wrapping_add));
    let records = experiment.records;
    let shape = latency_shape(experiment, mode, spread);
    let lateness = matches!(experiment.load_mode, LoadMode::ScheduledOpenLoopFixedRate)
        .then(|| latencies(records, shape.lateness_base, shape.lateness_step));
    ProducerBenchmarkV2 {
        schema: ProducerBenchmarkV2::SCHEMA.to_owned(),
        adapter: ADAPTER_NAME.to_owned(),
        adapter_version: ADAPTER_VERSION.to_owned(),
        run_id,
        load_mode: experiment.load_mode,
        declared: DeclaredExecution {
            payload_construction: "prebuilt-pool".to_owned(),
            ownership: "copy-in".to_owned(),
            completion_mode: "aggregate-batch-terminal".to_owned(),
            serialization: "excluded".to_owned(),
        },
        outcomes: OfferOutcomes {
            offered: records,
            accepted: records,
            acknowledged: records,
            failed: 0,
            timed_out: 0,
            unknown: 0,
        },
        timing: OfferTiming {
            clock: "monotonic-ns".to_owned(),
            intended_to_terminal: latencies(records, shape.terminal_base, shape.terminal_step),
            accepted_to_terminal: latencies(
                records,
                shape.terminal_base / 2,
                shape.terminal_step / 2,
            ),
            call_start_to_accepted: latencies(records, 10_000, 1_000),
            intended_to_call_start: lateness,
        },
        throughput: throughput(experiment, spread),
        queue: QueueObservation {
            max_outstanding_observed: experiment.application.max_outstanding_records.min(records),
            final_outstanding: 0,
        },
        resources: Some(ProcessResources {
            max_rss_bytes: 64 * 1024 * 1024,
            user_cpu_ns: records * 1_000,
            system_cpu_ns: records * 500,
        }),
        native_metrics_path: None,
        valid: true,
        invalid_reason: None,
    }
}

/// Chooses the latency shape this invocation reports.
///
/// Only `rate-slo:<threshold>` can choose the saturated shape, and it chooses it
/// purely from the resolved experiment: the offered rate above the threshold, and
/// the largest ceiling the experiment's objectives declare.
fn latency_shape(experiment: &ResolvedExperiment, mode: Mode, spread: u64) -> LatencyShape {
    let Mode::RateSlo(threshold) = mode else {
        return LatencyShape::healthy(spread);
    };
    let offered = experiment.offered_records_per_second.unwrap_or(0);
    if offered > threshold {
        let ceiling = experiment
            .slo
            .corrected_p99_ms
            .unwrap_or(0)
            .max(experiment.slo.schedule_delay_p99_ms.unwrap_or(0));
        LatencyShape::saturated(ceiling)
    } else {
        LatencyShape::healthy(spread)
    }
}

/// Records `count` latencies cycling through [`LATENCY_STEPS`] distinct values.
fn latencies(count: u64, base_ns: u64, step_ns: u64) -> EncodedHistogram {
    let mut histogram = Histogram::new();
    for index in 0..count {
        histogram.record(base_ns + (index % LATENCY_STEPS) * step_ns);
    }
    histogram.encode()
}

/// Goodput over the measured interval, deterministic per subject.
#[expect(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "a fixture's throughput is invented evidence, not identity-bearing arithmetic"
)]
fn throughput(experiment: &ResolvedExperiment, spread: u64) -> MeasuredThroughput {
    let acknowledged_records_per_second = 100_000.0 + spread as f64;
    let measured_duration_ns =
        (experiment.records as f64 / acknowledged_records_per_second * 1e9) as u64;
    MeasuredThroughput {
        measured_duration_ns,
        acknowledged_records_per_second,
        acknowledged_payload_bytes_per_second: acknowledged_records_per_second
            * f64::from(experiment.payload.bytes),
    }
}

/// `verify <bootstrap> <topic> <run-id> <records> <payload-bytes> <partitions>`.
fn verify(mode: Mode, tail: &[String]) -> i32 {
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
fn topics_create(mode: Mode, tail: &[String]) -> i32 {
    if mode == Mode::TopicsFail {
        eprintln!("fake-adapter: the topic tool was asked to fail");
        return 1;
    }
    println!("created {} topics", tail.len().saturating_sub(3));
    0
}

/// Reads and validates the resolved experiment.
fn read_experiment(path: &Path) -> Result<ResolvedExperiment, String> {
    let bytes = std::fs::read(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let experiment: ResolvedExperiment = bench_schema::parse_json_slice(&bytes)
        .map_err(|error| format!("parse {}: {error}", path.display()))?;
    experiment
        .validate()
        .map_err(|error| format!("validate {}: {error}", path.display()))?;
    Ok(experiment)
}

/// The subject name, taken from the output directory's last component.
fn subject_name(output: &Path) -> String {
    output
        .file_name()
        .map_or_else(String::new, |name| name.to_string_lossy().into_owned())
}

/// Writes an adapter status document, reporting but not failing on error.
fn write_status(output: &Path, status: &AdapterStatus) {
    let bytes = render(bench_schema::pretty_bytes(status));
    if let Err(error) = std::fs::write(output.join("status.json"), bytes.as_bytes()) {
        eprintln!("fake-adapter: could not write status.json: {error}");
    }
}

/// Turns rendered document bytes into text, substituting an empty document if
/// the render somehow failed.
///
/// The rendering is passed in rather than performed here because this binary
/// does not depend on `serde` directly.
fn render(rendered: bench_schema::SchemaResult<Vec<u8>>) -> String {
    rendered.map_or_else(
        |error| {
            eprintln!("fake-adapter: could not render a document: {error}");
            "{}\n".to_owned()
        },
        |bytes| String::from_utf8_lossy(&bytes).into_owned(),
    )
}

/// Prints a document to standard output.
fn print_document(rendered: bench_schema::SchemaResult<Vec<u8>>) -> i32 {
    print!("{}", render(rendered));
    0
}

/// Returns the value of a `--flag value` pair.
fn flag(arguments: &[String], name: &str) -> Option<String> {
    arguments
        .iter()
        .position(|argument| argument == name)
        .and_then(|index| arguments.get(index + 1))
        .cloned()
}
