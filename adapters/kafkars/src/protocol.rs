//! The adapter protocol: `describe`, `validate`, and `run` over the resolved
//! experiment document.
//!
//! # Two surfaces, one measurement
//!
//! This adapter keeps its legacy positional commands (`produce`,
//! `produce-fixed`, `payload`, `schedule`, `topics-*`) exactly as they were,
//! because the migrated Node control plane still drives them and its evidence
//! has to stay comparable. The protocol verbs added here are a second door into
//! the same rooms: they parse a `kafkars.experiment.v1` document, map it onto
//! the very same argument structs the legacy commands build, and call the very
//! same phase code. Nothing about the measured path is duplicated, so the two
//! surfaces cannot measure different things.
//!
//! The result document is byte-identical between the two doors as well. Both
//! render through [`producer::RunOutcome::render`]; the legacy arm prints the
//! line to stdout and this module writes the same line, newline-terminated,
//! into `result.json` — which is exactly what the legacy shell redirect
//! produced.
//!
//! # Declining is a first-class answer
//!
//! [`validate`] compares the experiment against what this adapter actually
//! configures. Everything in `producer.rs` above the argument surface is a
//! constant — the queue budget, the batching, the retry policy, the in-flight
//! ceiling — so an experiment asking for anything else would be run as a
//! different experiment. Saying so in a validate report is the difference
//! between a missing subject somebody can explain and a comparison that quietly
//! answers the wrong question.
//!
//! # Which subject am I?
//!
//! The control plane's output directory is `adapters/<subject>/`, so its last
//! component names the subject whose topics this process must use. When that
//! name is not a subject — `validate` is called without an output directory at
//! all — the fallback is the unique subject whose adapter is this one, and an
//! ambiguous experiment is refused rather than guessed at.

use std::{
    error::Error,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bench_schema::{
    AdapterCapabilities, AdapterDescription, AdapterStatus, ArrivalModel, ExperimentKind, LoadMode,
    ResolvedExperiment, SubjectSpec, ValidateReport, pretty_bytes,
};

use crate::{
    arguments::{FixedProduceArgs, ProduceArgs},
    producer,
};

/// Adapter name, matching the subject name the legacy harness used.
pub(crate) const ADAPTER_NAME: &str = "kafkars";

/// Result document this adapter writes for a closed-loop run.
pub(crate) const CLOSED_LOOP_RESULT_SCHEMA: &str = "kafkars.producer-benchmark.v1";

/// Result document this adapter writes for a fixed-rate run.
pub(crate) const FIXED_RATE_RESULT_SCHEMA: &str = "kafkars.producer-fixed-load.v1";

/// Per-record latency evidence, written next to the result.
pub(crate) const LATENCY_FILE: &str = "latency.csv";

/// The measurement document the control plane seals.
pub(crate) const RESULT_FILE: &str = "result.json";

/// This adapter's own terminal status.
pub(crate) const STATUS_FILE: &str = "status.json";

/// Client queue budget `producer.rs` configures, in bytes.
const QUEUE_BYTES: u64 = 64 * 1024 * 1024;

/// Records per broker-bound batch.
const BATCH_RECORDS: u32 = 256;

/// Bytes per broker-bound batch.
const BATCH_BYTES: u64 = 65_536;

/// Bytes per produce request.
const REQUEST_BYTES: u64 = 1_048_576;

/// Produce requests in flight per broker.
const MAX_IN_FLIGHT_REQUESTS_PER_BROKER: u32 = 5;

/// Milliseconds the client may wait to fill a batch.
const LINGER_MS: u64 = 5;

/// Milliseconds before a record's delivery is abandoned.
const DELIVERY_TIMEOUT_MS: u64 = 60_000;

/// Re-enqueue attempts after a retriable failure.
const RETRY_MAX_REPLACEMENTS: u32 = 600;

/// Milliseconds between retries.
const RETRY_BACKOFF_MS: u64 = 100;

/// Smallest payload the identity envelope fits in.
const MIN_PAYLOAD_BYTES: u32 = 64;

/// Callers the closed-loop phase admits from.
const CLOSED_LOOP_CALLERS: u32 = 1;

/// Callers the fixed-rate phase requires, exactly.
const FIXED_RATE_CALLERS: u32 = 4;

/// Prints the capability document.
pub(crate) fn describe() -> Result<(), Box<dyn Error>> {
    print_document(&description())
}

/// Prints the verdict on one resolved experiment.
pub(crate) fn validate(experiment_path: &Path) -> Result<(), Box<dyn Error>> {
    let experiment = read_experiment(experiment_path)?;
    let subject = subject_of(&experiment, None).ok().map(|s| s.name.clone());
    print_document(&report(&experiment, subject.as_deref()))
}

/// Runs one resolved experiment, writing every artefact into `output`.
///
/// A status document is written on every path out of this function, including
/// the ones that fail before the client is built. A missing status therefore
/// means the process was killed, which is a distinction the control plane
/// cannot make for itself.
pub(crate) fn run(experiment_path: &Path, output: &Path) -> Result<(), Box<dyn Error>> {
    let started_at = utc_rfc3339_millis(SystemTime::now());
    std::fs::create_dir_all(output)?;
    match execute(experiment_path, output) {
        Ok(outcome) => {
            let status = if outcome.valid {
                AdapterStatus::succeeded(started_at, now())
            } else {
                AdapterStatus::failed("drain", outcome.invalid_reason, started_at, now())
            };
            write_status(output, &status)?;
            if outcome.valid {
                Ok(())
            } else {
                Err(outcome.invalid_reason.into())
            }
        }
        Err(failure) => {
            let reason = failure.to_string();
            write_status(
                output,
                &AdapterStatus::failed("run", reason.clone(), started_at, now()),
            )?;
            Err(failure)
        }
    }
}

/// The measured path: parse, decline or accept, translate, produce, seal the
/// result document.
fn execute(experiment_path: &Path, output: &Path) -> Result<producer::RunOutcome, Box<dyn Error>> {
    let experiment = read_experiment(experiment_path)?;
    let subject = subject_of(&experiment, Some(output))?.name.clone();
    let verdict = report(&experiment, Some(&subject));
    if !verdict.supported {
        return Err(format!(
            "this adapter declined the experiment: {}",
            verdict.reasons.join("; ")
        )
        .into());
    }
    let outcome = match experiment.load_mode {
        LoadMode::ClosedLoop => producer::run(&produce_arguments(&experiment, &subject, output)?),
        LoadMode::ScheduledOpenLoopFixedRate => {
            producer::run_fixed(&fixed_arguments(&experiment, &subject, output)?)
        }
    }?;
    std::fs::write(output.join(RESULT_FILE), format!("{}\n", outcome.json))?;
    Ok(outcome)
}

/// Returns the capability document for this adapter.
///
/// Every claim here is checked against `producer.rs`: the client is built with
/// idempotence and `acks=all`, it never negotiates TLS, it has no transactional
/// call, and it compresses nothing. The version is the adapter's own, because
/// the adapter and the client it wraps are versioned together in this
/// repository's sibling layout.
pub(crate) fn description() -> AdapterDescription {
    let mut result_schemas = std::collections::BTreeMap::new();
    result_schemas.insert(LoadMode::ClosedLoop, CLOSED_LOOP_RESULT_SCHEMA.to_owned());
    result_schemas.insert(
        LoadMode::ScheduledOpenLoopFixedRate,
        FIXED_RATE_RESULT_SCHEMA.to_owned(),
    );
    AdapterDescription {
        schema: AdapterDescription::SCHEMA.to_owned(),
        name: ADAPTER_NAME.to_owned(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        capabilities: AdapterCapabilities {
            producer: true,
            consumer: false,
            idempotence: true,
            transactions: false,
            tls: false,
            compression: vec!["none".to_owned()],
            completion_modes: vec!["aggregate-batch-terminal".to_owned()],
            // Records are handed to the client as owned `Bytes`, so admission
            // moves the buffer rather than copying it.
            ownership_modes: vec!["owned-handoff".to_owned()],
            metric_families: vec![
                "latency".to_owned(),
                "throughput".to_owned(),
                "native-metrics".to_owned(),
            ],
        },
        result_schemas: Some(result_schemas),
    }
}

/// Judges a resolved experiment against what this adapter configures.
pub(crate) fn report(experiment: &ResolvedExperiment, subject: Option<&str>) -> ValidateReport {
    let mut reasons = Vec::new();
    if let Err(reason) = experiment.validate() {
        return ValidateReport::unsupported(vec![format!(
            "the experiment is not coherent: {reason}"
        )]);
    }
    check_shape(experiment, &mut reasons);
    check_producer(experiment, &mut reasons);
    check_runtime(experiment, subject, &mut reasons);
    if reasons.is_empty() {
        ValidateReport::supported()
    } else {
        ValidateReport::unsupported(reasons)
    }
}

/// Checks the experiment's kind, load mode, and application shape.
fn check_shape(experiment: &ResolvedExperiment, reasons: &mut Vec<String>) {
    match experiment.kind {
        ExperimentKind::Producer => {}
    }
    if experiment.payload.bytes < MIN_PAYLOAD_BYTES {
        reasons.push(format!(
            "a record carries a {MIN_PAYLOAD_BYTES}-byte identity envelope, and the \
             experiment asks for {} payload bytes",
            experiment.payload.bytes
        ));
    }
    if i32::try_from(experiment.cluster.partitions).is_err() {
        reasons.push(format!(
            "partitions are addressed as a signed 32-bit integer, and the experiment asks \
             for {}",
            experiment.cluster.partitions
        ));
    }
    if experiment.application.producer_instances != 1 {
        reasons.push(format!(
            "this adapter builds exactly one producer handle, and the experiment asks for {}",
            experiment.application.producer_instances
        ));
    }
    if experiment.application.admission_shape != "public-batch" {
        reasons.push(format!(
            "this adapter admits records through the public batch surface, not {:?}",
            experiment.application.admission_shape
        ));
    }
    if experiment.application.completion_shape != "aggregate-batch-terminal" {
        reasons.push(format!(
            "this adapter waits on the aggregate terminal state of each admitted batch, \
             not {:?}",
            experiment.application.completion_shape
        ));
    }
    if experiment.application.queue_bytes != QUEUE_BYTES {
        reasons.push(format!(
            "this adapter retains {QUEUE_BYTES} client queue bytes, and the experiment \
             asks for {}",
            experiment.application.queue_bytes
        ));
    }
    match experiment.load_mode {
        LoadMode::ClosedLoop => {
            if experiment.application.callers_per_producer != CLOSED_LOOP_CALLERS {
                reasons.push(format!(
                    "the closed-loop phase admits from exactly {CLOSED_LOOP_CALLERS} caller, \
                     and the experiment asks for {}",
                    experiment.application.callers_per_producer
                ));
            }
        }
        LoadMode::ScheduledOpenLoopFixedRate => {
            if experiment.application.callers_per_producer != FIXED_RATE_CALLERS {
                reasons.push(format!(
                    "the fixed-load headline requires exactly {FIXED_RATE_CALLERS} callers, \
                     and the experiment asks for {}",
                    experiment.application.callers_per_producer
                ));
            }
            if experiment.arrival != Some(ArrivalModel::Deterministic) {
                reasons.push(
                    "this adapter schedules evenly spaced arrivals and implements no other \
                     arrival process"
                        .to_owned(),
                );
            }
        }
    }
}

/// Checks the client settings `producer.rs` fixes at compile time.
fn check_producer(experiment: &ResolvedExperiment, reasons: &mut Vec<String>) {
    let Some(producer) = experiment.producer.as_ref() else {
        return;
    };
    let mut fixed = |field: &str, wanted: String, configured: &str| {
        if wanted != configured {
            reasons.push(format!(
                "this adapter configures {field}={configured} and offers no way to change \
                 it; the experiment asks for {wanted}"
            ));
        }
    };
    fixed("acks", producer.acks.clone(), "all");
    fixed("idempotence", producer.idempotence.to_string(), "true");
    fixed("compression", producer.compression.clone(), "none");
    fixed(
        "linger_ms",
        producer.linger_ms.to_string(),
        &LINGER_MS.to_string(),
    );
    fixed(
        "batch_records",
        producer.batch_records.to_string(),
        &BATCH_RECORDS.to_string(),
    );
    fixed(
        "batch_bytes",
        producer.batch_bytes.to_string(),
        &BATCH_BYTES.to_string(),
    );
    fixed(
        "request_bytes",
        producer.request_bytes.to_string(),
        &REQUEST_BYTES.to_string(),
    );
    fixed(
        "delivery_timeout_ms",
        producer.delivery_timeout_ms.to_string(),
        &DELIVERY_TIMEOUT_MS.to_string(),
    );
    fixed(
        "max_in_flight_requests_per_broker",
        producer.max_in_flight_requests_per_broker.to_string(),
        &MAX_IN_FLIGHT_REQUESTS_PER_BROKER.to_string(),
    );
    fixed(
        "retry_max_replacements",
        producer.retry_max_replacements.to_string(),
        &RETRY_MAX_REPLACEMENTS.to_string(),
    );
    fixed(
        "retry_backoff_ms",
        producer.retry_backoff_ms.to_string(),
        &RETRY_BACKOFF_MS.to_string(),
    );
    for (field, partitioning) in [
        ("partitioning", Some(&producer.partitioning)),
        ("warmup_partitioning", producer.warmup_partitioning.as_ref()),
    ] {
        if let Some(partitioning) = partitioning
            && partitioning != "explicit-round-robin"
        {
            reasons.push(format!(
                "this adapter assigns partitions itself, round robin by record sequence; \
                 the experiment asks for {field}={partitioning:?}"
            ));
        }
    }
    if let Some(primer) = producer.warmup_serialized_partition_primer_records
        && primer != experiment.cluster.partitions
    {
        reasons.push(format!(
            "the warmup primes exactly one record per partition, which is {} here, and the \
             experiment asks for {primer}",
            experiment.cluster.partitions
        ));
    }
}

/// Checks that the binding names topics this subject can produce to.
fn check_runtime(
    experiment: &ResolvedExperiment,
    subject: Option<&str>,
    reasons: &mut Vec<String>,
) {
    let Some(runtime) = experiment.runtime.as_ref() else {
        reasons.push(
            "the resolved experiment carries no runtime binding, so it names no topics and \
             no run id to produce under"
                .to_owned(),
        );
        return;
    };
    if let Some(subject) = subject
        && !runtime.topics.contains_key(subject)
    {
        reasons.push(format!(
            "the runtime binding names no topics for subject {subject:?}"
        ));
    }
}

/// Decides which subject of the experiment this process is running as.
pub(crate) fn subject_of<'a>(
    experiment: &'a ResolvedExperiment,
    output: Option<&Path>,
) -> Result<&'a SubjectSpec, Box<dyn Error>> {
    if let Some(name) = output
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        && let Some(subject) = experiment.subject(name)
    {
        return Ok(subject);
    }
    let mut ours = experiment
        .subjects
        .iter()
        .filter(|subject| subject.adapter_name == ADAPTER_NAME);
    match (ours.next(), ours.next()) {
        (Some(subject), None) => Ok(subject),
        (Some(_), Some(_)) => Err(format!(
            "the experiment has more than one {ADAPTER_NAME} subject and the output \
             directory does not say which one this is"
        )
        .into()),
        _ => Err(format!(
            "the experiment has no subject this adapter could be: no {ADAPTER_NAME} subject, \
             and no output directory naming one"
        )
        .into()),
    }
}

/// Maps a resolved experiment onto the legacy closed-loop argument struct.
pub(crate) fn produce_arguments(
    experiment: &ResolvedExperiment,
    subject: &str,
    output: &Path,
) -> Result<ProduceArgs, Box<dyn Error>> {
    let runtime = experiment
        .runtime
        .as_ref()
        .ok_or("the resolved experiment carries no runtime binding")?;
    let topics = runtime
        .topics
        .get(subject)
        .ok_or_else(|| format!("the resolved experiment has no topics for subject {subject:?}"))?;
    Ok(ProduceArgs {
        bootstrap: runtime.bootstrap.clone(),
        warmup_topic: topics.warmup.clone(),
        topic: topics.measured.clone(),
        run_id: runtime.run_id.clone(),
        warmup_records: usize::try_from(experiment.warmup_records)?,
        records: usize::try_from(experiment.records)?,
        payload_bytes: usize::try_from(experiment.payload.bytes)?,
        partitions: i32::try_from(experiment.cluster.partitions)?,
        max_outstanding: usize::try_from(experiment.application.max_outstanding_records)?,
        latency_path: latency_path(output),
    })
}

/// Maps a resolved experiment onto the legacy fixed-rate argument struct.
pub(crate) fn fixed_arguments(
    experiment: &ResolvedExperiment,
    subject: &str,
    output: &Path,
) -> Result<FixedProduceArgs, Box<dyn Error>> {
    let common = produce_arguments(experiment, subject, output)?;
    let offered_records_per_second = experiment
        .offered_records_per_second
        .ok_or("a fixed-rate experiment states no offered rate")?;
    Ok(FixedProduceArgs {
        common,
        offered_records_per_second,
        callers: usize::try_from(experiment.application.callers_per_producer)?,
    })
}

/// Where the per-record latency evidence goes.
pub(crate) fn latency_path(output: &Path) -> PathBuf {
    output.join(LATENCY_FILE)
}

/// Reads and parses a resolved experiment.
fn read_experiment(path: &Path) -> Result<ResolvedExperiment, Box<dyn Error>> {
    let bytes = std::fs::read(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    Ok(bench_schema::parse_json_slice(&bytes)
        .map_err(|error| format!("{} is not a resolved experiment: {error}", path.display()))?)
}

/// Writes the adapter status document.
fn write_status(output: &Path, status: &AdapterStatus) -> Result<(), Box<dyn Error>> {
    std::fs::write(output.join(STATUS_FILE), pretty_bytes(status)?)?;
    Ok(())
}

/// Prints a protocol document as pretty JSON on stdout.
fn print_document<T: serde::Serialize>(document: &T) -> Result<(), Box<dyn Error>> {
    use std::io::Write;
    std::io::stdout().write_all(&pretty_bytes(document)?)?;
    Ok(())
}

/// Returns the current time in the shape the status document wants.
fn now() -> String {
    utc_rfc3339_millis(SystemTime::now())
}

/// Formats a system time as `2026-08-12T14:03:05.123Z`.
///
/// Duplicated from the control plane on purpose: an adapter that imported
/// `benchctl` to print a timestamp would couple the thing being measured to the
/// thing measuring it, and this adapter is deliberately buildable without the
/// control plane in the graph at all.
pub(crate) fn utc_rfc3339_millis(time: SystemTime) -> String {
    let since_epoch = time.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    let total_seconds = i64::try_from(since_epoch.as_secs()).unwrap_or(i64::MAX);
    let days = total_seconds.div_euclid(86_400);
    let seconds_of_day = total_seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day / 60) % 60;
    let second = seconds_of_day % 60;
    let millisecond = since_epoch.subsec_millis();
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millisecond:03}Z")
}

/// Splits days-since-epoch into a civil (year, month, day).
fn civil_from_days(days: i64) -> (i64, u8, u8) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_position = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_position + 2) / 5 + 1;
    let month = if month_position < 10 {
        month_position + 3
    } else {
        month_position - 9
    };
    let year = if month <= 2 { year + 1 } else { year };
    (
        year,
        u8::try_from(month).unwrap_or(1),
        u8::try_from(day).unwrap_or(1),
    )
}
