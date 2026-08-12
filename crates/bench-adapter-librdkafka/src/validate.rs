//! Whether the C program can run this experiment as written.
//!
//! # What is checked, and why only that
//!
//! Two kinds of constraint live in the C sources, and both are checked here.
//!
//! *Argument-surface bounds* come from `config.c`: a payload of at least 64
//! bytes, a positive partition count that fits in an `int32_t`, an outstanding
//! record ceiling of 100 000, a run id of sixteen lowercase hexadecimal
//! characters, and — for the fixed-rate shape — an offered rate no greater than
//! one billion and exactly four callers. Violating one of these makes the child
//! exit non-zero with a message nobody upstream can act on, so it is much
//! better to decline in a document that says which bound was missed.
//!
//! *Configuration the C program hard-codes* comes from `producer.c`, which sets
//! acks, idempotence, compression, linger, batching, retries, in-flight
//! requests, and the delivery timeout unconditionally. An experiment asking for
//! anything else is declined rather than run: a comparison whose subjects
//! disagreed about `acks` is not a comparison, and the argument surface has no
//! way to tell the C program otherwise.
//!
//! Settings the C program does *not* express are deliberately not checked.
//! `request_bytes` is the clearest case: `producer.c` never sets
//! `message.max.bytes`, so this adapter has no basis for either honouring or
//! refusing it, and inventing a refusal would decline experiments the reference
//! client has always run. That asymmetry is real and belongs in the comparison's
//! prose, not in a check that pretends to knowledge.

use bench_schema::{ArrivalModel, ExperimentKind, LoadMode, ResolvedExperiment, ValidateReport};

/// Smallest payload `config.c` accepts (`BENCH_MIN_PAYLOAD_BYTES`).
pub(crate) const MIN_PAYLOAD_BYTES: u32 = 64;

/// Outstanding record ceiling (`BENCH_CLIENT_RECORD_CAPACITY`).
pub(crate) const MAX_OUTSTANDING_RECORDS: u64 = 100_000;

/// Highest offered rate `bench_parse_fixed_config` accepts.
pub(crate) const MAX_OFFERED_RECORDS_PER_SECOND: u64 = 1_000_000_000;

/// Caller count the fixed-rate shape demands, exactly.
pub(crate) const FIXED_RATE_CALLERS: u32 = 4;

/// Caller count the closed-loop shape runs with, always.
pub(crate) const CLOSED_LOOP_CALLERS: u32 = 1;

/// Queue budget the C program configures (`queue.buffering.max.kbytes`).
pub(crate) const QUEUE_BYTES: u64 = 67_108_864;

/// Judges a resolved experiment against the C adapter's real surface.
pub(crate) fn report(experiment: &ResolvedExperiment, subject: Option<&str>) -> ValidateReport {
    let mut reasons = Vec::new();
    if let Err(reason) = experiment.validate() {
        reasons.push(format!("the experiment is not coherent: {reason}"));
        return ValidateReport::unsupported(reasons);
    }
    check_shape(experiment, &mut reasons);
    check_bounds(experiment, &mut reasons);
    check_producer(experiment, &mut reasons);
    check_runtime(experiment, subject, &mut reasons);
    if reasons.is_empty() {
        ValidateReport::supported()
    } else {
        ValidateReport::unsupported(reasons)
    }
}

/// Checks the kind and the load mode.
fn check_shape(experiment: &ResolvedExperiment, reasons: &mut Vec<String>) {
    match experiment.kind {
        ExperimentKind::Producer => {}
    }
    if experiment.load_mode == LoadMode::ClosedLoop
        && experiment.application.callers_per_producer != CLOSED_LOOP_CALLERS
    {
        // `bench_parse_config` sets `callers = 1` and the closed phase admits
        // from a single thread, so a request for more would be honoured by
        // running fewer — silently measuring something else.
        reasons.push(format!(
            "the C adapter's closed-loop phase admits from exactly \
             {CLOSED_LOOP_CALLERS} caller, and the experiment asks for {}",
            experiment.application.callers_per_producer
        ));
    }
    if experiment.load_mode == LoadMode::ScheduledOpenLoopFixedRate {
        if experiment.application.callers_per_producer != FIXED_RATE_CALLERS {
            reasons.push(format!(
                "the fixed-rate shape requires exactly {FIXED_RATE_CALLERS} callers, \
                 and the experiment asks for {}",
                experiment.application.callers_per_producer
            ));
        }
        if experiment.arrival != Some(ArrivalModel::Deterministic) {
            reasons.push(
                "the C adapter schedules evenly spaced arrivals and implements no other \
                 arrival process"
                    .to_owned(),
            );
        }
        if experiment
            .offered_records_per_second
            .is_some_and(|rate| rate > MAX_OFFERED_RECORDS_PER_SECOND)
        {
            reasons.push(format!(
                "the C adapter refuses an offered rate above \
                 {MAX_OFFERED_RECORDS_PER_SECOND} records per second"
            ));
        }
    }
    if experiment.application.producer_instances != 1 {
        reasons.push(format!(
            "the C adapter creates exactly one producer handle, and the experiment asks \
             for {}",
            experiment.application.producer_instances
        ));
    }
    if experiment.application.admission_shape != "public-batch" {
        reasons.push(format!(
            "the C adapter admits records through rd_kafka_produce_batch, which is the \
             public-batch shape, not {:?}",
            experiment.application.admission_shape
        ));
    }
    if experiment.application.completion_shape != "aggregate-batch-terminal" {
        reasons.push(format!(
            "the C adapter waits on delivery reports per produced batch, which is the \
             aggregate-batch-terminal shape, not {:?}",
            experiment.application.completion_shape
        ));
    }
}

/// Checks the numeric bounds `config.c` enforces.
fn check_bounds(experiment: &ResolvedExperiment, reasons: &mut Vec<String>) {
    if experiment.payload.bytes < MIN_PAYLOAD_BYTES {
        reasons.push(format!(
            "the C adapter needs at least {MIN_PAYLOAD_BYTES} payload bytes to carry its \
             record identity, and the experiment asks for {}",
            experiment.payload.bytes
        ));
    }
    if i32::try_from(experiment.cluster.partitions).is_err() {
        reasons.push(format!(
            "the C adapter addresses partitions as a signed 32-bit integer, and the \
             experiment asks for {}",
            experiment.cluster.partitions
        ));
    }
    if experiment.application.max_outstanding_records > MAX_OUTSTANDING_RECORDS {
        reasons.push(format!(
            "the C adapter's queue holds at most {MAX_OUTSTANDING_RECORDS} records, and \
             the experiment asks for {}",
            experiment.application.max_outstanding_records
        ));
    }
    if experiment.application.queue_bytes != QUEUE_BYTES {
        reasons.push(format!(
            "the C adapter configures a {QUEUE_BYTES}-byte client queue, and the \
             experiment asks for {}",
            experiment.application.queue_bytes
        ));
    }
    if let Some(primer) = experiment
        .producer
        .as_ref()
        .and_then(|producer| producer.warmup_serialized_partition_primer_records)
        && primer != experiment.cluster.partitions
    {
        reasons.push(format!(
            "the C adapter primes exactly one record per partition during warmup, which \
             is {} here, and the experiment asks for {primer}",
            experiment.cluster.partitions
        ));
    }
}

/// Checks the client settings `producer.c` fixes at compile time.
fn check_producer(experiment: &ResolvedExperiment, reasons: &mut Vec<String>) {
    let Some(producer) = experiment.producer.as_ref() else {
        return;
    };
    let mut fixed = |field: &str, wanted: String, configured: &str| {
        if wanted != configured {
            reasons.push(format!(
                "the C adapter configures {field}={configured} and cannot be told \
                 otherwise; the experiment asks for {wanted}"
            ));
        }
    };
    fixed("acks", producer.acks.clone(), "all");
    fixed(
        "enable.idempotence",
        producer.idempotence.to_string(),
        "true",
    );
    fixed("compression.type", producer.compression.clone(), "none");
    fixed("linger.ms", producer.linger_ms.to_string(), "5");
    fixed(
        "batch.num.messages",
        producer.batch_records.to_string(),
        "256",
    );
    fixed("batch.size", producer.batch_bytes.to_string(), "65536");
    fixed(
        "message.timeout.ms",
        producer.delivery_timeout_ms.to_string(),
        "60000",
    );
    fixed(
        "max.in.flight.requests.per.connection",
        producer.max_in_flight_requests_per_broker.to_string(),
        "5",
    );
    fixed(
        "message.send.max.retries",
        producer.retry_max_replacements.to_string(),
        "600",
    );
    fixed(
        "retry.backoff.ms",
        producer.retry_backoff_ms.to_string(),
        "100",
    );
    for (field, partitioning) in [
        ("partitioning", Some(&producer.partitioning)),
        ("warmup_partitioning", producer.warmup_partitioning.as_ref()),
    ] {
        if let Some(partitioning) = partitioning
            && partitioning != "explicit-round-robin"
        {
            reasons.push(format!(
                "the C adapter assigns partitions itself, round robin by record sequence; \
                 the experiment asks for {field}={partitioning:?}"
            ));
        }
    }
}

/// Checks that the runtime binding names topics this subject can produce to.
fn check_runtime(
    experiment: &ResolvedExperiment,
    subject: Option<&str>,
    reasons: &mut Vec<String>,
) {
    let Some(runtime) = experiment.runtime.as_ref() else {
        reasons.push(
            "the resolved experiment carries no runtime binding, so it names no topics \
             and no run id to produce under"
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
