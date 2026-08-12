//! Bounded completion-driven execution through the public `kafkars` producer.
//!
//! # Two measured paths, one client
//!
//! The phases here are the legacy ones. They return their rendered report
//! rather than printing it, and the legacy command arms print exactly those
//! bytes — `kafkars.producer-benchmark.v1` and `kafkars.producer-fixed-load.v1`
//! — because a control plane's sealed evidence cannot be redefined after the
//! fact.
//!
//! [`v2`] is the second path, used only by the adapter protocol's `run` verb.
//! It measures the same workload with an admission clock that survives
//! queue-full retries and evidence that does not grow with the run, which is a
//! change in what is measured rather than in how it is printed, and so is a new
//! schema id rather than an edit to these.
//!
//! What the two must never differ in is the client they measure. Every
//! constant below is shared, and [`session`] applies them in one place, so a
//! change to the batching, the retry policy, or the queue budget reaches both
//! paths together or not at all.

mod batch_phase;
mod fixed_phase;
mod fixed_report;
mod fixed_run;
mod phase;
mod session;
mod turn;
#[cfg(test)]
mod turn_test;
mod v2;

use std::{
    error::Error,
    thread,
    time::{Duration, Instant},
};

use kafkars::{Client, ErrorKind, MetricsSnapshot, Producer};
use serde::Serialize;

use crate::{
    arguments::{FixedProduceArgs, ProduceArgs},
    report::{NativeMetrics, ProducerReport, ProducerSettings, latencies},
};

use self::{
    batch_phase::run_batch_phase,
    phase::{PhaseResult, PhaseSpec, run_phase, write_latencies},
    session::{CLOSED_LOOP_WAITING_BYTES, Session, SessionSpec},
};

pub(crate) use self::v2::{
    V2_COMPLETION_MODE, V2_OWNERSHIP, run_closed_loop_v2, run_fixed_rate_v2,
};

/// Client id the closed-loop phases identify themselves to the broker with.
const CLOSED_LOOP_CLIENT_ID: &str = "kafkars-raw-comparison";

/// Client id the fixed-rate phases identify themselves to the broker with.
const FIXED_RATE_CLIENT_ID: &str = "kafkars-fixed-load-comparison";

const QUEUE_BYTES: usize = 64 * 1024 * 1024;
const BATCH_RECORDS: usize = 256;
const BATCH_BYTES: usize = 65_536;
const REQUEST_BYTES: usize = 1024 * 1024;
const MAX_IN_FLIGHT_REQUESTS_PER_BROKER: usize = 5;
const LINGER: Duration = Duration::from_millis(5);
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(60);
const COMPLETION_TIMEOUT: Duration = Duration::from_secs(65);
const MAX_RETRIES: u32 = 600;
const RETRY_BACKOFF: Duration = Duration::from_millis(100);

/// One completed legacy phase: the exact report bytes, and whether the phase
/// held its terminal contract.
///
/// The bytes are carried rather than the report struct because the two report
/// shapes differ between load modes while the caller does not care. The v2
/// path does not use this type: it carries a typed
/// [`bench_schema::ProducerBenchmarkV2`] and serializes it once, at the write
/// site.
#[derive(Debug)]
pub(crate) struct RunOutcome {
    /// The report as a single JSON line, with no trailing newline.
    pub(crate) json: String,
    /// Whether every offered record reached a terminal, acknowledged state.
    pub(crate) valid: bool,
    /// The failure message the legacy surface reports when `valid` is false.
    pub(crate) invalid_reason: &'static str,
}

impl RunOutcome {
    /// Renders a report exactly as the legacy stdout arm always has.
    ///
    /// This is the single serialization site for the legacy result documents,
    /// and the bytes it produces are frozen: a control plane's sealed evidence
    /// was recorded from them.
    pub(crate) fn render<T: Serialize>(
        report: &T,
        valid: bool,
        invalid_reason: &'static str,
    ) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            json: serde_json::to_string(report)?,
            valid,
            invalid_reason,
        })
    }
}

/// Message the closed-loop phase fails with when it did not fully drain.
pub(crate) const CLOSED_LOOP_INVALID: &str =
    "producer phase did not acknowledge every accepted record";

/// Message the fixed-rate phase fails with when it did not fully settle.
pub(crate) const FIXED_RATE_INVALID: &str =
    "fixed-load phase did not settle every scheduled record";

pub(crate) fn run(arguments: &ProduceArgs) -> Result<RunOutcome, Box<dyn Error>> {
    let session = Session::open(SessionSpec {
        bootstrap: &arguments.bootstrap,
        client_id: CLOSED_LOOP_CLIENT_ID,
        topics: [&arguments.warmup_topic, &arguments.topic],
        partitions: arguments.partitions,
        max_outstanding: arguments.max_outstanding,
        waiting_bytes: CLOSED_LOOP_WAITING_BYTES,
    })?;

    if arguments.warmup_records > 0 {
        let warmup = run_phase(
            &session.producer,
            PhaseSpec {
                topic: &arguments.warmup_topic,
                run_id: &arguments.run_id,
                records: arguments.warmup_records,
                payload_bytes: arguments.payload_bytes,
                partitions: arguments.partitions,
                max_outstanding: arguments.max_outstanding,
                prime_partitions: true,
            },
        )?;
        flush(&session.producer)?;
        if !phase_is_valid(&warmup, arguments.warmup_records) {
            let reason = format!(
                "warmup admitted {}, acknowledged {}, and failed {} of {} records: {}",
                warmup.accepted,
                warmup.acknowledged,
                warmup.failed,
                arguments.warmup_records,
                warmup.failure_details.join("; "),
            );
            session.close()?;
            return Err(reason.into());
        }
    }

    let measured = PhaseSpec {
        topic: &arguments.topic,
        run_id: &arguments.run_id,
        records: arguments.records,
        payload_bytes: arguments.payload_bytes,
        partitions: arguments.partitions,
        max_outstanding: arguments.max_outstanding,
        prime_partitions: false,
    };
    let before = metrics(&session.client)?;
    let mut phase = run_batch_phase(&session.producer, measured)?;
    flush(&session.producer)?;
    let after = metrics(&session.client)?;
    write_latencies(&arguments.latency_path, &phase.samples)?;
    let native_metrics = NativeMetrics::between(&before, &after, phase.batch_admission);
    let report = build_report(arguments, &mut phase, native_metrics)?;
    let outcome = RunOutcome::render(&report, report.valid, CLOSED_LOOP_INVALID)?;

    session.close()?;
    Ok(outcome)
}

pub(crate) fn run_fixed(arguments: &FixedProduceArgs) -> Result<RunOutcome, Box<dyn Error>> {
    fixed_run::run(arguments)
}

fn build_report(
    arguments: &ProduceArgs,
    phase: &mut PhaseResult,
    native_metrics: NativeMetrics,
) -> Result<ProducerReport, Box<dyn Error>> {
    let latency = latencies(&mut phase.latencies);
    let seconds = phase.duration.as_secs_f64();
    let acknowledged_f64 = f64::from(u32::try_from(phase.acknowledged)?);
    let payload_f64 = f64::from(u32::try_from(arguments.payload_bytes)?);
    let acknowledged_bytes = u64::try_from(phase.acknowledged)?
        .checked_mul(u64::try_from(arguments.payload_bytes)?)
        .ok_or("acknowledged payload bytes overflowed")?;
    Ok(ProducerReport {
        schema: crate::protocol::LEGACY_CLOSED_LOOP_RESULT_SCHEMA,
        adapter: "kafkars",
        adapter_version: env!("CARGO_PKG_VERSION"),
        run_id: arguments.run_id.clone(),
        topic: arguments.topic.clone(),
        offered_records: arguments.records,
        accepted_records: phase.accepted,
        acknowledged_records: phase.acknowledged,
        failed_records: phase.failed,
        payload_bytes: arguments.payload_bytes,
        acknowledged_payload_bytes: acknowledged_bytes,
        duration_ns: phase.duration.as_nanos(),
        acknowledged_records_per_second: acknowledged_f64 / seconds,
        acknowledged_mib_per_second: acknowledged_f64 * payload_f64 / seconds / 1_048_576.0,
        latency_ns: latency,
        settings: ProducerSettings {
            acks: "all",
            idempotence: true,
            compression: "none",
            linger_ms: 5,
            batch_records: BATCH_RECORDS,
            batch_bytes: BATCH_BYTES,
            request_bytes: REQUEST_BYTES,
            max_in_flight_requests_per_broker: MAX_IN_FLIGHT_REQUESTS_PER_BROKER,
            queue_bytes: QUEUE_BYTES,
            max_outstanding_records: arguments.max_outstanding,
            retry_max_replacements: MAX_RETRIES,
            retry_backoff_ms: u64::try_from(RETRY_BACKOFF.as_millis())?,
            explicit_balanced_partitioning: true,
            admission_shape: "public-batch",
            completion_shape: "aggregate-batch-terminal",
        },
        native_metrics,
        valid: phase_is_valid(phase, arguments.records),
    })
}

fn phase_is_valid(phase: &PhaseResult, expected: usize) -> bool {
    phase.accepted == expected && phase.acknowledged == expected && phase.failed == 0
}

fn flush(producer: &Producer) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + COMPLETION_TIMEOUT;
    loop {
        match producer.flush().wait() {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == ErrorKind::Backpressure && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(2));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn close(producer: &Producer) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + COMPLETION_TIMEOUT;
    loop {
        match producer.close().wait() {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == ErrorKind::Backpressure && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(2));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn metrics(client: &Client) -> Result<MetricsSnapshot, Box<dyn Error>> {
    let deadline = Instant::now() + COMPLETION_TIMEOUT;
    loop {
        match client.metrics() {
            Ok(observer) => return Ok(observer.wait()?),
            Err(error) if error.kind() == ErrorKind::Backpressure && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(2));
            }
            Err(error) => return Err(error.into()),
        }
    }
}
