//! Bounded completion-driven execution through the public `kafkars` producer.
//!
//! A phase returns its rendered report rather than printing it. The legacy
//! command arms print exactly the bytes this module renders, and the adapter
//! protocol writes exactly those bytes to `result.json`, so the two surfaces
//! cannot drift: there is one serialization, and both callers use it.

mod batch_phase;
mod fixed_phase;
mod fixed_report;
mod fixed_run;
mod phase;

use std::{
    error::Error,
    thread,
    time::{Duration, Instant},
};

use kafkars::{Client, ErrorKind, MetricsSnapshot, Producer, ProducerLimits};
use serde::Serialize;

use crate::{
    arguments::{FixedProduceArgs, ProduceArgs},
    report::{NativeMetrics, ProducerReport, ProducerSettings, latencies},
    topics,
};

use self::{
    batch_phase::run_batch_phase,
    phase::{PhaseResult, PhaseSpec, run_phase, write_latencies},
};

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

/// One completed phase: the exact report bytes, and whether the phase held its
/// terminal contract.
///
/// The bytes are carried rather than the report struct because the two report
/// shapes differ between load modes while their callers do not care: the legacy
/// arm prints them, the protocol writes them to a file, and neither may
/// re-serialize what the other produced.
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
    /// This is the single serialization site for adapter results. A change here
    /// changes both the legacy stdout bytes and the sealed `result.json` bytes
    /// together, which is the only way they can be guaranteed to agree.
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
    let limits = ProducerLimits::default()
        .with_retained_bytes(QUEUE_BYTES)
        .with_in_flight_records(arguments.max_outstanding)
        .with_waiting_records(arguments.max_outstanding)
        .with_waiting_bytes(QUEUE_BYTES)
        .with_batch_records(BATCH_RECORDS)
        .with_batch_bytes(BATCH_BYTES)
        .with_request_bytes(REQUEST_BYTES)
        .with_max_in_flight_requests_per_broker(MAX_IN_FLIGHT_REQUESTS_PER_BROKER)
        .with_linger(LINGER);
    let client = Client::builder()
        .bootstrap_servers(arguments.bootstrap.split(',').map(str::to_owned))
        .client_id("kafkars-raw-comparison")
        .producer_limits(limits)
        .producer_retry(MAX_RETRIES, RETRY_BACKOFF)
        .producer_delivery_timeout(DELIVERY_TIMEOUT)
        .build()?;
    client.ready().wait()?;
    topics::await_ready(
        &client,
        &[&arguments.warmup_topic, &arguments.topic],
        arguments.partitions,
        3,
    )?;
    let producer = client.producer().build()?;

    if arguments.warmup_records > 0 {
        let warmup = run_phase(
            &producer,
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
        flush(&producer)?;
        if !phase_is_valid(&warmup, arguments.warmup_records) {
            close(&producer)?;
            client.shutdown().wait()?;
            return Err(format!(
                "warmup admitted {}, acknowledged {}, and failed {} of {} records: {}",
                warmup.accepted,
                warmup.acknowledged,
                warmup.failed,
                arguments.warmup_records,
                warmup.failure_details.join("; "),
            )
            .into());
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
    let before = metrics(&client)?;
    let mut phase = run_batch_phase(&producer, measured)?;
    flush(&producer)?;
    let after = metrics(&client)?;
    write_latencies(&arguments.latency_path, &phase.samples)?;
    let native_metrics = NativeMetrics::between(&before, &after, phase.batch_admission);
    let report = build_report(arguments, &mut phase, native_metrics)?;
    let outcome = RunOutcome::render(&report, report.valid, CLOSED_LOOP_INVALID)?;

    close(&producer)?;
    client.shutdown().wait()?;
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
        schema: "kafkars.producer-benchmark.v1",
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
