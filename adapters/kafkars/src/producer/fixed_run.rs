//! Public-client setup, warmup, fixed-rate execution, and complete shutdown.

use std::error::Error;

use kafkars::{Client, ProducerLimits};

use crate::{arguments::FixedProduceArgs, report::NativeMetrics, topics};

use super::{
    BATCH_BYTES, BATCH_RECORDS, DELIVERY_TIMEOUT, FIXED_RATE_INVALID, LINGER,
    MAX_IN_FLIGHT_REQUESTS_PER_BROKER, MAX_RETRIES, QUEUE_BYTES, REQUEST_BYTES, RETRY_BACKOFF,
    RunOutcome, batch_phase::run_batch_phase, fixed_phase, fixed_report, phase::PhaseSpec,
};

const WAITING_BYTES: usize = BATCH_BYTES;

pub(super) fn run(arguments: &FixedProduceArgs) -> Result<RunOutcome, Box<dyn Error>> {
    let common = &arguments.common;
    let limits = ProducerLimits::default()
        .with_retained_bytes(QUEUE_BYTES)
        .with_in_flight_records(common.max_outstanding)
        .with_waiting_records(common.max_outstanding)
        .with_waiting_bytes(WAITING_BYTES)
        .with_batch_records(BATCH_RECORDS)
        .with_batch_bytes(BATCH_BYTES)
        .with_request_bytes(REQUEST_BYTES)
        .with_max_in_flight_requests_per_broker(MAX_IN_FLIGHT_REQUESTS_PER_BROKER)
        .with_linger(LINGER);
    let client = Client::builder()
        .bootstrap_servers(common.bootstrap.split(',').map(str::to_owned))
        .client_id("kafkars-fixed-load-comparison")
        .producer_limits(limits)
        .producer_retry(MAX_RETRIES, RETRY_BACKOFF)
        .producer_delivery_timeout(DELIVERY_TIMEOUT)
        .build()?;
    client.ready().wait()?;
    topics::await_ready(
        &client,
        &[&common.warmup_topic, &common.topic],
        common.partitions,
        3,
    )?;
    let producer = client.producer().build()?;

    if common.warmup_records > 0 {
        let warmup = run_batch_phase(
            &producer,
            PhaseSpec {
                topic: &common.warmup_topic,
                run_id: &common.run_id,
                records: common.warmup_records,
                payload_bytes: common.payload_bytes,
                partitions: common.partitions,
                max_outstanding: common.max_outstanding,
                prime_partitions: true,
            },
        )?;
        super::flush(&producer)?;
        if !super::phase_is_valid(&warmup, common.warmup_records) {
            super::close(&producer)?;
            client.shutdown().wait()?;
            return Err("fixed-load warmup failed its exact terminal contract".into());
        }
    }
    let spec = fixed_phase::FixedPhaseSpec {
        phase: PhaseSpec {
            topic: &common.topic,
            run_id: &common.run_id,
            records: common.records,
            payload_bytes: common.payload_bytes,
            partitions: common.partitions,
            max_outstanding: common.max_outstanding,
            prime_partitions: false,
        },
        offered_records_per_second: arguments.offered_records_per_second,
        callers: arguments.callers,
    };
    let before = super::metrics(&client)?;
    let mut phase = fixed_phase::run_fixed_phase(&producer, spec)?;
    super::flush(&producer)?;
    let after = super::metrics(&client)?;
    fixed_phase::write_latencies(&common.latency_path, &phase.samples)?;
    let native_metrics = NativeMetrics::between(&before, &after, phase.batch_admission);
    let report = fixed_report::build(arguments, &mut phase, native_metrics)?;
    let outcome = RunOutcome::render(&report, report.valid, FIXED_RATE_INVALID)?;

    super::close(&producer)?;
    client.shutdown().wait()?;
    Ok(outcome)
}
