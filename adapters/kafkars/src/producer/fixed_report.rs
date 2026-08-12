//! Stable normalized report for one scheduled fixed-load producer phase.

use std::error::Error;

use serde::Serialize;

use crate::{
    arguments::FixedProduceArgs,
    report::{LatencyReport, NativeMetrics, ProducerSettings},
    schedule,
};

use super::{
    BATCH_BYTES, BATCH_RECORDS, MAX_IN_FLIGHT_REQUESTS_PER_BROKER, MAX_RETRIES, QUEUE_BYTES,
    REQUEST_BYTES, RETRY_BACKOFF, fixed_phase::FixedPhaseResult,
};

#[derive(Debug, Serialize)]
pub(super) struct FixedProducerReport {
    schema: &'static str,
    adapter: &'static str,
    adapter_version: &'static str,
    run_id: String,
    topic: String,
    offered_records: usize,
    accepted_records: usize,
    acknowledged_records: usize,
    failed_records: usize,
    payload_bytes: usize,
    acknowledged_payload_bytes: u64,
    load: FixedLoad,
    acknowledged_records_per_second_including_drain: f64,
    latency_ns: FixedLatencyReport,
    settings: ProducerSettings,
    native_metrics: NativeMetrics,
    pub(super) valid: bool,
}

#[derive(Debug, Serialize)]
struct FixedLoad {
    mode: &'static str,
    callers_per_producer: usize,
    offered_records_per_second: u64,
    schedule_span_ns: u64,
    drain_duration_ns: u128,
    admission_pressure_records: usize,
}

#[derive(Debug, Serialize)]
struct FixedLatencyReport {
    uncorrected: LatencyReport,
    corrected: LatencyReport,
    schedule_delay: LatencyReport,
}

pub(super) fn build(
    arguments: &FixedProduceArgs,
    phase: &mut FixedPhaseResult,
    native_metrics: NativeMetrics,
) -> Result<FixedProducerReport, Box<dyn Error>> {
    let common = &arguments.common;
    let acknowledged_bytes = u64::try_from(phase.acknowledged)?
        .checked_mul(u64::try_from(common.payload_bytes)?)
        .ok_or("acknowledged payload bytes overflowed")?;
    let valid = phase.offered == common.records
        && phase.accepted == common.records
        && phase.acknowledged == common.records
        && phase.failed == 0;
    let acknowledged = f64::from(u32::try_from(phase.acknowledged)?);
    let (uncorrected, corrected, schedule_delay) = phase.latency_reports();
    Ok(FixedProducerReport {
        schema: "kafkars.producer-fixed-load.v1",
        adapter: "kafkars",
        adapter_version: env!("CARGO_PKG_VERSION"),
        run_id: common.run_id.clone(),
        topic: common.topic.clone(),
        offered_records: phase.offered,
        accepted_records: phase.accepted,
        acknowledged_records: phase.acknowledged,
        failed_records: phase.failed,
        payload_bytes: common.payload_bytes,
        acknowledged_payload_bytes: acknowledged_bytes,
        load: FixedLoad {
            mode: "scheduled-open-loop-fixed-rate",
            callers_per_producer: arguments.callers,
            offered_records_per_second: arguments.offered_records_per_second,
            schedule_span_ns: schedule::intended_offset_ns(
                u64::try_from(common.records - 1)?,
                arguments.offered_records_per_second,
            )?,
            drain_duration_ns: phase.duration.as_nanos(),
            admission_pressure_records: phase.offered.saturating_sub(phase.accepted),
        },
        acknowledged_records_per_second_including_drain: acknowledged
            / phase.duration.as_secs_f64(),
        latency_ns: FixedLatencyReport {
            uncorrected,
            corrected,
            schedule_delay,
        },
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
            max_outstanding_records: common.max_outstanding,
            retry_max_replacements: MAX_RETRIES,
            retry_backoff_ms: u64::try_from(RETRY_BACKOFF.as_millis())?,
            explicit_balanced_partitioning: true,
            admission_shape: "public-batch",
            completion_shape: "aggregate-batch-terminal",
        },
        native_metrics,
        valid,
    })
}
