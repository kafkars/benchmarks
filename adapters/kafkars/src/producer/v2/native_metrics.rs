//! Mapping the pinned public Kafkars producer snapshot into sealed evidence.

use std::error::Error;

use bench_schema::{KafkarsNativeMetrics, KafkarsProducerMetricsSnapshot};
use kafkars::MetricsSnapshot;

/// Captures the public producer portion of one client metrics snapshot.
pub(super) fn snapshot(
    metrics: &MetricsSnapshot,
) -> Result<KafkarsProducerMetricsSnapshot, Box<dyn Error>> {
    let producer = metrics.producer();
    Ok(KafkarsProducerMetricsSnapshot {
        active_records: u64::try_from(producer.active_records())?,
        active_bytes: u64::try_from(producer.active_bytes())?,
        waiting_records: u64::try_from(producer.waiting_records())?,
        waiting_bytes: u64::try_from(producer.waiting_bytes())?,
        prepared_batches: u64::try_from(producer.prepared_batches())?,
        prepared_batch_bytes: u64::try_from(producer.prepared_batch_bytes())?,
        terminal_backlog: u64::try_from(producer.terminal_backlog())?,
        produce_requests: producer.produce_requests(),
        partition_batches: producer.produce_batches(),
        records: producer.produce_records(),
        encoded_record_bytes: producer.produce_encoded_bytes(),
        peak_in_flight_requests: u64::try_from(producer.peak_produce_in_flight_requests())?,
        peak_in_flight_requests_per_broker: u64::try_from(
            producer.peak_produce_in_flight_requests_per_broker(),
        )?,
        accepting: producer.accepting(),
        healthy: producer.healthy(),
    })
}

/// Builds the validated measured-window document.
pub(super) fn between(
    baseline: KafkarsProducerMetricsSnapshot,
    final_snapshot: KafkarsProducerMetricsSnapshot,
) -> Result<KafkarsNativeMetrics, Box<dyn Error>> {
    Ok(KafkarsNativeMetrics::between(baseline, final_snapshot)?)
}
