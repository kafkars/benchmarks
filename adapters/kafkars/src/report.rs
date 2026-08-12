//! Normalized producer result and latency records.

use kafkars::MetricsSnapshot;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub(crate) struct ProducerReport {
    pub(crate) schema: &'static str,
    pub(crate) adapter: &'static str,
    pub(crate) adapter_version: &'static str,
    pub(crate) run_id: String,
    pub(crate) topic: String,
    pub(crate) offered_records: usize,
    pub(crate) accepted_records: usize,
    pub(crate) acknowledged_records: usize,
    pub(crate) failed_records: usize,
    pub(crate) payload_bytes: usize,
    pub(crate) acknowledged_payload_bytes: u64,
    pub(crate) duration_ns: u128,
    pub(crate) acknowledged_records_per_second: f64,
    pub(crate) acknowledged_mib_per_second: f64,
    pub(crate) latency_ns: LatencyReport,
    pub(crate) settings: ProducerSettings,
    pub(crate) native_metrics: NativeMetrics,
    pub(crate) valid: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct LatencyReport {
    pub(crate) p50: u128,
    pub(crate) p95: u128,
    pub(crate) p99: u128,
    pub(crate) p999: u128,
    pub(crate) max: u128,
}

#[derive(Debug, Serialize)]
pub(crate) struct ProducerSettings {
    pub(crate) acks: &'static str,
    pub(crate) idempotence: bool,
    pub(crate) compression: &'static str,
    pub(crate) linger_ms: u64,
    pub(crate) batch_records: usize,
    pub(crate) batch_bytes: usize,
    pub(crate) request_bytes: usize,
    pub(crate) max_in_flight_requests_per_broker: usize,
    pub(crate) queue_bytes: usize,
    pub(crate) max_outstanding_records: usize,
    pub(crate) retry_max_replacements: u32,
    pub(crate) retry_backoff_ms: u64,
    pub(crate) explicit_balanced_partitioning: bool,
    pub(crate) admission_shape: &'static str,
    pub(crate) completion_shape: &'static str,
}

#[derive(Debug, Serialize)]
pub(crate) struct NativeMetrics {
    availability: &'static str,
    broker_calls: BrokerCallMetrics,
    broker_call_latency_ns: BrokerCallLatencyMetrics,
    producer_requests: ProducerRequestMetrics,
    application_batch_admission: ApplicationBatchAdmissionMetrics,
}

#[derive(Debug, Serialize)]
struct BrokerCallMetrics {
    admitted: u64,
    succeeded: u64,
    failed: u64,
}

#[derive(Debug, Serialize)]
struct BrokerCallLatencyMetrics {
    samples: u64,
    mailbox_total: u128,
    routing_total: u128,
    preparation_total: u128,
    writer_admission_total: u128,
    in_flight_total: u128,
    end_to_end_total: u128,
}

#[derive(Debug, Serialize)]
struct ProducerRequestMetrics {
    requests: u64,
    partition_batches: u64,
    records: u64,
    encoded_record_bytes: u64,
    peak_in_flight_requests: usize,
    peak_in_flight_requests_per_broker: usize,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub(crate) struct ApplicationBatchAdmissionMetrics {
    calls: u64,
    total_ns: u128,
    max_ns: u128,
    accepted_calls: u64,
    accepted_total_ns: u128,
    accepted_max_ns: u128,
    rejected_calls: u64,
    rejected_total_ns: u128,
    rejected_max_ns: u128,
}

impl ApplicationBatchAdmissionMetrics {
    pub(crate) fn record_accepted(&mut self, elapsed: std::time::Duration) {
        self.record(elapsed, true);
    }

    pub(crate) fn record_rejected(&mut self, elapsed: std::time::Duration) {
        self.record(elapsed, false);
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.calls = self.calls.saturating_add(other.calls);
        self.total_ns = self.total_ns.saturating_add(other.total_ns);
        self.max_ns = self.max_ns.max(other.max_ns);
        self.accepted_calls = self.accepted_calls.saturating_add(other.accepted_calls);
        self.accepted_total_ns = self
            .accepted_total_ns
            .saturating_add(other.accepted_total_ns);
        self.accepted_max_ns = self.accepted_max_ns.max(other.accepted_max_ns);
        self.rejected_calls = self.rejected_calls.saturating_add(other.rejected_calls);
        self.rejected_total_ns = self
            .rejected_total_ns
            .saturating_add(other.rejected_total_ns);
        self.rejected_max_ns = self.rejected_max_ns.max(other.rejected_max_ns);
    }

    fn record(&mut self, elapsed: std::time::Duration, accepted: bool) {
        let elapsed_ns = elapsed.as_nanos();
        self.calls = self.calls.saturating_add(1);
        self.total_ns = self.total_ns.saturating_add(elapsed_ns);
        self.max_ns = self.max_ns.max(elapsed_ns);
        if accepted {
            self.accepted_calls = self.accepted_calls.saturating_add(1);
            self.accepted_total_ns = self.accepted_total_ns.saturating_add(elapsed_ns);
            self.accepted_max_ns = self.accepted_max_ns.max(elapsed_ns);
        } else {
            self.rejected_calls = self.rejected_calls.saturating_add(1);
            self.rejected_total_ns = self.rejected_total_ns.saturating_add(elapsed_ns);
            self.rejected_max_ns = self.rejected_max_ns.max(elapsed_ns);
        }
    }
}

impl NativeMetrics {
    pub(crate) fn between(
        before: &MetricsSnapshot,
        after: &MetricsSnapshot,
        application_batch_admission: ApplicationBatchAdmissionMetrics,
    ) -> Self {
        let before_calls = before.calls();
        let after_calls = after.calls();
        let before_latency = before.latency();
        let after_latency = after.latency();
        let before_producer = before.producer();
        let after_producer = after.producer();
        Self {
            availability: "captured",
            broker_calls: BrokerCallMetrics {
                admitted: after_calls
                    .admitted()
                    .saturating_sub(before_calls.admitted()),
                succeeded: after_calls
                    .succeeded()
                    .saturating_sub(before_calls.succeeded()),
                failed: after_calls.failed().saturating_sub(before_calls.failed()),
            },
            broker_call_latency_ns: BrokerCallLatencyMetrics {
                samples: after_latency
                    .end_to_end()
                    .samples()
                    .saturating_sub(before_latency.end_to_end().samples()),
                mailbox_total: duration_delta(
                    before_latency.mailbox().total(),
                    after_latency.mailbox().total(),
                ),
                routing_total: duration_delta(
                    before_latency.routing().total(),
                    after_latency.routing().total(),
                ),
                preparation_total: duration_delta(
                    before_latency.preparation().total(),
                    after_latency.preparation().total(),
                ),
                writer_admission_total: duration_delta(
                    before_latency.writer_admission().total(),
                    after_latency.writer_admission().total(),
                ),
                in_flight_total: duration_delta(
                    before_latency.in_flight().total(),
                    after_latency.in_flight().total(),
                ),
                end_to_end_total: duration_delta(
                    before_latency.end_to_end().total(),
                    after_latency.end_to_end().total(),
                ),
            },
            producer_requests: ProducerRequestMetrics {
                requests: after_producer
                    .produce_requests()
                    .saturating_sub(before_producer.produce_requests()),
                partition_batches: after_producer
                    .produce_batches()
                    .saturating_sub(before_producer.produce_batches()),
                records: after_producer
                    .produce_records()
                    .saturating_sub(before_producer.produce_records()),
                encoded_record_bytes: after_producer
                    .produce_encoded_bytes()
                    .saturating_sub(before_producer.produce_encoded_bytes()),
                peak_in_flight_requests: after_producer.peak_produce_in_flight_requests(),
                peak_in_flight_requests_per_broker: after_producer
                    .peak_produce_in_flight_requests_per_broker(),
            },
            application_batch_admission,
        }
    }
}

fn duration_delta(before: std::time::Duration, after: std::time::Duration) -> u128 {
    after.saturating_sub(before).as_nanos()
}

pub(crate) fn latencies(values: &mut [u128]) -> LatencyReport {
    values.sort_unstable();
    LatencyReport {
        p50: percentile(values, 500),
        p95: percentile(values, 950),
        p99: percentile(values, 990),
        p999: percentile(values, 999),
        max: values.last().copied().unwrap_or(0),
    }
}

fn percentile(values: &[u128], per_thousand: usize) -> u128 {
    if values.is_empty() {
        return 0;
    }
    let rank = values
        .len()
        .saturating_mul(per_thousand)
        .saturating_add(999)
        / 1_000;
    values[rank.saturating_sub(1).min(values.len() - 1)]
}
