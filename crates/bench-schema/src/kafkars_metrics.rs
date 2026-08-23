//! `kafkars.kafkars-native-metrics.v1`: exact public producer counters and
//! boundary gauges captured around one measured phase.
//!
//! The two snapshots state what Kafkars exposed immediately before and after
//! measurement. `delta` is redundant by design: it makes request economics
//! directly readable while validation proves it is exactly `final - baseline`.
//! Counters that Kafkars does not expose — total wire bytes, retries, and
//! request timeouts — do not appear, so readers cannot mistake absence for zero.

use serde::{Deserialize, Serialize};

use crate::error::{SchemaError, SchemaResult};
use crate::require_schema;
use crate::schema_id::KAFKARS_NATIVE_METRICS_V1;

/// Producer gauges and cumulative counters at one public metrics boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkarsProducerMetricsSnapshot {
    /// Application records retained by active producer ownership.
    pub active_records: u64,
    /// Application bytes retained by active producer ownership.
    pub active_bytes: u64,
    /// Records retained in bounded FIFO waiting ownership.
    pub waiting_records: u64,
    /// Application bytes retained in bounded FIFO waiting ownership.
    pub waiting_bytes: u64,
    /// Protocol-materialized batches not yet released by execution.
    pub prepared_batches: u64,
    /// Encoded record-batch bytes retained by prepared execution.
    pub prepared_batch_bytes: u64,
    /// Terminal decisions awaiting completion publication.
    pub terminal_backlog: u64,
    /// Cumulative driver-accepted Produce requests.
    pub produce_requests: u64,
    /// Cumulative partition batches in accepted Produce requests.
    pub partition_batches: u64,
    /// Cumulative records in accepted Produce requests.
    pub records: u64,
    /// Cumulative encoded record bytes in accepted Produce requests.
    pub encoded_record_bytes: u64,
    /// Process-lifetime peak Produce requests owned by transport.
    pub peak_in_flight_requests: u64,
    /// Process-lifetime peak Produce requests owned by one broker connection.
    pub peak_in_flight_requests_per_broker: u64,
    /// Whether the producer accepted records at this boundary.
    pub accepting: bool,
    /// Whether the producer host retained healthy ownership at this boundary.
    pub healthy: bool,
}

/// Exact cumulative-counter movement across the measured phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkarsProducerMetricsDelta {
    /// Driver-accepted Produce requests during measurement.
    pub produce_requests: u64,
    /// Partition batches in those requests.
    pub partition_batches: u64,
    /// Records in those requests.
    pub records: u64,
    /// Encoded record bytes in those requests.
    pub encoded_record_bytes: u64,
}

/// Native Kafkars metrics sealed next to one v2 producer result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkarsNativeMetrics {
    /// Always [`KAFKARS_NATIVE_METRICS_V1`].
    pub schema: String,
    /// Snapshot after warmup and immediately before measurement.
    pub baseline: KafkarsProducerMetricsSnapshot,
    /// Snapshot immediately after the measured phase drained.
    #[serde(rename = "final")]
    pub final_snapshot: KafkarsProducerMetricsSnapshot,
    /// Exact cumulative-counter movement between the boundaries.
    pub delta: KafkarsProducerMetricsDelta,
}

impl KafkarsNativeMetrics {
    /// The schema id of this document.
    pub const SCHEMA: &'static str = KAFKARS_NATIVE_METRICS_V1;

    /// Builds and validates one measured window from its boundary snapshots.
    ///
    /// # Errors
    ///
    /// Returns an error if a cumulative counter moved backwards.
    pub fn between(
        baseline: KafkarsProducerMetricsSnapshot,
        final_snapshot: KafkarsProducerMetricsSnapshot,
    ) -> SchemaResult<Self> {
        let document = Self {
            schema: Self::SCHEMA.to_owned(),
            baseline,
            final_snapshot,
            delta: KafkarsProducerMetricsDelta::between(baseline, final_snapshot)?,
        };
        document.validate()?;
        Ok(document)
    }

    /// Parses and validates one native-metrics document.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid JSON, the wrong schema id, backwards
    /// counters, or a delta that does not match its boundary snapshots.
    pub fn from_slice(bytes: &[u8]) -> SchemaResult<Self> {
        let document: Self = serde_json::from_slice(bytes)
            .map_err(|error| SchemaError::parse(format!("Kafkars native metrics: {error}")))?;
        document.validate()?;
        Ok(document)
    }

    /// Checks the schema id, counter monotonicity, peaks, and redundant delta.
    ///
    /// # Errors
    ///
    /// Returns an error naming the first inconsistent field.
    pub fn validate(&self) -> SchemaResult<()> {
        require_schema(&self.schema, Self::SCHEMA)?;
        let expected = KafkarsProducerMetricsDelta::between(self.baseline, self.final_snapshot)?;
        if self.delta != expected {
            return Err(SchemaError::invalid_field(
                "delta",
                "must equal final cumulative counters minus baseline counters",
            ));
        }
        monotonic(
            "final.peak_in_flight_requests",
            self.baseline.peak_in_flight_requests,
            self.final_snapshot.peak_in_flight_requests,
        )?;
        monotonic(
            "final.peak_in_flight_requests_per_broker",
            self.baseline.peak_in_flight_requests_per_broker,
            self.final_snapshot.peak_in_flight_requests_per_broker,
        )?;
        Ok(())
    }
}

impl KafkarsProducerMetricsDelta {
    fn between(
        baseline: KafkarsProducerMetricsSnapshot,
        final_snapshot: KafkarsProducerMetricsSnapshot,
    ) -> SchemaResult<Self> {
        Ok(Self {
            produce_requests: delta(
                "final.produce_requests",
                baseline.produce_requests,
                final_snapshot.produce_requests,
            )?,
            partition_batches: delta(
                "final.partition_batches",
                baseline.partition_batches,
                final_snapshot.partition_batches,
            )?,
            records: delta("final.records", baseline.records, final_snapshot.records)?,
            encoded_record_bytes: delta(
                "final.encoded_record_bytes",
                baseline.encoded_record_bytes,
                final_snapshot.encoded_record_bytes,
            )?,
        })
    }
}

fn delta(field: &str, baseline: u64, final_value: u64) -> SchemaResult<u64> {
    final_value.checked_sub(baseline).ok_or_else(|| {
        SchemaError::invalid_field(field, "cumulative counter moved backwards from baseline")
    })
}

fn monotonic(field: &str, baseline: u64, final_value: u64) -> SchemaResult<()> {
    if final_value < baseline {
        return Err(SchemaError::invalid_field(
            field,
            "process-lifetime peak moved backwards from baseline",
        ));
    }
    Ok(())
}
