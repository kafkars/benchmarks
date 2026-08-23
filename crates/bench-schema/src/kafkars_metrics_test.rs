//! Tests for the Kafkars native metrics window and its redundant delta.
#![expect(clippy::unwrap_used, reason = "exact fixtures should fail immediately")]

use crate::{
    KafkarsNativeMetrics, KafkarsProducerMetricsDelta, KafkarsProducerMetricsSnapshot,
    SchemaErrorKind,
};

fn snapshot(offset: u64) -> KafkarsProducerMetricsSnapshot {
    KafkarsProducerMetricsSnapshot {
        active_records: offset,
        active_bytes: offset * 100,
        waiting_records: offset,
        waiting_bytes: offset * 100,
        prepared_batches: offset,
        prepared_batch_bytes: offset * 200,
        terminal_backlog: offset,
        produce_requests: 10 + offset,
        partition_batches: 20 + offset,
        records: 100 + offset,
        encoded_record_bytes: 1_000 + offset,
        peak_in_flight_requests: 5 + offset,
        peak_in_flight_requests_per_broker: 2 + offset,
        accepting: true,
        healthy: true,
    }
}

#[test]
fn boundary_snapshots_round_trip_with_exact_deltas() {
    let document = KafkarsNativeMetrics::between(snapshot(0), snapshot(5)).unwrap();

    assert_eq!(
        document.delta,
        KafkarsProducerMetricsDelta {
            produce_requests: 5,
            partition_batches: 5,
            records: 5,
            encoded_record_bytes: 5,
        }
    );
    let bytes = serde_json::to_vec(&document).unwrap();
    let parsed = KafkarsNativeMetrics::from_slice(&bytes).unwrap();
    assert_eq!(parsed, document);
    assert!(String::from_utf8(bytes).unwrap().contains("\"final\""));
}

#[test]
fn a_counter_that_moves_backwards_is_rejected() {
    let error = KafkarsNativeMetrics::between(snapshot(5), snapshot(0)).unwrap_err();

    assert_eq!(error.kind(), SchemaErrorKind::InvalidField);
    assert!(error.context().contains("produce_requests"), "{error}");
}

#[test]
fn a_delta_that_disagrees_with_the_snapshots_is_rejected() {
    let mut document = KafkarsNativeMetrics::between(snapshot(0), snapshot(5)).unwrap();
    document.delta.records = 99;

    let error = document.validate().unwrap_err();

    assert_eq!(error.kind(), SchemaErrorKind::InvalidField);
    assert!(error.context().contains("delta"), "{error}");
}

#[test]
fn the_schema_id_is_fail_closed() {
    let mut document = KafkarsNativeMetrics::between(snapshot(0), snapshot(5)).unwrap();
    document.schema = "kafkars.kafkars-native-metrics.v2".to_owned();

    assert_eq!(
        document.validate().unwrap_err().kind(),
        SchemaErrorKind::WrongSchema
    );
}
