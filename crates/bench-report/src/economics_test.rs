//! Tests for the native-statistics normalization, including the cases where the
//! honest answer is that nothing was reported.
#![expect(
    clippy::unwrap_used,
    reason = "test fixtures assert exact outcomes and must fail immediately"
)]

use serde_json::{Value, json};

use crate::economics::{
    STATISTICS_FILE_NAME, read_request_economics, request_economics_from_snapshots,
};
use crate::error::ReportErrorKind;

/// Tolerance for comparing computed statistics against hand-checked values.
const TOLERANCE: f64 = 1e-9;

/// Builds one envelope snapshot with the counters a producer reports.
fn snapshot(phase: &str, produce: u64, records: u64, payload: u64, wire: u64) -> Value {
    json!({
        "schema": "kafkars.librdkafka-statistics.v1",
        "phase": phase,
        "captured_ns": 1_000,
        "statistics": {
            "type": "producer",
            "tx": produce + 3,
            "tx_bytes": wire,
            "rx": produce,
            "rx_bytes": wire / 8,
            "txmsgs": records,
            "txmsg_bytes": payload,
            "brokers": {
                "bootstrap:9092/bootstrap": {"nodeid": -1, "req": {"Metadata": 1}},
                "broker-1:9092/1": {
                    "nodeid": 1,
                    "txretries": 2,
                    "req_timeouts": 1,
                    "req": {"Produce": produce / 2, "Metadata": 1}
                },
                "broker-2:9092/2": {
                    "nodeid": 2,
                    "txretries": 3,
                    "req_timeouts": 0,
                    "req": {"Produce": produce - produce / 2, "ApiVersion": 1}
                }
            },
            "topics": {
                "measured": {
                    "batchcnt": {"cnt": 2, "sum": 400, "min": 100, "max": 300},
                    "batchsize": {"cnt": 2, "sum": 4_000, "min": 1_000, "max": 3_000}
                },
                "warmup": {
                    "batchcnt": {"cnt": 1, "sum": 10, "min": 10, "max": 10},
                    "batchsize": {"cnt": 1, "sum": 100, "min": 100, "max": 100}
                }
            }
        }
    })
}

/// A three-snapshot stream: warmup traffic, then the measured window.
fn stream() -> Vec<Value> {
    vec![
        snapshot("warmup", 100, 1_000, 100_000, 120_000),
        snapshot("baseline", 200, 2_000, 200_000, 240_000),
        snapshot("measured", 600, 6_000, 600_000, 720_000),
        snapshot("final", 1_200, 12_000, 1_200_000, 1_440_000),
    ]
}

#[test]
fn the_statistics_file_name_matches_the_adapter_contract() {
    assert_eq!(STATISTICS_FILE_NAME, "client-metrics.jsonl");
}

#[test]
fn counters_are_differenced_across_the_measured_window_only() {
    let economics = request_economics_from_snapshots(&stream(), 10_000, Some("measured")).unwrap();

    // baseline 200 produce requests, final 1200: the 100 from warmup are gone.
    assert_eq!(economics.produce_requests, Some(1_000));
    assert_eq!(economics.transmitted_records, Some(10_000));
    assert_eq!(economics.transmitted_payload_bytes, Some(1_000_000));
    assert_eq!(economics.transmitted_request_bytes, Some(1_200_000));
    assert_eq!(economics.received_bytes, Some(150_000));
    assert_eq!(economics.snapshots, 4);
    assert_eq!(economics.measured_windows, 2);
}

#[test]
fn per_broker_counters_exclude_the_bootstrap_pseudo_broker() {
    let economics = request_economics_from_snapshots(&stream(), 10_000, Some("measured")).unwrap();

    // Two real brokers at 2 and 3 retries each, differenced across the window.
    assert_eq!(economics.retries, Some(0));
    assert_eq!(economics.timeouts, Some(0));
    assert_eq!(economics.all_requests, Some(1_000));
}

#[test]
fn batch_windows_are_summed_rather_than_differenced() {
    let economics = request_economics_from_snapshots(&stream(), 10_000, Some("measured")).unwrap();

    // Two measured snapshots, each reporting a window of 2 batches / 400
    // records: reset-on-emit windows accumulate.
    let records = economics.batch_records.unwrap();
    assert_eq!(records.samples, 4);
    assert_eq!(records.total, 800);
    assert_eq!(records.minimum, Some(100));
    assert_eq!(records.maximum, Some(300));
    assert!((records.mean().unwrap() - 200.0).abs() < TOLERANCE);

    let bytes = economics.batch_bytes.unwrap();
    assert_eq!(bytes.samples, 4);
    assert_eq!(bytes.total, 8_000);
}

#[test]
fn a_topic_filter_excludes_the_warmup_topic() {
    let filtered = request_economics_from_snapshots(&stream(), 10_000, Some("measured")).unwrap();
    let unfiltered = request_economics_from_snapshots(&stream(), 10_000, None).unwrap();

    assert_eq!(filtered.batch_records.unwrap().samples, 4);
    assert_eq!(unfiltered.batch_records.unwrap().samples, 6);
}

#[test]
fn normalizations_are_anchored_to_the_acknowledged_count() {
    let economics = request_economics_from_snapshots(&stream(), 10_000, Some("measured")).unwrap();

    // 1000 produce requests for 10000 acknowledged records is 100000 per million.
    assert!(
        (economics.produce_requests_per_million_acknowledged.unwrap() - 100_000.0).abs()
            < TOLERANCE
    );
    assert!((economics.records_per_produce_request.unwrap() - 10.0).abs() < TOLERANCE);
    // 1000000 payload bytes inside 1200000 transmitted bytes.
    assert!(
        (economics.payload_bytes_per_transmitted_byte.unwrap() - (1.0 / 1.2)).abs() < TOLERANCE
    );
    assert!(economics.is_reported());
}

#[test]
fn a_zero_acknowledged_count_leaves_the_normalization_absent() {
    let economics = request_economics_from_snapshots(&stream(), 0, Some("measured")).unwrap();

    assert_eq!(economics.produce_requests_per_million_acknowledged, None);
    assert_eq!(economics.produce_requests, Some(1_000));
}

#[test]
fn a_stream_without_native_counters_reports_nothing_rather_than_zero() {
    let empty = vec![
        json!({"phase": "baseline", "statistics": {"type": "producer"}}),
        json!({"phase": "final", "statistics": {"type": "producer"}}),
    ];

    let economics = request_economics_from_snapshots(&empty, 10_000, None).unwrap();

    assert_eq!(economics.produce_requests, None);
    assert_eq!(economics.all_requests, None);
    assert_eq!(economics.transmitted_records, None);
    assert_eq!(economics.transmitted_payload_bytes, None);
    assert_eq!(economics.retries, None);
    assert_eq!(economics.batch_records, None);
    assert_eq!(economics.records_per_produce_request, None);
    assert_eq!(economics.produce_requests_per_million_acknowledged, None);
    assert!(!economics.is_reported());
}

#[test]
fn a_counter_that_moved_backwards_is_absent_rather_than_negative() {
    let backwards = vec![
        json!({"phase": "baseline", "statistics": {"tx": 500, "txmsgs": 10}}),
        json!({"phase": "final", "statistics": {"tx": 100, "txmsgs": 20}}),
    ];

    let economics = request_economics_from_snapshots(&backwards, 10, None).unwrap();

    assert_eq!(economics.all_requests, None);
    assert_eq!(economics.transmitted_records, Some(10));
}

#[test]
fn a_stream_without_phase_labels_uses_the_whole_span() {
    let unlabelled = vec![
        json!({"tx": 10, "txmsgs": 100}),
        json!({"tx": 20, "txmsgs": 200}),
        json!({"tx": 60, "txmsgs": 600}),
    ];

    let economics = request_economics_from_snapshots(&unlabelled, 500, None).unwrap();

    assert_eq!(economics.all_requests, Some(50));
    assert_eq!(economics.transmitted_records, Some(500));
    assert_eq!(economics.measured_windows, 2);
}

#[test]
fn a_single_snapshot_cannot_be_differenced() {
    let error = request_economics_from_snapshots(&[json!({"tx": 1})], 1, None).unwrap_err();

    assert_eq!(error.kind(), ReportErrorKind::Sample);
}

#[test]
fn a_stream_is_read_from_disk() {
    let directory = std::env::temp_dir().join(format!(
        "bench-report-economics-{}",
        std::process::id().wrapping_add(1)
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join(STATISTICS_FILE_NAME);
    let mut text = String::new();
    for snapshot in stream() {
        text.push_str(&snapshot.to_string());
        text.push('\n');
    }
    std::fs::write(&path, text).unwrap();

    let economics = read_request_economics(&path, 10_000, Some("measured")).unwrap();

    assert_eq!(economics.produce_requests, Some(1_000));
    std::fs::remove_dir_all(&directory).unwrap();
}

#[test]
fn a_malformed_line_names_itself() {
    let directory = std::env::temp_dir().join(format!(
        "bench-report-economics-bad-{}",
        std::process::id().wrapping_add(2)
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join(STATISTICS_FILE_NAME);
    std::fs::write(&path, "{\"tx\":1}\nnot json\n").unwrap();

    let error = read_request_economics(&path, 1, None).unwrap_err();

    assert_eq!(error.kind(), ReportErrorKind::Document);
    assert!(error.context().contains("line 2"), "{}", error.context());
    std::fs::remove_dir_all(&directory).unwrap();
}

#[test]
fn a_missing_stream_is_an_io_failure() {
    let path = std::env::temp_dir().join("bench-report-economics-absent/client-metrics.jsonl");

    let error = read_request_economics(&path, 1, None).unwrap_err();

    assert_eq!(error.kind(), ReportErrorKind::Io);
}
