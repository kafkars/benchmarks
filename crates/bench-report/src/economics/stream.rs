//! Reading a whole native statistics stream, and deciding which part of it was
//! the measured phase.
//!
//! Cumulative counters are differenced between the `baseline` snapshot and the
//! `final` one, which is what isolates the measured phase from warmup. The
//! reset-on-emit windows are aggregated across the snapshots between them
//! instead — see [`super::window`].

use std::path::Path;

use serde_json::Value;

use crate::error::{ReportError, ReportResult};

use super::RequestEconomics;
use super::normalize::{per_million, ratio};
use super::snapshot::{broker_counter_delta, counter_delta, request_type_delta, statistics_of};
use super::window::aggregate_topic_window;

/// The file name adapters write their native statistics stream to.
pub const STATISTICS_FILE_NAME: &str = "client-metrics.jsonl";

/// Reads a native statistics stream and normalizes it into request economics.
///
/// `acknowledged_records` comes from the measurement document, not from the
/// stream, so the normalization is anchored to what the harness counted.
/// `measured_topic` restricts the batch windows to the measured topic; passing
/// `None` aggregates every topic, which is only correct when the stream covers
/// one topic.
///
/// # Errors
///
/// Returns an [`ReportErrorKind::Io`](crate::ReportErrorKind::Io) error when
/// the file cannot be read and a
/// [`ReportErrorKind::Document`](crate::ReportErrorKind::Document) error when a
/// line is not JSON.
pub fn read_request_economics(
    path: &Path,
    acknowledged_records: u64,
    measured_topic: Option<&str>,
) -> ReportResult<RequestEconomics> {
    let text = std::fs::read_to_string(path).map_err(|error| ReportError::io(path, &error))?;
    let mut snapshots = Vec::new();
    for (offset, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(line).map_err(|error| {
            ReportError::document(format!(
                "{}: line {}: {error}",
                path.display(),
                offset.saturating_add(1)
            ))
        })?;
        snapshots.push(value);
    }
    request_economics_from_snapshots(&snapshots, acknowledged_records, measured_topic)
}

/// Normalizes already-parsed statistics snapshots into request economics.
///
/// Each snapshot may be the sealed envelope (`{schema, phase, captured_ns,
/// statistics}`) or a bare librdkafka statistics object; both are accepted, so
/// a stream written before the envelope existed still reads.
///
/// # Errors
///
/// Returns a [`ReportErrorKind::Sample`](crate::ReportErrorKind::Sample) error
/// when there are no snapshots to difference.
pub fn request_economics_from_snapshots(
    snapshots: &[Value],
    acknowledged_records: u64,
    measured_topic: Option<&str>,
) -> ReportResult<RequestEconomics> {
    if snapshots.len() < 2 {
        return Err(ReportError::sample(
            "request economics need a baseline and a final snapshot",
        ));
    }
    let (first, last) = measured_window(snapshots);
    let baseline = statistics_of(snapshots.get(first));
    let terminal = statistics_of(snapshots.get(last));
    let windows: Vec<&Value> = snapshots
        .get(first.saturating_add(1)..=last)
        .unwrap_or_default()
        .iter()
        .collect();

    let produce_requests = request_type_delta(baseline, terminal, "Produce");
    let transmitted_records = counter_delta(baseline, terminal, "txmsgs");
    let transmitted_payload_bytes = counter_delta(baseline, terminal, "txmsg_bytes");
    let transmitted_request_bytes = counter_delta(baseline, terminal, "tx_bytes");

    Ok(RequestEconomics {
        snapshots: snapshots.len(),
        measured_windows: windows.len(),
        produce_requests,
        all_requests: counter_delta(baseline, terminal, "tx"),
        transmitted_records,
        transmitted_payload_bytes,
        transmitted_request_bytes,
        received_bytes: counter_delta(baseline, terminal, "rx_bytes"),
        retries: broker_counter_delta(baseline, terminal, "txretries"),
        timeouts: broker_counter_delta(baseline, terminal, "req_timeouts"),
        batch_records: aggregate_topic_window(&windows, measured_topic, "batchcnt"),
        batch_bytes: aggregate_topic_window(&windows, measured_topic, "batchsize"),
        produce_requests_per_million_acknowledged: per_million(
            produce_requests,
            acknowledged_records,
        ),
        records_per_produce_request: ratio(transmitted_records, produce_requests),
        payload_bytes_per_transmitted_byte: ratio(
            transmitted_payload_bytes,
            transmitted_request_bytes,
        ),
    })
}

/// Returns the indexes bounding the measured window.
///
/// The `baseline` snapshot is the last one taken before the measured phase and
/// the `final` one the last taken after it, exactly as the legacy module reads
/// them. When the stream carries no phase labels, or labels them in an order
/// that cannot bound a window, the whole stream is the window: that is weaker
/// evidence, and it is stated rather than rejected.
fn measured_window(snapshots: &[Value]) -> (usize, usize) {
    let last_with_phase = |phase: &str| {
        snapshots
            .iter()
            .enumerate()
            .filter(|(_, snapshot)| snapshot.get("phase").and_then(Value::as_str) == Some(phase))
            .map(|(index, _)| index)
            .next_back()
    };
    let fallback = (0, snapshots.len().saturating_sub(1));
    let (Some(baseline), Some(terminal)) = (last_with_phase("baseline"), last_with_phase("final"))
    else {
        return fallback;
    };
    if baseline >= terminal {
        return fallback;
    }
    (baseline, terminal)
}
