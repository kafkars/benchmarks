//! Request economics: how much broker traffic a subject spent to deliver the
//! records it delivered.
//!
//! # What this is for
//!
//! Two clients can post the same goodput and the same p99 while spending very
//! different numbers of produce requests, wire bytes, and batches to do it. The
//! difference is invisible in a latency histogram and is exactly what an
//! operator pays for, so it is worth reporting on its own axis. These numbers
//! are *descriptive*: they say what the client did, not whether it was right to
//! do it, and a batching strategy that looks wasteful at one payload size may
//! be the correct one at another.
//!
//! # Where the numbers come from
//!
//! The librdkafka statistics stream (`client-metrics.jsonl`), one JSON snapshot
//! per line. Field meanings follow the legacy control plane's
//! `librdkafka-statistics.mjs`, which is the behavioral reference:
//!
//! - `tx` / `tx_bytes` — requests and request bytes put on the wire, cumulative;
//! - `rx_bytes` — response bytes taken off it, cumulative;
//! - `txmsgs` / `txmsg_bytes` — records and payload bytes transmitted, cumulative;
//! - `brokers.*.req.Produce` — produce requests per broker, cumulative;
//! - `brokers.*.txretries` / `req_timeouts` — retries and timeouts, cumulative;
//! - `topics.<topic>.batchcnt` / `batchsize` — *reset-on-emit* rolling windows,
//!   so they are aggregated across snapshots rather than differenced.
//!
//! Cumulative counters are differenced between the `baseline` snapshot and the
//! `final` one, which is what isolates the measured phase from warmup.
//!
//! # Lenient on purpose, absent rather than invented
//!
//! Unlike the legacy module, which fails closed on any deviation from the
//! pinned schema, this one reads what is there: snapshots are parsed as generic
//! JSON, unknown keys are ignored, and a counter that is missing yields `None`
//! rather than a zero. A subject whose client has no native statistics at all —
//! `kafkars` today, whose client-side counters are blocked on the client
//! repository — therefore reports `None` for every field here, and nothing
//! downstream may turn that absence into a number. An absent measurement and a
//! measurement of zero are different claims.

use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{ReportError, ReportResult};

/// The file name adapters write their native statistics stream to.
pub const STATISTICS_FILE_NAME: &str = "client-metrics.jsonl";

/// One reset-on-emit rolling window aggregated across the measured snapshots.
///
/// librdkafka clears these windows every time it emits them, so the aggregate
/// is a sum over snapshots, not a difference between two of them. Getting that
/// backwards silently reports one snapshot's worth of batching as the whole
/// run's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchWindow {
    /// Observations the windows counted in total: the number of batches.
    pub samples: u64,
    /// Sum of the observed values across every window.
    pub total: u64,
    /// Smallest value any window reported, when any window reported one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum: Option<u64>,
    /// Largest value any window reported, when any window reported one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum: Option<u64>,
}

impl BatchWindow {
    /// Returns the mean observation, or `None` when nothing was observed.
    #[must_use]
    #[expect(
        clippy::cast_precision_loss,
        reason = "reporting statistic over counter values, not identity arithmetic"
    )]
    pub fn mean(&self) -> Option<f64> {
        if self.samples == 0 {
            None
        } else {
            Some(self.total as f64 / self.samples as f64)
        }
    }
}

/// What one subject spent, in broker traffic, for the records it delivered.
///
/// Every field is optional because every field is a report of something the
/// client said about itself, and a client that says nothing must not be made to
/// look like a client that said zero.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RequestEconomics {
    /// Snapshots read from the stream.
    pub snapshots: usize,
    /// Snapshots that fell inside the measured window.
    pub measured_windows: usize,
    /// Produce requests sent during the measured window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub produce_requests: Option<u64>,
    /// Requests of every kind sent during the measured window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub all_requests: Option<u64>,
    /// Records the client put on the wire during the measured window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transmitted_records: Option<u64>,
    /// Payload bytes the client put on the wire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transmitted_payload_bytes: Option<u64>,
    /// Total request bytes the client put on the wire, payload plus framing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transmitted_request_bytes: Option<u64>,
    /// Response bytes the client read back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub received_bytes: Option<u64>,
    /// Request retries across all brokers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retries: Option<u64>,
    /// Request timeouts across all brokers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeouts: Option<u64>,
    /// Records per broker-bound batch, aggregated over the measured windows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_records: Option<BatchWindow>,
    /// Bytes per broker-bound batch, aggregated over the measured windows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_bytes: Option<BatchWindow>,
    /// Produce requests spent per million acknowledged records.
    ///
    /// Normalized per million so that runs of different lengths compare, and
    /// anchored to the harness's own acknowledged count rather than to the
    /// client's self-reported `txmsgs`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub produce_requests_per_million_acknowledged: Option<f64>,
    /// Records the client packed into an average produce request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub records_per_produce_request: Option<f64>,
    /// Fraction of transmitted bytes that were payload rather than framing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_bytes_per_transmitted_byte: Option<f64>,
}

impl RequestEconomics {
    /// Reports whether any native counter was found at all.
    ///
    /// False for a subject whose client emits no statistics; renderers use it
    /// to print "not reported" instead of a row of dashes that reads like zero.
    #[must_use]
    pub fn is_reported(&self) -> bool {
        self.produce_requests.is_some()
            || self.all_requests.is_some()
            || self.transmitted_records.is_some()
    }

    /// Totals several attempts' economics and renormalizes over their combined
    /// acknowledged record count.
    ///
    /// A counter that any part left absent is absent in the total. Summing the
    /// parts that happened to report would produce a total over a set of
    /// attempts nobody can name, which is worse than reporting nothing.
    #[must_use]
    pub fn total(parts: &[Self], acknowledged_records: u64) -> Self {
        let sum = |read: fn(&Self) -> Option<u64>| -> Option<u64> {
            parts
                .iter()
                .map(read)
                .try_fold(0u64, |total, value| Some(total.saturating_add(value?)))
        };
        let produce_requests = sum(|part| part.produce_requests);
        let transmitted_records = sum(|part| part.transmitted_records);
        let transmitted_payload_bytes = sum(|part| part.transmitted_payload_bytes);
        let transmitted_request_bytes = sum(|part| part.transmitted_request_bytes);
        Self {
            snapshots: parts.iter().map(|part| part.snapshots).sum(),
            measured_windows: parts.iter().map(|part| part.measured_windows).sum(),
            produce_requests,
            all_requests: sum(|part| part.all_requests),
            transmitted_records,
            transmitted_payload_bytes,
            transmitted_request_bytes,
            received_bytes: sum(|part| part.received_bytes),
            retries: sum(|part| part.retries),
            timeouts: sum(|part| part.timeouts),
            batch_records: total_window(parts, |part| part.batch_records),
            batch_bytes: total_window(parts, |part| part.batch_bytes),
            produce_requests_per_million_acknowledged: per_million(
                produce_requests,
                acknowledged_records,
            ),
            records_per_produce_request: ratio(transmitted_records, produce_requests),
            payload_bytes_per_transmitted_byte: ratio(
                transmitted_payload_bytes,
                transmitted_request_bytes,
            ),
        }
    }
}

/// Totals one batch window across several attempts, absent when any part is.
fn total_window(
    parts: &[RequestEconomics],
    read: fn(&RequestEconomics) -> Option<BatchWindow>,
) -> Option<BatchWindow> {
    let mut total = BatchWindow {
        samples: 0,
        total: 0,
        minimum: None,
        maximum: None,
    };
    for part in parts {
        let window = read(part)?;
        total.samples = total.samples.saturating_add(window.samples);
        total.total = total.total.saturating_add(window.total);
        total.minimum = match (total.minimum, window.minimum) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        };
        total.maximum = match (total.maximum, window.maximum) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (left, right) => left.or(right),
        };
    }
    Some(total)
}

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

/// Returns the librdkafka statistics object inside a snapshot, whether it is
/// wrapped in the sealed envelope or not.
fn statistics_of(snapshot: Option<&Value>) -> Option<&Value> {
    let snapshot = snapshot?;
    match snapshot.get("statistics") {
        Some(inner) if inner.is_object() => Some(inner),
        _ => snapshot.is_object().then_some(snapshot),
    }
}

/// Returns a non-negative integer field of an object.
fn counter(value: Option<&Value>, key: &str) -> Option<u64> {
    value?.get(key)?.as_u64()
}

/// Returns the increase of a cumulative counter across the measured window.
///
/// A counter that moved backwards is reported as `None`: librdkafka counters
/// are monotonic, so a decrease means the two snapshots came from different
/// client instances and the difference means nothing.
fn counter_delta(baseline: Option<&Value>, terminal: Option<&Value>, key: &str) -> Option<u64> {
    let start = counter(baseline, key)?;
    let finish = counter(terminal, key)?;
    finish.checked_sub(start)
}

/// Returns the brokers a snapshot describes, excluding the bootstrap
/// pseudo-broker, which carries a negative node id and no request accounting.
fn brokers(snapshot: Option<&Value>) -> Vec<&Value> {
    let Some(map) = snapshot
        .and_then(|value| value.get("brokers"))
        .and_then(Value::as_object)
    else {
        return Vec::new();
    };
    map.values()
        .filter(|broker| {
            broker
                .get("nodeid")
                .and_then(Value::as_i64)
                .is_some_and(|node| node >= 0)
        })
        .collect()
}

/// Returns the increase of a per-broker counter, summed across brokers.
fn broker_counter_delta(
    baseline: Option<&Value>,
    terminal: Option<&Value>,
    key: &str,
) -> Option<u64> {
    let finish = broker_total(terminal, key)?;
    let start = broker_total(baseline, key).unwrap_or(0);
    finish.checked_sub(start)
}

/// Sums one counter across every broker in a snapshot.
fn broker_total(snapshot: Option<&Value>, key: &str) -> Option<u64> {
    let brokers = brokers(snapshot);
    if brokers.is_empty() {
        return None;
    }
    let mut total = 0u64;
    let mut seen = false;
    for broker in brokers {
        if let Some(value) = counter(Some(broker), key) {
            total = total.saturating_add(value);
            seen = true;
        }
    }
    seen.then_some(total)
}

/// Returns the increase of one request-type counter, summed across brokers.
fn request_type_delta(
    baseline: Option<&Value>,
    terminal: Option<&Value>,
    request: &str,
) -> Option<u64> {
    let finish = request_type_total(terminal, request)?;
    let start = request_type_total(baseline, request).unwrap_or(0);
    finish.checked_sub(start)
}

/// Sums one request-type counter across every broker in a snapshot.
fn request_type_total(snapshot: Option<&Value>, request: &str) -> Option<u64> {
    let brokers = brokers(snapshot);
    if brokers.is_empty() {
        return None;
    }
    let mut total = 0u64;
    let mut seen = false;
    for broker in brokers {
        if let Some(value) = broker
            .get("req")
            .and_then(|requests| requests.get(request))
            .and_then(Value::as_u64)
        {
            total = total.saturating_add(value);
            seen = true;
        }
    }
    seen.then_some(total)
}

/// Aggregates one reset-on-emit topic window across the measured snapshots.
fn aggregate_topic_window(
    windows: &[&Value],
    measured_topic: Option<&str>,
    key: &str,
) -> Option<BatchWindow> {
    let mut samples = 0u64;
    let mut total = 0u64;
    let mut minimum: Option<u64> = None;
    let mut maximum: Option<u64> = None;
    let mut seen = false;
    for snapshot in windows {
        let Some(topics) = statistics_of(Some(snapshot))
            .and_then(|statistics| statistics.get("topics"))
            .and_then(Value::as_object)
        else {
            continue;
        };
        for (name, topic) in topics {
            if measured_topic.is_some_and(|wanted| wanted != name) {
                continue;
            }
            let Some(window) = topic.get(key) else {
                continue;
            };
            seen = true;
            let count = counter(Some(window), "cnt").unwrap_or(0);
            if count == 0 {
                continue;
            }
            samples = samples.saturating_add(count);
            total = total.saturating_add(counter(Some(window), "sum").unwrap_or(0));
            if let Some(low) = counter(Some(window), "min") {
                minimum = Some(minimum.map_or(low, |current: u64| current.min(low)));
            }
            if let Some(high) = counter(Some(window), "max") {
                maximum = Some(maximum.map_or(high, |current: u64| current.max(high)));
            }
        }
    }
    seen.then_some(BatchWindow {
        samples,
        total,
        minimum,
        maximum,
    })
}

/// Returns `numerator / denominator` when both are present and the denominator
/// is not zero.
#[expect(
    clippy::cast_precision_loss,
    reason = "reporting statistic over counter values, not identity arithmetic"
)]
fn ratio(numerator: Option<u64>, denominator: Option<u64>) -> Option<f64> {
    let denominator = denominator.filter(|value| *value > 0)?;
    Some(numerator? as f64 / denominator as f64)
}

/// Returns `count` scaled to a per-million-records rate.
#[expect(
    clippy::cast_precision_loss,
    reason = "reporting statistic over counter values, not identity arithmetic"
)]
fn per_million(count: Option<u64>, acknowledged_records: u64) -> Option<f64> {
    if acknowledged_records == 0 {
        return None;
    }
    Some(count? as f64 * 1_000_000.0 / acknowledged_records as f64)
}
