//! What one subject spent, and how several attempts of it add up.
//!
//! Every field is optional because every field is a report of something the
//! client said about itself. Summing the parts that happened to report would
//! produce a total over a set of attempts nobody can name, so a counter that
//! any part left absent is absent in the total.

use serde::{Deserialize, Serialize};

use super::normalize::{per_million, ratio};
use super::window::{BatchWindow, total_window};

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
