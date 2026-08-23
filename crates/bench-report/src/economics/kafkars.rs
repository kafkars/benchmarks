//! Request economics carried by the versioned Kafkars native-metrics sidecar.

use bench_schema::KafkarsNativeMetrics;

use super::normalize::{per_million, ratio};
use super::{BatchWindow, RequestEconomics};

/// Normalizes exact public Kafkars counter deltas without inventing absent data.
pub(super) fn from_document(
    document: &KafkarsNativeMetrics,
    acknowledged_records: u64,
) -> RequestEconomics {
    let delta = document.delta;
    let produce_requests = Some(delta.produce_requests);
    let transmitted_records = Some(delta.records);
    RequestEconomics {
        snapshots: 2,
        measured_windows: 1,
        produce_requests,
        all_requests: None,
        transmitted_records,
        transmitted_payload_bytes: None,
        transmitted_request_bytes: None,
        received_bytes: None,
        retries: None,
        timeouts: None,
        batch_records: Some(BatchWindow {
            samples: delta.partition_batches,
            total: delta.records,
            minimum: None,
            maximum: None,
        }),
        batch_bytes: Some(BatchWindow {
            samples: delta.partition_batches,
            total: delta.encoded_record_bytes,
            minimum: None,
            maximum: None,
        }),
        produce_requests_per_million_acknowledged: per_million(
            produce_requests,
            acknowledged_records,
        ),
        records_per_produce_request: ratio(transmitted_records, produce_requests),
        payload_bytes_per_transmitted_byte: None,
    }
}
