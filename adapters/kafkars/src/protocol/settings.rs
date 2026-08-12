//! The client settings `producer.rs` fixes at compile time, and the check that
//! declines an experiment asking for any other value.
//!
//! Each constant is the value the client is actually built with. They are
//! restated here rather than read from the producer because the check is about
//! what a reader was promised, and a check that read its expectation from the
//! thing it checks would pass by construction.

use bench_schema::ResolvedExperiment;

/// Client queue budget `producer.rs` configures, in bytes.
pub(super) const QUEUE_BYTES: u64 = 64 * 1024 * 1024;

/// Records per broker-bound batch.
const BATCH_RECORDS: u32 = 256;

/// Bytes per broker-bound batch.
const BATCH_BYTES: u64 = 65_536;

/// Bytes per produce request.
const REQUEST_BYTES: u64 = 1_048_576;

/// Produce requests in flight per broker.
const MAX_IN_FLIGHT_REQUESTS_PER_BROKER: u32 = 5;

/// Milliseconds the client may wait to fill a batch.
const LINGER_MS: u64 = 5;

/// Milliseconds before a record's delivery is abandoned.
const DELIVERY_TIMEOUT_MS: u64 = 60_000;

/// Re-enqueue attempts after a retriable failure.
const RETRY_MAX_REPLACEMENTS: u32 = 600;

/// Milliseconds between retries.
const RETRY_BACKOFF_MS: u64 = 100;

/// Checks the client settings `producer.rs` fixes at compile time.
pub(super) fn check_producer(experiment: &ResolvedExperiment, reasons: &mut Vec<String>) {
    let Some(producer) = experiment.producer.as_ref() else {
        return;
    };
    let mut fixed = |field: &str, wanted: String, configured: &str| {
        if wanted != configured {
            reasons.push(format!(
                "this adapter configures {field}={configured} and offers no way to change \
                 it; the experiment asks for {wanted}"
            ));
        }
    };
    fixed("acks", producer.acks.clone(), "all");
    fixed("idempotence", producer.idempotence.to_string(), "true");
    fixed("compression", producer.compression.clone(), "none");
    fixed(
        "linger_ms",
        producer.linger_ms.to_string(),
        &LINGER_MS.to_string(),
    );
    fixed(
        "batch_records",
        producer.batch_records.to_string(),
        &BATCH_RECORDS.to_string(),
    );
    fixed(
        "batch_bytes",
        producer.batch_bytes.to_string(),
        &BATCH_BYTES.to_string(),
    );
    fixed(
        "request_bytes",
        producer.request_bytes.to_string(),
        &REQUEST_BYTES.to_string(),
    );
    fixed(
        "delivery_timeout_ms",
        producer.delivery_timeout_ms.to_string(),
        &DELIVERY_TIMEOUT_MS.to_string(),
    );
    fixed(
        "max_in_flight_requests_per_broker",
        producer.max_in_flight_requests_per_broker.to_string(),
        &MAX_IN_FLIGHT_REQUESTS_PER_BROKER.to_string(),
    );
    fixed(
        "retry_max_replacements",
        producer.retry_max_replacements.to_string(),
        &RETRY_MAX_REPLACEMENTS.to_string(),
    );
    fixed(
        "retry_backoff_ms",
        producer.retry_backoff_ms.to_string(),
        &RETRY_BACKOFF_MS.to_string(),
    );
    for (field, partitioning) in [
        ("partitioning", Some(&producer.partitioning)),
        ("warmup_partitioning", producer.warmup_partitioning.as_ref()),
    ] {
        if let Some(partitioning) = partitioning
            && partitioning != "explicit-round-robin"
        {
            reasons.push(format!(
                "this adapter assigns partitions itself, round robin by record sequence; \
                 the experiment asks for {field}={partitioning:?}"
            ));
        }
    }
    if let Some(primer) = producer.warmup_serialized_partition_primer_records
        && primer != experiment.cluster.partitions
    {
        reasons.push(format!(
            "the warmup primes exactly one record per partition, which is {} here, and the \
             experiment asks for {primer}",
            experiment.cluster.partitions
        ));
    }
}
