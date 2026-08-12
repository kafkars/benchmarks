//! Batch record construction and exact aggregate terminal accounting.

use std::{error::Error, sync::Arc, time::Instant};

use bytes::Bytes;
use kafkars::{ErrorKind, Record, SendBatchResult};

use crate::payload;

use super::{BatchSlot, PhaseResult, PhaseSpec};
use crate::producer::phase::LatencySample;

pub(super) enum BatchCompletion {
    Complete,
    Retry(Vec<Record>),
}

pub(super) fn record_result(
    phase: &mut PhaseResult,
    slot: &BatchSlot,
    started: Instant,
    batch: SendBatchResult,
) -> Result<BatchCompletion, Box<dyn Error>> {
    let completed = Instant::now();
    let (deliveries, rejection) = batch.into_parts();
    let accepted = deliveries.len();
    record_terminals(phase, deliveries, slot, started, completed);
    phase.accepted += accepted;
    match rejection {
        None if accepted == slot.offered => Ok(BatchCompletion::Complete),
        None => Err("batch admission lost its unaccepted suffix".into()),
        Some(rejection) if accepted == 0 && rejection.error().kind() == ErrorKind::Backpressure => {
            let (records, _error) = rejection.into_parts();
            Ok(BatchCompletion::Retry(records))
        }
        Some(rejection) => {
            let (records, error) = rejection.into_parts();
            let first_partition = records.first().and_then(Record::explicit_partition);
            let recent_failures = phase
                .failure_details
                .iter()
                .rev()
                .take(3)
                .cloned()
                .collect::<Vec<_>>()
                .join("; ");
            Err(format!(
                "batch admitted {accepted} of {} records and returned {} beginning at partition {first_partition:?} after later batches may have crossed admission: {error}; recent terminal failures: {recent_failures}",
                slot.offered,
                records.len(),
            )
            .into())
        }
    }
}

pub(in crate::producer) fn records(
    first_sequence: usize,
    end_sequence: usize,
    spec: PhaseSpec<'_>,
    partitions: usize,
) -> Result<Vec<Record>, Box<dyn Error>> {
    let count = end_sequence
        .checked_sub(first_sequence)
        .ok_or("batch record range is reversed")?;
    let value_bytes = count
        .checked_mul(spec.payload_bytes)
        .ok_or("batch payload slab size overflowed")?;
    let key_bytes = count
        .checked_mul(8)
        .ok_or("batch key slab size overflowed")?;
    let mut values = vec![0; value_bytes];
    let mut keys = Vec::with_capacity(key_bytes);
    for (index, sequence) in (first_sequence..end_sequence).enumerate() {
        let sequence = u64::try_from(sequence)?;
        let value_start = index * spec.payload_bytes;
        payload::write(
            &mut values[value_start..value_start + spec.payload_bytes],
            spec.run_id,
            sequence,
        );
        keys.extend_from_slice(&sequence.to_be_bytes());
    }
    let values = Bytes::from(values);
    let keys = Bytes::from(keys);
    let topic = Arc::<str>::from(spec.topic);
    (0..count)
        .map(|index| {
            let sequence = first_sequence + index;
            let key_start = index * 8;
            let value_start = index * spec.payload_bytes;
            Ok(Record::to(Arc::clone(&topic))
                .key(keys.slice(key_start..key_start + 8))
                .value(values.slice(value_start..value_start + spec.payload_bytes))
                .partition(i32::try_from(sequence % partitions)?))
        })
        .collect()
}

fn record_terminals(
    result: &mut PhaseResult,
    deliveries: Vec<Result<kafkars::RecordMetadata, kafkars::KafkaError>>,
    slot: &BatchSlot,
    started: Instant,
    completed: Instant,
) {
    for (index, delivery) in deliveries.into_iter().enumerate() {
        let sequence = slot.first_sequence + index;
        match delivery {
            Ok(_metadata) => record_success(result, sequence, slot.admitted, started, completed),
            Err(error) => {
                result.failed += 1;
                result.failure_details.push(format!(
                    "sequence {sequence} kind={:?} delivery={:?} broker_code={:?} retry={:?} fatal={} message={error}",
                    error.kind(),
                    error.delivery_status(),
                    error.broker_code(),
                    error.retry_advice(),
                    error.is_fatal(),
                ));
            }
        }
    }
}

fn record_success(
    result: &mut PhaseResult,
    sequence: usize,
    admitted: Instant,
    started: Instant,
    completed: Instant,
) {
    let latency = completed.duration_since(admitted).as_nanos();
    result.acknowledged += 1;
    result.latencies.push(latency);
    result.samples.push(LatencySample {
        sequence,
        admitted_ns: admitted.duration_since(started).as_nanos(),
        completed_ns: completed.duration_since(started).as_nanos(),
        latency_ns: latency,
    });
}
