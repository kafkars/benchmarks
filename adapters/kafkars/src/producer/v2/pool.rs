//! Payload bytes built before the measured interval, copied inside it.

use std::{error::Error, sync::Arc};

use bytes::Bytes;
use kafkars::Record;

use crate::payload;

/// Bytes in a record key: the sequence number, big endian.
const KEY_BYTES: usize = 8;

/// Every payload the deterministic identity function can produce.
///
/// # What is prebuilt and what is not
///
/// The legacy closed-loop phase called `payload::make` per record inside the
/// measured interval: an allocation and a full byte-by-byte filler
/// computation, charged to the latency of the record that happened to trigger
/// it. Here the filler is computed once, before the run, into
/// [`payload::FILLER_PERIOD`] templates — which is *every* distinct payload
/// the function has, because the filler depends on the sequence only through
/// `sequence % 16`.
///
/// The copy is not prebuilt, and this adapter does not pretend otherwise.
/// `Producer::send_batch` takes ownership of the records, so the bytes handed
/// to the client cannot be the pool's own; each offer gets its own buffer,
/// filled by one `copy_from_slice` from a template plus a sixteen-byte
/// rewrite of the sequence field. One allocation backs a whole batch and is
/// sliced per record, so the measured path allocates per batch rather than per
/// record. The document declares this as `payload_construction: "prebuilt-pool-per-offer-sequence"`
/// with `ownership: "owned-per-offer-from-pool"` rather than claiming a
/// zero-copy handoff it does not perform.
///
/// `pool_test` asserts the result byte-for-byte against [`payload::make`] over
/// the sequences the committed conformance vectors pin, so the residue
/// argument above is checked rather than believed.
#[derive(Debug)]
pub(super) struct PayloadPool {
    templates: Vec<u8>,
    payload_bytes: usize,
}

impl PayloadPool {
    /// Builds every template. Call before the measured interval begins.
    pub(super) fn build(run_id: &str, payload_bytes: usize) -> Result<Self, Box<dyn Error>> {
        if payload_bytes < crate::arguments::MIN_PAYLOAD_BYTES {
            return Err(format!(
                "a payload of {payload_bytes} bytes cannot hold the identity envelope"
            )
            .into());
        }
        let period = usize::try_from(payload::FILLER_PERIOD)?;
        let total = period
            .checked_mul(payload_bytes)
            .ok_or("the payload template pool overflowed")?;
        let mut templates = vec![b'0'; total];
        for residue in 0..period {
            let start = residue * payload_bytes;
            payload::write(
                &mut templates[start..start + payload_bytes],
                run_id,
                u64::try_from(residue)?,
            );
        }
        Ok(Self {
            templates,
            payload_bytes,
        })
    }

    /// Bytes in one record's payload.
    pub(super) const fn payload_bytes(&self) -> usize {
        self.payload_bytes
    }

    /// Writes one record's payload into `target`.
    fn write(&self, target: &mut [u8], sequence: u64) {
        let residue = usize::try_from(sequence % payload::FILLER_PERIOD).unwrap_or(0);
        let start = residue * self.payload_bytes;
        target.copy_from_slice(&self.templates[start..start + self.payload_bytes]);
        payload::write_sequence(target, sequence);
    }
}

/// Builds one batch of records, partitioned round robin by sequence.
pub(super) fn records(
    pool: &PayloadPool,
    topic: &Arc<str>,
    first_sequence: u64,
    count: u64,
    partitions: usize,
) -> Result<Vec<Record>, Box<dyn Error>> {
    let count = usize::try_from(count)?;
    let payload_bytes = pool.payload_bytes();
    let partitions = u64::try_from(partitions)?;
    if partitions == 0 {
        return Err("a batch cannot be partitioned across zero partitions".into());
    }
    let mut values = vec![
        0u8;
        count
            .checked_mul(payload_bytes)
            .ok_or("the batch payload slab overflowed")?
    ];
    let mut keys = Vec::with_capacity(
        count
            .checked_mul(KEY_BYTES)
            .ok_or("the batch key slab overflowed")?,
    );
    for index in 0..count {
        let sequence = first_sequence
            .checked_add(u64::try_from(index)?)
            .ok_or("a record sequence overflowed")?;
        let start = index * payload_bytes;
        pool.write(&mut values[start..start + payload_bytes], sequence);
        keys.extend_from_slice(&sequence.to_be_bytes());
    }
    let values = Bytes::from(values);
    let keys = Bytes::from(keys);
    (0..count)
        .map(|index| {
            let sequence = first_sequence + u64::try_from(index)?;
            let key = index * KEY_BYTES;
            let value = index * payload_bytes;
            Ok(Record::to(Arc::clone(topic))
                .key(keys.slice(key..key + KEY_BYTES))
                .value(values.slice(value..value + payload_bytes))
                .partition(i32::try_from(sequence % partitions)?))
        })
        .collect()
}
