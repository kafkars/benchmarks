//! Deterministic batch ownership over one canonical open-loop record schedule.

use std::error::Error;

const NANOS_PER_SECOND: u64 = 1_000_000_000;
const MAX_OFFERED_RECORDS_PER_SECOND: u64 = NANOS_PER_SECOND;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ScheduledBatch {
    pub(crate) index: u64,
    pub(crate) caller: u64,
    pub(crate) first_sequence: u64,
    pub(crate) count: u64,
    pub(crate) intended_ns: u64,
}

pub(crate) fn batches(
    rate: u64,
    records: u64,
    batch_records: u64,
    callers: u64,
) -> Result<Vec<ScheduledBatch>, Box<dyn Error>> {
    validate(rate, records, batch_records, callers)?;
    let batch_count = records.div_ceil(batch_records);
    let capacity = usize::try_from(batch_count)?;
    let mut result = Vec::new();
    result.try_reserve_exact(capacity)?;
    for index in 0..batch_count {
        let first_sequence = index
            .checked_mul(batch_records)
            .ok_or("schedule batch sequence overflowed")?;
        let count = batch_records.min(records - first_sequence);
        let last_sequence = first_sequence + count - 1;
        result.push(ScheduledBatch {
            index,
            caller: index % callers,
            first_sequence,
            count,
            intended_ns: intended_offset_ns(last_sequence, rate)?,
        });
    }
    Ok(result)
}

pub(crate) fn intended_offset_ns(sequence: u64, rate: u64) -> Result<u64, Box<dyn Error>> {
    if rate == 0 || rate > MAX_OFFERED_RECORDS_PER_SECOND {
        return Err("offered rate must be within 1..=1,000,000,000".into());
    }
    let seconds = sequence / rate;
    let remainder = sequence % rate;
    seconds
        .checked_mul(NANOS_PER_SECOND)
        .and_then(|whole| {
            remainder
                .checked_mul(NANOS_PER_SECOND)
                .and_then(|scaled| whole.checked_add(scaled / rate))
        })
        .ok_or_else(|| "schedule offset overflowed".into())
}

fn validate(
    rate: u64,
    records: u64,
    batch_records: u64,
    callers: u64,
) -> Result<(), Box<dyn Error>> {
    if records == 0 || batch_records == 0 || callers == 0 {
        return Err("schedule records, batch records, and callers must be positive".into());
    }
    let _ = intended_offset_ns(records - 1, rate)?;
    Ok(())
}
