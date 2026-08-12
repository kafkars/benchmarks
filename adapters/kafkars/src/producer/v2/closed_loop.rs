//! The closed-loop offer loop: one caller, offering as fast as it is allowed.

use std::error::Error;

use crate::producer::{BATCH_RECORDS, COMPLETION_TIMEOUT, DELIVERY_TIMEOUT};

use super::{
    PhaseContext, PhaseShape,
    engine::{AdmissionOrder, OfferEngine},
    measurement::Measurement,
    pool,
};

/// Offers every record, then drains what the client still owns.
///
/// There is no schedule here: the caller offers whenever the budget allows, so
/// an offer's `intended` time is its `call_start` and the document carries no
/// scheduler lateness. Backpressure shows up as a longer admission wait rather
/// than as lateness, which is the whole difference between closed-loop and
/// open-loop evidence.
pub(super) fn run(
    context: &PhaseContext<'_>,
    shape: PhaseShape,
    measurement: Measurement,
) -> Result<Measurement, Box<dyn Error>> {
    let mut engine = OfferEngine::new(context, shape.budget, measurement)?;
    let batch_records = u64::try_from(BATCH_RECORDS)?;
    let partitions = u64::try_from(shape.partitions)?;
    let mut next_sequence = 0u64;
    while next_sequence < shape.records {
        let limit = admission_limit(&engine, shape, partitions, next_sequence);
        while engine.active_offers() >= limit {
            engine.settle_within(COMPLETION_TIMEOUT)?;
        }
        let count = batch_records
            .min(limit - engine.active_offers())
            .min(shape.records - next_sequence);
        let records = pool::records(
            context.pool,
            &context.topic,
            next_sequence,
            count,
            shape.partitions,
        )?;
        engine.admit(records, next_sequence, AdmissionOrder::Single)?;
        next_sequence += count;
    }
    engine.drain(DELIVERY_TIMEOUT)?;
    Ok(engine.finish())
}

/// Offers the client may own while `next_sequence` is being offered.
///
/// A priming phase admits its first record per partition alone, so every
/// partition has a leader and a connection before any measured concurrency
/// starts. That is warmup behavior and never applies to a measured interval.
fn admission_limit(
    engine: &OfferEngine<'_>,
    shape: PhaseShape,
    partitions: u64,
    next_sequence: u64,
) -> u64 {
    if shape.prime_partitions && next_sequence < partitions {
        1
    } else {
        engine.budget()
    }
}
