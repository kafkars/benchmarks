//! The fixed-rate offer loop: four callers over one immutable schedule.

use std::{
    error::Error,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use kafkars::Producer;

use crate::{
    producer::{BATCH_RECORDS, COMPLETION_TIMEOUT, DELIVERY_TIMEOUT, turn::AdmissionTurn},
    schedule::Schedule,
};

use super::{
    PhaseContext,
    clock::RunClock,
    engine::{AdmissionOrder, OfferEngine},
    measurement::Measurement,
    outstanding::OutstandingGauge,
    pool::{self, PayloadPool},
};

/// Delay before the first record is due, so every caller is running by then.
const EPOCH_DELAY: Duration = Duration::from_millis(100);

/// What one fixed-rate phase needs.
#[derive(Debug)]
pub(super) struct FixedRateSpec<'a> {
    /// The producer every caller admits through.
    pub(super) producer: &'a Producer,
    /// Prebuilt payload bytes.
    pub(super) pool: &'a PayloadPool,
    /// The measured topic.
    pub(super) topic: &'a str,
    /// Records the schedule covers.
    pub(super) records: u64,
    /// Partitions offers are assigned to.
    pub(super) partitions: usize,
    /// Offers the client may own across all callers.
    pub(super) budget: u64,
    /// Callers sharing the schedule.
    pub(super) callers: usize,
    /// The rate the schedule was built at.
    pub(super) offered_records_per_second: u64,
    /// The shared outstanding-offer observation.
    pub(super) outstanding: &'a OutstandingGauge,
}

/// One fixed-rate phase's merged evidence and the interval it covers.
#[derive(Debug)]
pub(super) struct FixedRateOutcome {
    /// Every caller's evidence, merged.
    pub(super) measurement: Measurement,
    /// Nanoseconds from the schedule epoch to the end of the drain.
    pub(super) measured_duration_ns: u64,
}

/// Runs the schedule across `callers` threads and merges their evidence.
///
/// The schedule is never materialized. Each caller walks the batch indexes it
/// owns and asks [`Schedule`] for one batch at a time, so a ten-million-record
/// run holds four callers' worth of loop variables rather than a vector of
/// forty thousand scheduled batches.
pub(super) fn run(spec: &FixedRateSpec<'_>) -> Result<FixedRateOutcome, Box<dyn Error>> {
    let schedule = Schedule::new(
        spec.offered_records_per_second,
        spec.records,
        u64::try_from(BATCH_RECORDS)?,
        u64::try_from(spec.callers)?,
    )?;
    let callers = u64::try_from(spec.callers)?;
    let base_budget = spec.budget / callers;
    let extra_budget = spec.budget % callers;
    if base_budget < u64::try_from(BATCH_RECORDS)? {
        return Err("the fixed-rate budget must hold one full batch per caller".into());
    }
    let epoch = Instant::now()
        .checked_add(EPOCH_DELAY)
        .ok_or("the fixed-rate epoch overflowed")?;
    let clock = RunClock::starting_at(epoch);
    let turn = AdmissionTurn::new();
    let topic = Arc::<str>::from(spec.topic);

    let measurements = thread::scope(|scope| {
        let mut handles = Vec::with_capacity(spec.callers);
        for caller in 0..callers {
            let producer = spec.producer.clone();
            let topic = Arc::clone(&topic);
            let turn = &turn;
            let budget = base_budget + u64::from(caller < extra_budget);
            handles.push(scope.spawn(move || {
                let context = PhaseContext {
                    producer: &producer,
                    pool: spec.pool,
                    topic,
                    clock,
                    outstanding: spec.outstanding,
                };
                run_caller(&context, spec, schedule, caller, budget, turn)
                    .map_err(|error| error.to_string())
            }));
        }
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| "a fixed-rate caller panicked".to_owned())?
            })
            .collect::<Result<Vec<_>, String>>()
    })
    .map_err(|error| -> Box<dyn Error> { error.into() })?;

    let mut merged = Measurement::scheduled(spec.offered_records_per_second);
    for measurement in &measurements {
        merged.merge(measurement);
    }
    Ok(FixedRateOutcome {
        measurement: merged,
        measured_duration_ns: clock.at(Instant::now()),
    })
}

/// One caller's stripe of the schedule.
///
/// The records are built before the due wait, exactly as the legacy phase
/// builds them, so payload construction cannot masquerade as scheduler
/// lateness. What follows the wait is the application's own budget check and
/// then the public call, in that order, which is why `intended_to_call_start`
/// carries application queueing and `call_start_to_accepted` carries the
/// client's.
fn run_caller(
    context: &PhaseContext<'_>,
    spec: &FixedRateSpec<'_>,
    schedule: Schedule,
    caller: u64,
    budget: u64,
    turn: &AdmissionTurn,
) -> Result<Measurement, Box<dyn Error>> {
    let measurement = Measurement::scheduled(spec.offered_records_per_second);
    let mut engine = OfferEngine::new(context, budget, measurement)?;
    let batch_count = schedule.batch_count();
    let mut index = caller;
    while index < batch_count {
        let batch = schedule.batch(index)?;
        let records = pool::records(
            context.pool,
            &context.topic,
            batch.first_sequence,
            batch.count,
            spec.partitions,
        )?;
        engine.wait_until(batch.intended_ns)?;
        while engine.active_offers() + batch.count > budget {
            engine.settle_within(COMPLETION_TIMEOUT)?;
        }
        engine.admit(
            records,
            batch.first_sequence,
            AdmissionOrder::Linearized(turn, batch.index),
        )?;
        index += u64::try_from(spec.callers)?;
    }
    engine.drain(DELIVERY_TIMEOUT)?;
    Ok(engine.finish())
}
