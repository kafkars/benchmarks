//! Four-caller execution of one immutable open-loop schedule.

mod admission_turn;
#[cfg(test)]
mod admission_turn_test;
mod caller;
mod result;

use std::{error::Error, sync::Arc, thread, time::Instant};

use kafkars::Producer;

use crate::schedule;

use super::phase::PhaseSpec;

pub(super) use self::result::{FixedPhaseResult, write_latencies};

#[derive(Clone, Copy, Debug)]
pub(super) struct FixedPhaseSpec<'a> {
    pub(super) phase: PhaseSpec<'a>,
    pub(super) offered_records_per_second: u64,
    pub(super) callers: usize,
}

pub(super) fn run_fixed_phase(
    producer: &Producer,
    spec: FixedPhaseSpec<'_>,
) -> Result<FixedPhaseResult, Box<dyn Error>> {
    let schedules = schedule::batches(
        spec.offered_records_per_second,
        u64::try_from(spec.phase.records)?,
        u64::try_from(super::BATCH_RECORDS)?,
        u64::try_from(spec.callers)?,
    )?;
    let mut by_caller = (0..spec.callers).map(|_| Vec::new()).collect::<Vec<_>>();
    for batch in schedules {
        by_caller[usize::try_from(batch.caller)?].push(batch);
    }
    let epoch = Instant::now()
        .checked_add(std::time::Duration::from_millis(100))
        .ok_or("fixed-load epoch overflowed")?;
    let base_budget = spec.phase.max_outstanding / spec.callers;
    let extra_budget = spec.phase.max_outstanding % spec.callers;
    if base_budget < super::BATCH_RECORDS {
        return Err("fixed-load budget must hold one full batch per caller".into());
    }
    let admission_turn = Arc::new(admission_turn::AdmissionTurn::new());

    let results = thread::scope(|scope| {
        let mut handles = Vec::with_capacity(spec.callers);
        for (caller_id, batches) in by_caller.into_iter().enumerate() {
            let producer = producer.clone();
            let admission_turn = Arc::clone(&admission_turn);
            let budget = base_budget + usize::from(caller_id < extra_budget);
            handles.push(scope.spawn(move || {
                caller::run(
                    &producer,
                    spec,
                    batches,
                    caller_id,
                    budget,
                    epoch,
                    &admission_turn,
                )
                .map_err(|error| error.to_string())
            }));
        }
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| "fixed-load caller panicked".to_owned())?
            })
            .collect::<Result<Vec<_>, String>>()
    })
    .map_err(|error| -> Box<dyn Error> { error.into() })?;
    FixedPhaseResult::merge(results, spec.phase.records)
}
