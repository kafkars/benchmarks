//! One caller's bounded batch futures over its immutable schedule stripe.

use std::{
    error::Error,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, SyncSender, sync_channel},
    },
    task::{Context, Poll, Wake, Waker},
    thread,
    time::{Duration, Instant},
};

use kafkars::producer::{Producer, Record, SendBatch, SendBatchResult};

use crate::schedule::ScheduledBatch;

use super::{FixedPhaseResult, FixedPhaseSpec, result};
use crate::producer::{COMPLETION_TIMEOUT, batch_phase::result::records, turn::AdmissionTurn};

#[derive(Debug)]
pub(super) struct Slot {
    pub(super) schedule: ScheduledBatch,
    pub(super) admitted: Instant,
    operation: Pin<Box<SendBatch>>,
    wake: Arc<BatchWake>,
}

#[derive(Debug)]
struct BatchWake {
    local_index: usize,
    sender: SyncSender<usize>,
    queued: AtomicBool,
}

impl Wake for BatchWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self
            .queued
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
            && self.sender.try_send(self.local_index).is_err()
        {
            self.queued.store(false, Ordering::Release);
        }
    }
}

pub(super) fn run(
    producer: &Producer,
    spec: FixedPhaseSpec<'_>,
    schedules: Vec<ScheduledBatch>,
    caller_id: usize,
    budget: usize,
    epoch: Instant,
    admission_turn: &AdmissionTurn,
) -> Result<FixedPhaseResult, Box<dyn Error>> {
    let mut execution = CallerExecution::new(caller_id, schedules.len(), budget, epoch);
    let partitions = usize::try_from(spec.phase.partitions)?;
    for (local_index, schedule) in schedules.into_iter().enumerate() {
        let first = usize::try_from(schedule.first_sequence)?;
        let end = first
            .checked_add(usize::try_from(schedule.count)?)
            .ok_or("fixed-load scheduled batch range overflowed")?;
        let pending = records(first, end, spec.phase, partitions)?;
        execution.wait_until(spec, schedule.intended_ns)?;
        while execution.active_records + pending.len() > execution.budget {
            execution.settle_one(spec)?;
        }
        execution.admit(
            producer,
            spec,
            local_index,
            schedule,
            pending,
            admission_turn,
        )?;
    }
    while execution.active_records > 0 {
        execution.settle_one(spec)?;
    }
    Ok(execution.finish())
}

struct CallerExecution {
    sender: SyncSender<usize>,
    receiver: Receiver<usize>,
    slots: Vec<Option<Slot>>,
    result: FixedPhaseResult,
    active_records: usize,
    budget: usize,
    epoch: Instant,
}

impl CallerExecution {
    fn new(caller_id: usize, slots: usize, budget: usize, epoch: Instant) -> Self {
        let (sender, receiver) = sync_channel(slots.max(1));
        Self {
            sender,
            receiver,
            slots: (0..slots).map(|_| None).collect(),
            result: FixedPhaseResult::for_caller(caller_id, slots),
            active_records: 0,
            budget,
            epoch,
        }
    }

    fn wait_until(
        &mut self,
        spec: FixedPhaseSpec<'_>,
        intended_ns: u64,
    ) -> Result<(), Box<dyn Error>> {
        let due = self
            .epoch
            .checked_add(Duration::from_nanos(intended_ns))
            .ok_or("fixed-load due time overflowed")?;
        loop {
            let now = Instant::now();
            if now >= due {
                return Ok(());
            }
            let wait = due.duration_since(now).min(Duration::from_millis(1));
            if self.active_records == 0 {
                thread::sleep(wait);
            } else if let Ok(local_index) = self.receiver.recv_timeout(wait) {
                self.settle_index(local_index, Some(spec))?;
            }
        }
    }

    fn admit(
        &mut self,
        producer: &Producer,
        spec: FixedPhaseSpec<'_>,
        local_index: usize,
        schedule: ScheduledBatch,
        mut pending: Vec<Record>,
        admission_turn: &AdmissionTurn,
    ) -> Result<(), Box<dyn Error>> {
        loop {
            let permit = admission_turn.wait(schedule.index)?;
            let admitted = Instant::now();
            let operation = producer.send_batch(pending);
            let elapsed = admitted.elapsed();
            let slot = Slot {
                schedule,
                admitted,
                operation: Box::pin(operation),
                wake: Arc::new(BatchWake {
                    local_index,
                    sender: self.sender.clone(),
                    queued: AtomicBool::new(false),
                }),
            };
            match poll_slot(slot) {
                SlotPoll::Pending(slot) => {
                    self.result.batch_admission.record_accepted(elapsed);
                    self.active_records += usize::try_from(schedule.count)?;
                    self.slots[local_index] = Some(slot);
                    permit.complete()?;
                    return Ok(());
                }
                SlotPoll::Complete(slot, batch) => {
                    match result::record_batch(&mut self.result, &slot, batch, self.epoch, spec)? {
                        result::BatchCompletion::Complete => {
                            self.result.batch_admission.record_accepted(elapsed);
                            permit.complete()?;
                            return Ok(());
                        }
                        result::BatchCompletion::Retry(records) => {
                            self.result.batch_admission.record_rejected(elapsed);
                            pending = records;
                            permit.retry();
                            thread::yield_now();
                        }
                    }
                }
            }
        }
    }

    fn settle_one(&mut self, spec: FixedPhaseSpec<'_>) -> Result<(), Box<dyn Error>> {
        loop {
            let local_index = self.receiver.recv_timeout(COMPLETION_TIMEOUT)?;
            if self.settle_index(local_index, Some(spec))? {
                return Ok(());
            }
        }
    }

    fn settle_index(
        &mut self,
        local_index: usize,
        spec: Option<FixedPhaseSpec<'_>>,
    ) -> Result<bool, Box<dyn Error>> {
        let Some(slot) = self
            .slots
            .get_mut(local_index)
            .ok_or("fixed-load completion referenced an invalid slot")?
            .take()
        else {
            return Ok(false);
        };
        match poll_slot(slot) {
            SlotPoll::Pending(slot) => {
                self.slots[local_index] = Some(slot);
                Ok(false)
            }
            SlotPoll::Complete(slot, batch) => {
                self.active_records = self
                    .active_records
                    .saturating_sub(usize::try_from(slot.schedule.count)?);
                let spec = spec.ok_or("fixed-load completion became ready before its due wait")?;
                match result::record_batch(&mut self.result, &slot, batch, self.epoch, spec)? {
                    result::BatchCompletion::Complete => Ok(true),
                    result::BatchCompletion::Retry(_) => {
                        Err("accepted fixed-load batch completed as wholly unadmitted".into())
                    }
                }
            }
        }
    }

    fn finish(mut self) -> FixedPhaseResult {
        self.result.duration = self.epoch.elapsed();
        self.result
    }
}

enum SlotPoll {
    Pending(Slot),
    Complete(Slot, SendBatchResult),
}

fn poll_slot(mut slot: Slot) -> SlotPoll {
    slot.wake.queued.store(false, Ordering::Release);
    let waker = Waker::from(Arc::clone(&slot.wake));
    let mut context = Context::from_waker(&waker);
    match slot.operation.as_mut().poll(&mut context) {
        Poll::Pending => SlotPoll::Pending(slot),
        Poll::Ready(result) => SlotPoll::Complete(slot, result),
    }
}
