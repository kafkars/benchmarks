//! Sliding batch-native admission and aggregate terminal evidence.

#[cfg(test)]
mod batch_phase_test;
pub(super) mod result;

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

use kafkars::producer::{Producer, SendBatch, SendBatchResult};

use crate::report::ApplicationBatchAdmissionMetrics;

use super::{
    BATCH_RECORDS, COMPLETION_TIMEOUT,
    phase::{PhaseResult, PhaseSpec},
};

use self::result::{BatchCompletion, record_result, records};

#[derive(Debug)]
struct BatchSlot {
    first_sequence: usize,
    offered: usize,
    admitted: Instant,
    operation: Pin<Box<SendBatch>>,
    wake: Arc<BatchWake>,
}

#[derive(Debug)]
struct BatchWake {
    first_sequence: usize,
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
            && self.sender.try_send(self.first_sequence).is_err()
        {
            self.queued.store(false, Ordering::Release);
        }
    }
}

pub(super) fn run_batch_phase(
    producer: &Producer,
    spec: PhaseSpec<'_>,
) -> Result<PhaseResult, Box<dyn Error>> {
    if spec.max_outstanding == 0 {
        return Err("maximum outstanding records must be positive".into());
    }
    let mut execution = BatchExecution::new(spec.records, spec.max_outstanding);
    let partitions = usize::try_from(spec.partitions)?;

    while execution.next_sequence < spec.records || execution.active_records > 0 {
        execution.fill(producer, spec, partitions)?;
        if execution.active_records > 0 {
            execution.settle_one()?;
        }
    }
    Ok(execution.finish())
}

struct BatchExecution {
    sender: SyncSender<usize>,
    receiver: Receiver<usize>,
    slots: Vec<Option<BatchSlot>>,
    result: PhaseResult,
    active_records: usize,
    next_sequence: usize,
    started: Instant,
}

impl BatchExecution {
    fn new(records: usize, max_outstanding: usize) -> Self {
        let (sender, receiver) = sync_channel(max_outstanding);
        Self {
            sender,
            receiver,
            slots: (0..records).map(|_| None).collect(),
            result: PhaseResult {
                accepted: 0,
                acknowledged: 0,
                failed: 0,
                duration: Duration::ZERO,
                latencies: Vec::with_capacity(records),
                samples: Vec::with_capacity(records),
                failure_details: Vec::new(),
                batch_admission: ApplicationBatchAdmissionMetrics::default(),
            },
            active_records: 0,
            next_sequence: 0,
            started: Instant::now(),
        }
    }

    fn fill(
        &mut self,
        producer: &Producer,
        spec: PhaseSpec<'_>,
        partitions: usize,
    ) -> Result<(), Box<dyn Error>> {
        let mut admission_limit = active_limit(spec, partitions, self.next_sequence);
        while self.next_sequence < spec.records && self.active_records < admission_limit {
            let available = admission_limit - self.active_records;
            let offered = BATCH_RECORDS
                .min(available)
                .min(spec.records - self.next_sequence);
            self.admit_chunk(producer, spec, partitions, offered)?;
            admission_limit = active_limit(spec, partitions, self.next_sequence);
        }
        Ok(())
    }

    fn admit_chunk(
        &mut self,
        producer: &Producer,
        spec: PhaseSpec<'_>,
        partitions: usize,
        offered: usize,
    ) -> Result<(), Box<dyn Error>> {
        let first_sequence = self.next_sequence;
        let end_sequence = first_sequence + offered;
        let mut pending = records(first_sequence, end_sequence, spec, partitions)?;
        loop {
            let admitted = Instant::now();
            let operation = producer.send_batch(pending);
            let admission_elapsed = admitted.elapsed();
            let slot = BatchSlot {
                first_sequence,
                offered,
                admitted,
                operation: Box::pin(operation),
                wake: Arc::new(BatchWake {
                    first_sequence,
                    sender: self.sender.clone(),
                    queued: AtomicBool::new(false),
                }),
            };
            match poll_slot(slot) {
                BatchPoll::Pending(slot) => {
                    self.result
                        .batch_admission
                        .record_accepted(admission_elapsed);
                    self.slots[first_sequence] = Some(slot);
                    self.active_records += offered;
                    self.next_sequence = end_sequence;
                    return Ok(());
                }
                BatchPoll::Complete(slot, result) => {
                    match record_result(&mut self.result, &slot, self.started, result)? {
                        BatchCompletion::Complete => {
                            self.result
                                .batch_admission
                                .record_accepted(admission_elapsed);
                            self.next_sequence = end_sequence;
                            return Ok(());
                        }
                        BatchCompletion::Retry(records) => {
                            self.result
                                .batch_admission
                                .record_rejected(admission_elapsed);
                            pending = records;
                            thread::yield_now();
                        }
                    }
                }
            }
        }
    }

    fn settle_one(&mut self) -> Result<(), Box<dyn Error>> {
        loop {
            let first_sequence = self.receiver.recv_timeout(COMPLETION_TIMEOUT)?;
            let Some(slot) = self
                .slots
                .get_mut(first_sequence)
                .ok_or("batch completion referenced an out-of-range sequence")?
                .take()
            else {
                continue;
            };
            match poll_slot(slot) {
                BatchPoll::Pending(slot) => self.slots[first_sequence] = Some(slot),
                BatchPoll::Complete(slot, result) => {
                    self.active_records = self.active_records.saturating_sub(slot.offered);
                    match record_result(&mut self.result, &slot, self.started, result)? {
                        BatchCompletion::Complete => return Ok(()),
                        BatchCompletion::Retry(_records) => {
                            return Err("pending batch completed as wholly unadmitted".into());
                        }
                    }
                }
            }
        }
    }

    fn finish(mut self) -> PhaseResult {
        self.result.duration = self.started.elapsed();
        self.result
    }
}

fn active_limit(spec: PhaseSpec<'_>, partitions: usize, next_sequence: usize) -> usize {
    if spec.prime_partitions && next_sequence < partitions {
        1
    } else {
        spec.max_outstanding
    }
}

enum BatchPoll {
    Pending(BatchSlot),
    Complete(BatchSlot, SendBatchResult),
}

fn poll_slot(mut slot: BatchSlot) -> BatchPoll {
    slot.wake.queued.store(false, Ordering::Release);
    let waker = Waker::from(Arc::clone(&slot.wake));
    let mut context = Context::from_waker(&waker);
    match slot.operation.as_mut().poll(&mut context) {
        Poll::Pending => BatchPoll::Pending(slot),
        Poll::Ready(result) => BatchPoll::Complete(slot, result),
    }
}
