//! Bounded per-record admission, completion waking, and raw latency evidence.

use std::{
    error::Error,
    fs::File,
    future::Future,
    io::{BufWriter, Write},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, SyncSender, sync_channel},
    },
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};

use kafkars::producer::{Producer, Record, Send};

use crate::payload;
use crate::report::ApplicationBatchAdmissionMetrics;

use super::COMPLETION_TIMEOUT;

#[derive(Debug)]
pub(super) struct PhaseResult {
    pub(super) accepted: usize,
    pub(super) acknowledged: usize,
    pub(super) failed: usize,
    pub(super) duration: Duration,
    pub(super) latencies: Vec<u128>,
    pub(super) samples: Vec<LatencySample>,
    pub(super) failure_details: Vec<String>,
    pub(super) batch_admission: ApplicationBatchAdmissionMetrics,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct PhaseSpec<'a> {
    pub(super) topic: &'a str,
    pub(super) run_id: &'a str,
    pub(super) records: usize,
    pub(super) payload_bytes: usize,
    pub(super) partitions: i32,
    pub(super) max_outstanding: usize,
    pub(super) prime_partitions: bool,
}

#[derive(Debug)]
pub(super) struct LatencySample {
    pub(super) sequence: usize,
    pub(super) admitted_ns: u128,
    pub(super) completed_ns: u128,
    pub(super) latency_ns: u128,
}

#[derive(Debug)]
struct Slot {
    sequence: usize,
    admitted: Instant,
    delivery: Pin<Box<Send>>,
    wake: Arc<CompletionWake>,
}

#[derive(Debug)]
struct CompletionWake {
    sequence: usize,
    sender: SyncSender<usize>,
    queued: AtomicBool,
}

impl Wake for CompletionWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self
            .queued
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
            && self.sender.try_send(self.sequence).is_err()
        {
            self.queued.store(false, Ordering::Release);
        }
    }
}

pub(super) fn run_phase(
    producer: &Producer,
    spec: PhaseSpec<'_>,
) -> Result<PhaseResult, Box<dyn Error>> {
    let mut execution = PhaseExecution::new(spec.records, spec.max_outstanding);
    let partitions = usize::try_from(spec.partitions)?;

    for sequence in 0..spec.records {
        let admission_limit = if spec.prime_partitions && sequence < partitions {
            1
        } else {
            spec.max_outstanding
        };
        while execution.active >= admission_limit {
            execution.settle_one()?;
        }
        let record = Record::to(spec.topic)
            .key(u64::try_from(sequence)?.to_be_bytes().to_vec())
            .value(payload::make(
                spec.run_id,
                u64::try_from(sequence)?,
                spec.payload_bytes,
            ))
            .partition(i32::try_from(sequence % partitions)?);
        execution.admit(producer, record, sequence);
    }
    while execution.active > 0 {
        execution.settle_one()?;
    }
    Ok(execution.finish())
}

struct PhaseExecution {
    sender: SyncSender<usize>,
    receiver: Receiver<usize>,
    slots: Vec<Option<Slot>>,
    result: PhaseResult,
    active: usize,
    started: Instant,
}

impl PhaseExecution {
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
            active: 0,
            started: Instant::now(),
        }
    }

    fn admit(&mut self, producer: &Producer, record: Record, sequence: usize) {
        let admitted = Instant::now();
        let delivery = producer.send(record);
        self.result.accepted += 1;
        let slot = Slot {
            sequence,
            admitted,
            delivery: Box::pin(delivery),
            wake: Arc::new(CompletionWake {
                sequence,
                sender: self.sender.clone(),
                queued: AtomicBool::new(false),
            }),
        };
        match poll_slot(slot, self.started, &mut self.result) {
            SlotPoll::Pending(slot) => {
                self.slots[sequence] = Some(slot);
                self.active += 1;
            }
            SlotPoll::Complete(Some(sample)) => self.result.samples.push(sample),
            SlotPoll::Complete(None) => {}
        }
    }

    fn settle_one(&mut self) -> Result<(), Box<dyn Error>> {
        loop {
            let sequence = self.receiver.recv_timeout(COMPLETION_TIMEOUT)?;
            let Some(slot) = self
                .slots
                .get_mut(sequence)
                .ok_or("completion referenced an out-of-range sequence")?
                .take()
            else {
                continue;
            };
            match poll_slot(slot, self.started, &mut self.result) {
                SlotPoll::Complete(sample) => {
                    self.result.samples.extend(sample);
                    self.active = self.active.saturating_sub(1);
                    return Ok(());
                }
                SlotPoll::Pending(slot) => self.slots[sequence] = Some(slot),
            }
        }
    }

    fn finish(mut self) -> PhaseResult {
        self.result.duration = self.started.elapsed();
        self.result
    }
}

enum SlotPoll {
    Pending(Slot),
    Complete(Option<LatencySample>),
}

fn poll_slot(mut slot: Slot, started: Instant, result: &mut PhaseResult) -> SlotPoll {
    slot.wake.queued.store(false, Ordering::Release);
    let waker = Waker::from(Arc::clone(&slot.wake));
    let mut context = Context::from_waker(&waker);
    match slot.delivery.as_mut().poll(&mut context) {
        Poll::Ready(Ok(_metadata)) => complete(&slot, started, result),
        Poll::Ready(Err(error)) => {
            result.failed += 1;
            result.failure_details.push(format!(
                "sequence {} kind={:?} delivery={:?} broker_code={:?} retry={:?} fatal={} message={error}",
                slot.sequence,
                error.kind(),
                error.delivery_status(),
                error.broker_code(),
                error.retry_advice(),
                error.is_fatal(),
            ));
            SlotPoll::Complete(None)
        }
        Poll::Pending => SlotPoll::Pending(slot),
    }
}

fn complete(slot: &Slot, started: Instant, result: &mut PhaseResult) -> SlotPoll {
    let completed = Instant::now();
    result.acknowledged += 1;
    result
        .latencies
        .push(completed.duration_since(slot.admitted).as_nanos());
    SlotPoll::Complete(Some(LatencySample {
        sequence: slot.sequence,
        admitted_ns: slot.admitted.duration_since(started).as_nanos(),
        completed_ns: completed.duration_since(started).as_nanos(),
        latency_ns: completed.duration_since(slot.admitted).as_nanos(),
    }))
}

pub(super) fn write_latencies(
    path: &std::path::Path,
    samples: &[LatencySample],
) -> Result<(), Box<dyn Error>> {
    let mut output = BufWriter::new(File::create(path)?);
    writeln!(output, "sequence,admitted_ns,completed_ns,latency_ns")?;
    for sample in samples {
        writeln!(
            output,
            "{},{},{},{}",
            sample.sequence, sample.admitted_ns, sample.completed_ns, sample.latency_ns
        )?;
    }
    output.flush()?;
    Ok(())
}
