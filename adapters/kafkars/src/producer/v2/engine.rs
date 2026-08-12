//! Bounded admission, completion waking, and deadline-bounded drain.

use std::{
    error::Error,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, RecvTimeoutError, SyncSender, sync_channel},
    },
    task::{Context, Poll, Wake, Waker},
    thread,
    time::{Duration, Instant},
};

use kafkars::{
    ErrorKind, KafkaError, Producer, Record, RecordMetadata, SendBatch, SendBatchResult,
};

use crate::producer::turn::{AdmissionPermit, AdmissionTurn};

use super::{
    PhaseContext,
    admission::AdmissionClock,
    clock::RunClock,
    measurement::{Measurement, OfferGroup, Terminal},
    outstanding::OutstandingGauge,
    slab::OfferSlab,
};

/// How a caller's public admission calls are ordered against other callers.
#[derive(Clone, Copy, Debug)]
pub(super) enum AdmissionOrder<'a> {
    /// The only caller admits in its own order.
    Single,
    /// Callers take turns, so the public call sequence is the schedule's.
    Linearized(&'a AdmissionTurn, u64),
}

/// One offer group parked in the slab while the client owns it.
#[derive(Debug)]
struct Slot {
    group: OfferGroup,
    operation: Pin<Box<SendBatch>>,
    wake: Arc<GroupWake>,
}

/// The waker one parked group hands the client.
#[derive(Debug)]
struct GroupWake {
    index: usize,
    sender: SyncSender<usize>,
    queued: AtomicBool,
}

impl Wake for GroupWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self
            .queued
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
            && self.sender.try_send(self.index).is_err()
        {
            self.queued.store(false, Ordering::Release);
        }
    }
}

/// The offer engine: it owns the budget, the slab, and the measurement.
///
/// Every path through this type is bounded by the offer budget. The slab holds
/// at most one entry per offer the client owns, the completion channel holds at
/// most one wake per slab entry, and the measurement is four histograms. What
/// is *not* here is as important: no per-record vector, no sample list, and no
/// table indexed by sequence number.
#[derive(Debug)]
pub(super) struct OfferEngine<'a> {
    producer: &'a Producer,
    clock: RunClock,
    outstanding: &'a OutstandingGauge,
    slab: OfferSlab<Slot>,
    sender: SyncSender<usize>,
    receiver: Receiver<usize>,
    measurement: Measurement,
    active_offers: u64,
    budget: u64,
}

impl<'a> OfferEngine<'a> {
    /// Allocates every bounded structure this phase will use.
    pub(super) fn new(
        context: &PhaseContext<'a>,
        budget: u64,
        measurement: Measurement,
    ) -> Result<Self, Box<dyn Error>> {
        if budget == 0 {
            return Err("an offer budget must be positive".into());
        }
        let capacity = usize::try_from(budget)?;
        let (sender, receiver) = sync_channel(capacity);
        Ok(Self {
            producer: context.producer,
            clock: context.clock,
            outstanding: context.outstanding,
            slab: OfferSlab::with_capacity(capacity),
            sender,
            receiver,
            measurement,
            active_offers: 0,
            budget,
        })
    }

    /// Offers the client owns right now, from this engine.
    pub(super) const fn active_offers(&self) -> u64 {
        self.active_offers
    }

    /// The offer budget this engine admits within.
    pub(super) const fn budget(&self) -> u64 {
        self.budget
    }

    /// Offers one group, retrying a wholly refused offer without restarting
    /// its admission clock.
    ///
    /// This is the P0 fix in one place: `call_start` is taken once, before the
    /// first `send_batch`, and the loop below can only reach it through
    /// [`AdmissionClock::rejected`], which has no way to move it. Everything
    /// the engine records about the group afterwards — the offered count, the
    /// scheduler lateness, the admission wait — is derived from that one
    /// immutable value, so none of it can be recomputed against a later
    /// attempt.
    pub(super) fn admit(
        &mut self,
        records: Vec<Record>,
        first_sequence: u64,
        order: AdmissionOrder<'_>,
    ) -> Result<(), Box<dyn Error>> {
        let count = u64::try_from(records.len())?;
        if count == 0 {
            return Err("an offer group must carry at least one record".into());
        }
        if self.active_offers.saturating_add(count) > self.budget {
            return Err("an offer group was admitted past the offer budget".into());
        }
        let index = self.slab.reserve()?;
        let wake = Arc::new(GroupWake {
            index,
            sender: self.sender.clone(),
            queued: AtomicBool::new(false),
        });
        let mut pending = records;
        let mut admission = AdmissionClock::start(self.clock.now_ns());
        loop {
            let permit = order.wait()?;
            let operation = self.producer.send_batch(pending);
            let accepted_ns = self.clock.now_ns();
            let group = OfferGroup {
                first_sequence,
                count,
                admission,
                accepted_ns,
            };
            let slot = Slot {
                group,
                operation: Box::pin(operation),
                wake: Arc::clone(&wake),
            };
            match poll_slot(slot) {
                SlotPoll::Parked(slot) => {
                    self.measurement.record_offered(&group)?;
                    self.measurement.record_accepted(&group);
                    self.slab.store(index, slot)?;
                    self.active_offers = self.active_offers.saturating_add(count);
                    self.outstanding.admitted(count);
                    return complete(permit);
                }
                SlotPoll::Ready(result) => match classify(result, count)? {
                    Admission::Accepted(deliveries) => {
                        let terminal_ns = self.clock.now_ns();
                        self.slab.release(index)?;
                        self.measurement.record_offered(&group)?;
                        self.measurement.record_accepted(&group);
                        self.record_terminals(&group, terminal_ns, &deliveries)?;
                        return complete(permit);
                    }
                    Admission::Refused(returned) => {
                        admission.rejected();
                        pending = returned;
                        abandon(permit);
                        thread::yield_now();
                    }
                },
            }
        }
    }

    /// Settles one group, or reports that the client did not settle in time.
    pub(super) fn settle_within(&mut self, timeout: Duration) -> Result<(), Box<dyn Error>> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or("an offer completion deadline overflowed")?;
        if self.settle(deadline)? || self.sweep()? {
            return Ok(());
        }
        Err(format!(
            "the client settled no offer group within {} seconds, with {} offers outstanding",
            timeout.as_secs(),
            self.active_offers
        )
        .into())
    }

    /// Waits until `offset_ns` on the run clock, settling groups meanwhile.
    pub(super) fn wait_until(&mut self, offset_ns: u64) -> Result<(), Box<dyn Error>> {
        let due = self
            .clock
            .origin()
            .checked_add(Duration::from_nanos(offset_ns))
            .ok_or("a scheduled offer time overflowed")?;
        loop {
            let now = Instant::now();
            if now >= due {
                return Ok(());
            }
            let wait = due.duration_since(now).min(Duration::from_millis(1));
            if self.active_offers == 0 {
                thread::sleep(wait);
            } else {
                let deadline = now
                    .checked_add(wait)
                    .ok_or("a scheduled settle deadline overflowed")?;
                self.settle(deadline)?;
            }
        }
    }

    /// Waits out the outstanding offers, then counts what never resolved.
    ///
    /// The deadline is the client's own delivery timeout: past it, a record
    /// that has not reached a terminal is not going to. Those offers are
    /// counted as `unknown` rather than assumed lost or assumed delivered,
    /// because the adapter genuinely does not know which, and they stay
    /// visible as the run's final outstanding depth.
    pub(super) fn drain(&mut self, timeout: Duration) -> Result<(), Box<dyn Error>> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or("the drain deadline overflowed")?;
        while self.active_offers > 0 && self.settle(deadline)? {}
        while self.active_offers > 0 && self.sweep()? {}
        self.measurement.record_unknown(self.active_offers);
        Ok(())
    }

    /// Surrenders the accumulated evidence.
    pub(super) fn finish(self) -> Measurement {
        self.measurement
    }

    /// Polls every parked group, settling any the client already finished.
    ///
    /// A completion wake can be lost. The waker's queued flag belongs to one
    /// group, a slab index outlives the group that reserved it, and a full
    /// channel drops a notification rather than blocking the client's own
    /// thread — so a stale wake can occupy the slot a live one needed. None of
    /// that may turn into a hung phase, or into an offer reported as `unknown`
    /// that in fact reached a terminal, so both paths that were about to give
    /// up look for themselves first. The cost is one poll per outstanding
    /// group, on paths that only run when nothing arrived for a full
    /// completion timeout.
    fn sweep(&mut self) -> Result<bool, Box<dyn Error>> {
        let mut settled = false;
        for index in 0..self.slab.capacity() {
            let Some(slot) = self.slab.take(index)? else {
                continue;
            };
            let group = slot.group;
            match poll_slot(slot) {
                SlotPoll::Parked(slot) => self.slab.store(index, slot)?,
                SlotPoll::Ready(result) => {
                    let terminal_ns = self.clock.now_ns();
                    self.slab.release(index)?;
                    self.active_offers = self.active_offers.saturating_sub(group.count);
                    self.outstanding.settled(group.count);
                    match classify(result, group.count)? {
                        Admission::Accepted(deliveries) => {
                            self.record_terminals(&group, terminal_ns, &deliveries)?;
                            settled = true;
                        }
                        Admission::Refused(_) => {
                            return Err(
                                "an owned offer group completed as wholly unadmitted".into()
                            );
                        }
                    }
                }
            }
        }
        Ok(settled)
    }

    /// Records one group's aggregate terminal, diagnosing the first failure.
    fn record_terminals(
        &mut self,
        group: &OfferGroup,
        terminal_ns: u64,
        deliveries: &[Result<RecordMetadata, KafkaError>],
    ) -> Result<(), Box<dyn Error>> {
        for (index, delivery) in deliveries.iter().enumerate() {
            if let Err(error) = delivery {
                let sequence = group
                    .first_sequence
                    .saturating_add(u64::try_from(index).unwrap_or(u64::MAX));
                self.measurement.note_failure(sequence, error);
            }
        }
        self.measurement
            .record_terminals(group, terminal_ns, deliveries.iter().map(Terminal::of))
    }

    /// Settles one group, returning `false` when `deadline` passed first.
    fn settle(&mut self, deadline: Instant) -> Result<bool, Box<dyn Error>> {
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Ok(false);
            }
            let index = match self.receiver.recv_timeout(deadline.duration_since(now)) {
                Ok(index) => index,
                Err(RecvTimeoutError::Timeout) => return Ok(false),
                Err(RecvTimeoutError::Disconnected) => {
                    return Err("the offer completion channel closed".into());
                }
            };
            let Some(slot) = self.slab.take(index)? else {
                continue;
            };
            let group = slot.group;
            match poll_slot(slot) {
                SlotPoll::Parked(slot) => self.slab.store(index, slot)?,
                SlotPoll::Ready(result) => {
                    let terminal_ns = self.clock.now_ns();
                    self.slab.release(index)?;
                    self.active_offers = self.active_offers.saturating_sub(group.count);
                    self.outstanding.settled(group.count);
                    match classify(result, group.count)? {
                        Admission::Accepted(deliveries) => {
                            self.record_terminals(&group, terminal_ns, &deliveries)?;
                            return Ok(true);
                        }
                        Admission::Refused(_) => {
                            return Err(
                                "an owned offer group completed as wholly unadmitted".into()
                            );
                        }
                    }
                }
            }
        }
    }
}

impl<'a> AdmissionOrder<'a> {
    /// Takes this caller's turn, when callers take turns.
    fn wait(self) -> Result<Option<AdmissionPermit<'a>>, Box<dyn Error>> {
        match self {
            Self::Single => Ok(None),
            Self::Linearized(turn, index) => turn.wait(index).map(Some),
        }
    }
}

/// Releases a turn to the next batch index.
fn complete(permit: Option<AdmissionPermit<'_>>) -> Result<(), Box<dyn Error>> {
    match permit {
        Some(permit) => permit.complete(),
        None => Ok(()),
    }
}

/// Keeps this caller's turn for another attempt at the same offer.
fn abandon(permit: Option<AdmissionPermit<'_>>) {
    if let Some(permit) = permit {
        permit.retry();
    }
}

/// What one resolved `send_batch` said about ownership.
enum Admission {
    /// The client took the whole group; these are its terminals.
    Accepted(Vec<Result<RecordMetadata, KafkaError>>),
    /// The client took nothing and handed the exact records back.
    Refused(Vec<Record>),
}

/// Reads one batch result as an ownership answer.
///
/// A partial admission is refused rather than reinterpreted: its unaccepted
/// suffix could only be re-offered after later records had already crossed the
/// boundary, which would silently reorder the run.
fn classify(result: SendBatchResult, count: u64) -> Result<Admission, Box<dyn Error>> {
    let (deliveries, rejection) = result.into_parts();
    let accepted = u64::try_from(deliveries.len())?;
    match rejection {
        None if accepted == count => Ok(Admission::Accepted(deliveries)),
        None => Err(format!(
            "an offer group of {count} reported {accepted} terminals and no rejection"
        )
        .into()),
        Some(rejection)
            if deliveries.is_empty() && rejection.error().kind() == ErrorKind::Backpressure =>
        {
            let (records, _error) = rejection.into_parts();
            Ok(Admission::Refused(records))
        }
        Some(rejection) => {
            let (records, error) = rejection.into_parts();
            Err(format!(
                "an offer group of {count} was partially admitted: {accepted} accepted, {} \
                 returned, after later offers may have crossed admission: {error}",
                records.len(),
            )
            .into())
        }
    }
}

/// What one poll of a parked group said.
enum SlotPoll {
    /// The client still owns the group.
    Parked(Slot),
    /// The group resolved.
    Ready(SendBatchResult),
}

/// Polls one group against its own waker.
fn poll_slot(mut slot: Slot) -> SlotPoll {
    slot.wake.queued.store(false, Ordering::Release);
    let waker = Waker::from(Arc::clone(&slot.wake));
    let mut context = Context::from_waker(&waker);
    match slot.operation.as_mut().poll(&mut context) {
        Poll::Pending => SlotPoll::Parked(slot),
        Poll::Ready(result) => SlotPoll::Ready(result),
    }
}
