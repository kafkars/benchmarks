//! The completion path: settling one group, sweeping for a lost wake, and the
//! deadline-bounded drain that ends a phase.

use std::{
    error::Error,
    sync::mpsc::RecvTimeoutError,
    thread,
    time::{Duration, Instant},
};

use kafkars::{KafkaError, RecordMetadata};

use crate::producer::{
    flush_until,
    v2::measurement::{Measurement, OfferGroup, Terminal},
};

use super::{
    OfferEngine,
    slot::{Admission, SlotPoll, classify, poll_slot},
};

impl OfferEngine<'_> {
    /// Settles one group, or reports that the client did not settle in time.
    pub(in crate::producer::v2) fn settle_within(
        &mut self,
        timeout: Duration,
    ) -> Result<(), Box<dyn Error>> {
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
    pub(in crate::producer::v2) fn wait_until(
        &mut self,
        offset_ns: u64,
    ) -> Result<(), Box<dyn Error>> {
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

    /// Waits out the outstanding offers, flushes the client, then counts what
    /// never resolved.
    ///
    /// The deadline is the client's own delivery timeout: past it, a record
    /// that has not reached a terminal is not going to. Those offers are
    /// counted as `unknown` rather than assumed lost or assumed delivered,
    /// because the adapter genuinely does not know which, and they stay
    /// visible as the run's final outstanding depth.
    ///
    /// # Why the flush is here and not after
    ///
    /// The phase used to snapshot `unknown` and then flush, so records the
    /// flush was about to deliver had already been counted as offers with no
    /// terminal — the adapter blamed the client for work it had not yet asked
    /// it to finish. The flush therefore happens first, inside the same
    /// deadline rather than after it (a 60-second drain followed by a
    /// 65-second flush is a 125-second bound nobody chose), and `unknown` is
    /// whatever is genuinely left afterwards. The librdkafka adapter drains in
    /// the same order: `rd_kafka_flush`, then the snapshot under the lock.
    pub(in crate::producer::v2) fn drain(
        &mut self,
        timeout: Duration,
    ) -> Result<(), Box<dyn Error>> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or("the drain deadline overflowed")?;
        while self.active_offers > 0 && self.settle(deadline)? {}
        flush_until(self.producer, deadline)?;
        while self.active_offers > 0 && self.settle(deadline)? {}
        // One sweep always runs, because a lost wake is exactly the case this
        // path exists for; further sweeps only while the drain budget lasts,
        // since each one polls every slab entry and past the deadline the
        // answer is `unknown` either way.
        while self.active_offers > 0 && self.sweep()? {
            if Instant::now() >= deadline {
                break;
            }
        }
        self.measurement.record_unknown(self.active_offers);
        Ok(())
    }

    /// Surrenders the accumulated evidence.
    pub(in crate::producer::v2) fn finish(self) -> Measurement {
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
                    self.active_offers = self.active_offers.saturating_sub(group.attempt.count);
                    self.outstanding.settled(group.attempt.count);
                    match classify(result, group.attempt.count)? {
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
    pub(super) fn record_terminals(
        &mut self,
        group: &OfferGroup,
        terminal_ns: u64,
        deliveries: &[Result<RecordMetadata, KafkaError>],
    ) -> Result<(), Box<dyn Error>> {
        for (index, delivery) in deliveries.iter().enumerate() {
            if let Err(error) = delivery {
                let sequence = group
                    .attempt
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
                    self.active_offers = self.active_offers.saturating_sub(group.attempt.count);
                    self.outstanding.settled(group.attempt.count);
                    match classify(result, group.attempt.count)? {
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
