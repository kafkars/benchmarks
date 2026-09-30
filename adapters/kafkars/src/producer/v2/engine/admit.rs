//! The admission path: one offer group, across every attempt at admitting it.
//!
//! The engine's allocation happens here too, because the budget that bounds
//! admission is the same number that sizes the slab and the completion
//! channel.

use std::{
    error::Error,
    sync::{Arc, atomic::AtomicBool, mpsc::sync_channel},
    thread,
};

use kafkars::producer::Record;

use crate::producer::{
    turn::AdmissionPermit,
    v2::{
        PhaseContext,
        admission::AdmissionClock,
        measurement::{Measurement, OfferAttempt, OfferGroup},
        slab::OfferSlab,
    },
};

use super::{
    AdmissionOrder, GroupWake, OfferEngine, Slot,
    slot::{Admission, SlotPoll, classify, poll_slot},
};

impl<'a> OfferEngine<'a> {
    /// Allocates every bounded structure this phase will use.
    pub(in crate::producer::v2) fn new(
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
    pub(in crate::producer::v2) const fn active_offers(&self) -> u64 {
        self.active_offers
    }

    /// The offer budget this engine admits within.
    pub(in crate::producer::v2) const fn budget(&self) -> u64 {
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
    ///
    /// # Where the two brackets start
    ///
    /// The caller's own outstanding-budget wait is *over* by the time this is
    /// entered, and the stamp below is the first thing that happens; everything
    /// after it — the submission-order turn, the client's call, every
    /// queue-full retry of the same offer — is inside the admission wait.
    /// `docs/EVIDENCE.md` states that bracket normatively and the librdkafka
    /// adapter takes its stamp at the same point
    /// (`v2_phase.c: await_budget_then_stamp`), so the two adapters'
    /// `call_start_to_accepted` and `intended_to_call_start` divide the same
    /// wall-clock interval in the same place.
    ///
    /// The offer is counted as *offered* immediately after the stamp, while the
    /// client has still said nothing about it, so a refused or failed admission
    /// leaves `offered` above `accepted` rather than agreeing with it because
    /// the same line recorded both.
    pub(in crate::producer::v2) fn admit(
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
        // The bracket opens here, before the engine's own bookkeeping, because
        // the C adapter reserves its slab entry inside the bracket too. What
        // the harness spends between the budget gate and the client's call is
        // the harness's to account for, in both adapters.
        let mut attempt = OfferAttempt {
            first_sequence,
            count,
            admission: AdmissionClock::start(self.clock.now_ns()),
        };
        let index = self.slab.reserve()?;
        let wake = Arc::new(GroupWake {
            index,
            sender: self.sender.clone(),
            queued: AtomicBool::new(false),
        });
        self.measurement.record_offered(&attempt)?;
        let mut pending = records;
        loop {
            let permit = order.wait()?;
            let operation = self.producer.send_batch(pending);
            let accepted_ns = self.clock.now_ns();
            let group = OfferGroup {
                attempt,
                accepted_ns,
            };
            let slot = Slot {
                group,
                operation: Box::pin(operation),
                wake: Arc::clone(&wake),
            };
            match poll_slot(slot) {
                SlotPoll::Parked(slot) => {
                    self.slab.store(index, slot)?;
                    self.active_offers = self.active_offers.saturating_add(count);
                    self.outstanding.admitted(count);
                    // The turn is released before the histograms are touched:
                    // the client has the bytes, so the next caller's admission
                    // may begin, and the cost of recording this group must not
                    // be charged to that caller's admission wait.
                    complete(permit)?;
                    self.measurement.record_accepted(&group);
                    return Ok(());
                }
                SlotPoll::Ready(result) => match classify(result, count)? {
                    Admission::Accepted(deliveries) => {
                        let terminal_ns = self.clock.now_ns();
                        self.slab.release(index)?;
                        // The client owned this group, however briefly, so the
                        // outstanding gauge sees it: `max_outstanding_observed`
                        // must be the same statistic in both adapters, and the
                        // C adapter counts an offer from the moment its slot is
                        // reserved.
                        self.outstanding.admitted(count);
                        self.outstanding.settled(count);
                        complete(permit)?;
                        self.measurement.record_accepted(&group);
                        self.record_terminals(&group, terminal_ns, &deliveries)?;
                        return Ok(());
                    }
                    Admission::Refused(returned) => {
                        attempt.admission.rejected();
                        self.measurement.record_refusal();
                        pending = returned;
                        abandon(permit);
                        thread::yield_now();
                    }
                },
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
