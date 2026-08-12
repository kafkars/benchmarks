//! Bounded admission, completion waking, and deadline-bounded drain.
//!
//! The structures live here and the behavior lives next door, because every
//! path in this module tree is bounded by the same three allocations and a
//! reader has to be able to see them at once: the slab holds at most one entry
//! per offer the client owns, the completion channel at most one wake per slab
//! entry, and the measurement is four histograms.
//!
//! # Layout
//!
//! - `admit` — the admission path: one offer group across every attempt at it.
//! - `settle` — the completion path: settling, sweeping, and the final drain.
//! - `slot` — a parked group's waker and poll, and how one resolved batch
//!   result reads as an ownership answer.

use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::AtomicBool,
        mpsc::{Receiver, SyncSender},
    },
};

use kafkars::{Producer, SendBatch};

use crate::producer::turn::AdmissionTurn;

use super::{
    clock::RunClock,
    measurement::{Measurement, OfferGroup},
    outstanding::OutstandingGauge,
    slab::OfferSlab,
};

mod admit;
mod settle;
mod slot;

// Reached only from `engine_test`, which pins the diagnosis a refusal no retry
// can clear produces. Everything else about that path is exercised through
// `admit`.
#[cfg(test)]
pub(super) use slot::wholly_refused;

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
