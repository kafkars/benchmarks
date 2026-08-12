//! The shared count of offers the client currently owns, and what they weigh.

use std::sync::atomic::{AtomicU64, Ordering};

/// Offers in flight now, the most ever in flight at once, and the same two
/// figures in payload bytes.
///
/// The fixed-rate phase admits from four callers against one client, so a
/// per-caller maximum would answer a question nobody asked: the sum of four
/// maxima that never coincided overstates the queue, and the largest of them
/// understates it. Four relaxed atomics, updated once per batch rather than
/// once per record, give the actual joint observation for the price of an
/// increment every few hundred offers.
///
/// # Why bytes are accumulated rather than multiplied afterwards
///
/// Under today's payload contract every record in a run is the same size, so
/// the byte high-water and the record high-water move together and multiplying
/// one by the payload size afterwards would give the same answer. It is
/// accumulated at the admit and settle sites anyway, for two reasons. A reader
/// comparing two clients' retained memory should not have to know the payload
/// size to do it — the figure belongs in the document, not in the reader's
/// head. And the moment a payload profile with variable record sizes exists,
/// the multiplication silently becomes wrong while this stays right; a
/// measurement that is correct only because of a property nobody wrote down is
/// a measurement waiting to lie.
#[derive(Debug)]
pub(super) struct OutstandingGauge {
    current: AtomicU64,
    maximum: AtomicU64,
    current_bytes: AtomicU64,
    maximum_bytes: AtomicU64,
    /// Payload bytes one offer carries; the run's fixed record size.
    bytes_per_offer: u64,
}

impl OutstandingGauge {
    /// Creates a gauge for a run whose records carry `bytes_per_offer` bytes.
    pub(super) fn new(bytes_per_offer: u64) -> Self {
        Self {
            current: AtomicU64::new(0),
            maximum: AtomicU64::new(0),
            current_bytes: AtomicU64::new(0),
            maximum_bytes: AtomicU64::new(0),
            bytes_per_offer,
        }
    }

    /// Records that the client took ownership of `count` more offers.
    pub(super) fn admitted(&self, count: u64) {
        let now = self
            .current
            .fetch_add(count, Ordering::Relaxed)
            .saturating_add(count);
        self.maximum.fetch_max(now, Ordering::Relaxed);
        let bytes = self.weigh(count);
        let now_bytes = self
            .current_bytes
            .fetch_add(bytes, Ordering::Relaxed)
            .saturating_add(bytes);
        self.maximum_bytes.fetch_max(now_bytes, Ordering::Relaxed);
    }

    /// Records that `count` offers reached a terminal.
    pub(super) fn settled(&self, count: u64) {
        self.current.fetch_sub(count, Ordering::Relaxed);
        self.current_bytes
            .fetch_sub(self.weigh(count), Ordering::Relaxed);
    }

    /// Offers the client owns right now.
    pub(super) fn current(&self) -> u64 {
        self.current.load(Ordering::Relaxed)
    }

    /// The most offers the client ever owned at once.
    pub(super) fn maximum(&self) -> u64 {
        self.maximum.load(Ordering::Relaxed)
    }

    /// The most payload bytes the client ever owned at once.
    pub(super) fn maximum_bytes(&self) -> u64 {
        self.maximum_bytes.load(Ordering::Relaxed)
    }

    /// What `count` offers weigh in payload bytes.
    fn weigh(&self, count: u64) -> u64 {
        count.saturating_mul(self.bytes_per_offer)
    }
}
