//! The shared count of offers the client currently owns.

use std::sync::atomic::{AtomicU64, Ordering};

/// Offers in flight now, and the most ever in flight at once.
///
/// The fixed-rate phase admits from four callers against one client, so a
/// per-caller maximum would answer a question nobody asked: the sum of four
/// maxima that never coincided overstates the queue, and the largest of them
/// understates it. Two relaxed atomics, updated once per batch rather than
/// once per record, give the actual joint observation for the price of an
/// increment every few hundred offers.
#[derive(Debug, Default)]
pub(super) struct OutstandingGauge {
    current: AtomicU64,
    maximum: AtomicU64,
}

impl OutstandingGauge {
    /// Records that the client took ownership of `count` more offers.
    pub(super) fn admitted(&self, count: u64) {
        let now = self
            .current
            .fetch_add(count, Ordering::Relaxed)
            .saturating_add(count);
        self.maximum.fetch_max(now, Ordering::Relaxed);
    }

    /// Records that `count` offers reached a terminal.
    pub(super) fn settled(&self, count: u64) {
        self.current.fetch_sub(count, Ordering::Relaxed);
    }

    /// Offers the client owns right now.
    pub(super) fn current(&self) -> u64 {
        self.current.load(Ordering::Relaxed)
    }

    /// The most offers the client ever owned at once.
    pub(super) fn maximum(&self) -> u64 {
        self.maximum.load(Ordering::Relaxed)
    }
}
