//! The single monotonic clock every v2 timestamp is read from.

use std::time::Instant;

/// Nanoseconds since the measured interval began.
///
/// All four of an offer's timestamps are produced here, so every duration this
/// path reports is a subtraction between two readings of one monotonic clock —
/// never a wall clock, and never two clocks compared to each other. Readings
/// saturate instead of wrapping: an instant before the origin reads zero and a
/// run longer than five centuries reads `u64::MAX`, both of which are wrong in
/// a way a reader can see rather than a way that looks plausible.
#[derive(Clone, Copy, Debug)]
pub(super) struct RunClock {
    origin: Instant,
}

impl RunClock {
    /// Starts the clock at `origin`, which becomes nanosecond zero.
    pub(super) const fn starting_at(origin: Instant) -> Self {
        Self { origin }
    }

    /// Reads the clock now.
    pub(super) fn now_ns(self) -> u64 {
        self.at(Instant::now())
    }

    /// Places an already-taken instant on the clock.
    pub(super) fn at(self, instant: Instant) -> u64 {
        u64::try_from(instant.saturating_duration_since(self.origin).as_nanos()).unwrap_or(u64::MAX)
    }

    /// The origin, for callers that must wait until an absolute offset.
    pub(super) const fn origin(self) -> Instant {
        self.origin
    }
}
