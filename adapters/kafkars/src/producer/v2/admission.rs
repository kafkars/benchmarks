//! The admission clock: started once per offer, never restarted.

/// How long one immutable offer waited for the client to take ownership of it.
///
/// # The defect this type exists to make unrepresentable
///
/// The legacy phases timestamped admission *inside* their retry loop:
///
/// ```text
/// loop {
///     let admitted = Instant::now();          // reset on every attempt
///     let operation = producer.send_batch(pending);
///     match poll(operation) {
///         Rejected(records) => { pending = records; continue; }
///         ...
///     }
/// }
/// ```
///
/// An offer that bounced off a full queue nine times and was taken on the
/// tenth therefore reported only the tenth attempt's wait. The nine intervals
/// the application spent under backpressure — exactly the intervals a reader
/// most wants to see — were subtracted from every latency derived from that
/// record, and the busier the client, the more flattering the number became.
///
/// This type has no reset. [`Self::start`] is called once, before the first
/// public-API attempt, and [`Self::rejected`] deliberately records that an
/// attempt failed *without* touching `call_start`, so the admission wait it
/// reports spans every attempt of the same offer.
#[derive(Clone, Copy, Debug)]
pub(super) struct AdmissionClock {
    call_start_ns: u64,
    attempts: u32,
}

impl AdmissionClock {
    /// Starts the clock at the first public-API attempt.
    pub(super) const fn start(call_start_ns: u64) -> Self {
        Self {
            call_start_ns,
            attempts: 1,
        }
    }

    /// When the first attempt began.
    pub(super) const fn call_start_ns(self) -> u64 {
        self.call_start_ns
    }

    /// Public-API attempts made for this offer, including the accepted one.
    pub(super) const fn attempts(self) -> u32 {
        self.attempts
    }

    /// Records that the client refused an attempt and the same offer will be
    /// presented again.
    ///
    /// The offer's identity is unchanged, so its `call_start` is unchanged;
    /// the interval until the next attempt stays inside the admission wait.
    pub(super) const fn rejected(&mut self) {
        self.attempts = self.attempts.saturating_add(1);
    }

    /// The admission wait, spanning every attempt of the same offer.
    pub(super) const fn wait_ns(self, accepted_ns: u64) -> u64 {
        accepted_ns.saturating_sub(self.call_start_ns)
    }
}
