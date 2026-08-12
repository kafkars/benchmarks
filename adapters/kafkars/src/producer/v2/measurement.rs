//! Bounded evidence: four histograms, six counters, and nothing per record.

use std::error::Error;

use bench_schema::{Histogram, OfferOutcomes, OfferTiming};
use kafkars::{ErrorKind, KafkaError, RecordMetadata};

use crate::schedule;

use super::admission::AdmissionClock;

/// The clock discriminator every v2 duration carries.
const CLOCK: &str = "monotonic-ns";

/// Where one offer ended.
///
/// The engine reads this off a client delivery; everything downstream counts
/// `Terminal`s, so the accounting can be exercised without a broker and
/// without fabricating client types a test has no way to build.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Terminal {
    /// The broker acknowledged the record.
    Acknowledged,
    /// The record reached a failure terminal.
    Failed,
    /// The record's delivery deadline elapsed.
    TimedOut,
}

impl Terminal {
    /// Reads one delivery outcome.
    pub(super) fn of(delivery: &Result<RecordMetadata, KafkaError>) -> Self {
        match delivery {
            Ok(_metadata) => Self::Acknowledged,
            Err(error) if error.kind() == ErrorKind::Timeout => Self::TimedOut,
            Err(_error) => Self::Failed,
        }
    }
}

/// One offer group's immutable identity, as it stands when its public call
/// begins.
///
/// This carries no `accepted_ns` because at this point there is none, and that
/// absence is the point: everything the measurement records about an *offer* —
/// the offered count, the scheduler lateness, the attempts — is derivable from
/// this type alone, so none of it can be made conditional on the client having
/// said yes.
#[derive(Clone, Copy, Debug)]
pub(super) struct OfferAttempt {
    /// Sequence of the first offer in the group.
    pub(super) first_sequence: u64,
    /// Offers the group carries.
    pub(super) count: u64,
    /// The admission clock, started at the first attempt and never restarted.
    pub(super) admission: AdmissionClock,
}

/// One offer group the client took, and when it took it.
#[derive(Clone, Copy, Debug)]
pub(super) struct OfferGroup {
    /// The identity and admission clock the attempt began with.
    pub(super) attempt: OfferAttempt,
    /// When the call that transferred ownership returned.
    pub(super) accepted_ns: u64,
}

/// Everything the v2 document reports, accumulated in space that does not grow
/// with the run.
///
/// The histograms are the whole of the latency evidence: no sorted array of
/// samples, no percentile computed here. A reader derives percentiles from the
/// buckets, which is the only way two adapters in two languages can be held to
/// the same answer.
#[derive(Debug)]
pub(super) struct Measurement {
    rate: Option<u64>,
    intended_to_terminal: Histogram,
    accepted_to_terminal: Histogram,
    call_start_to_accepted: Histogram,
    intended_to_call_start: Option<Histogram>,
    outcomes: OfferOutcomes,
    admission_attempts: u64,
    first_failure: Option<String>,
}

impl Measurement {
    /// A measurement whose offers have no schedule, so `intended` is
    /// `call_start`.
    pub(super) fn closed_loop() -> Self {
        Self::new(None)
    }

    /// A measurement whose offers are due at the canonical open-loop schedule.
    pub(super) fn scheduled(offered_records_per_second: u64) -> Self {
        Self::new(Some(offered_records_per_second))
    }

    fn new(rate: Option<u64>) -> Self {
        Self {
            rate,
            intended_to_terminal: Histogram::new(),
            accepted_to_terminal: Histogram::new(),
            call_start_to_accepted: Histogram::new(),
            intended_to_call_start: rate.map(|_| Histogram::new()),
            outcomes: OfferOutcomes {
                offered: 0,
                accepted: 0,
                acknowledged: 0,
                failed: 0,
                timed_out: 0,
                unknown: 0,
            },
            admission_attempts: 0,
            first_failure: None,
        }
    }

    /// Records that a group's public-API attempt began, and how late it was.
    ///
    /// Called once per group, at its *first* public call and before the client
    /// has answered it, which is what keeps `offered` from being a second name
    /// for `accepted`: a group the client refuses outright leaves `offered`
    /// above `accepted` rather than agreeing with it by construction. Every
    /// further attempt at the same offer is counted by [`Self::record_refusal`]
    /// and adds no offered record, because the offer's identity has not
    /// changed.
    ///
    /// The lateness of a 256-record group costs 256 divisions and 256 bucket
    /// updates, and that cost now lands inside the group's own admission wait.
    /// It has to: lateness is `call_start - intended`, so it cannot be computed
    /// before `call_start` exists, and deferring it until the call returns
    /// would make the sample's existence depend on the answer. The librdkafka
    /// adapter pays the identical cost at the identical point
    /// (`v2_phase.c: offer_batch`), so the two adapters' admission waits carry
    /// the same overhead rather than differing by it.
    pub(super) fn record_offered(&mut self, attempt: &OfferAttempt) -> Result<(), Box<dyn Error>> {
        self.outcomes.offered = self.outcomes.offered.saturating_add(attempt.count);
        self.admission_attempts = self
            .admission_attempts
            .saturating_add(u64::from(attempt.admission.attempts()));
        let Some(lateness) = self.intended_to_call_start.as_mut() else {
            return Ok(());
        };
        let call_start_ns = attempt.admission.call_start_ns();
        for sequence in sequences(attempt) {
            let intended_ns = intended_ns(self.rate, sequence, call_start_ns)?;
            lateness.record(call_start_ns.saturating_sub(intended_ns));
        }
        Ok(())
    }

    /// Records one more public call at the same offer, after a refusal.
    ///
    /// The offer is not re-offered — it is the same offer, with the same
    /// `intended` and the same `call_start` — so this moves the attempt count
    /// and nothing else.
    pub(super) const fn record_refusal(&mut self) {
        self.admission_attempts = self.admission_attempts.saturating_add(1);
    }

    /// Records that the client took ownership of a group.
    ///
    /// The wait spans every attempt of the same offer, because
    /// [`AdmissionClock`] cannot be restarted.
    pub(super) fn record_accepted(&mut self, group: &OfferGroup) {
        self.outcomes.accepted = self.outcomes.accepted.saturating_add(group.attempt.count);
        let wait_ns = group.attempt.admission.wait_ns(group.accepted_ns);
        for _ in 0..group.attempt.count {
            self.call_start_to_accepted.record(wait_ns);
        }
    }

    /// Records a group's aggregate terminal, one outcome per offer.
    pub(super) fn record_terminals(
        &mut self,
        group: &OfferGroup,
        terminal_ns: u64,
        terminals: impl ExactSizeIterator<Item = Terminal>,
    ) -> Result<(), Box<dyn Error>> {
        if u64::try_from(terminals.len())? != group.attempt.count {
            return Err(format!(
                "an offer group of {} carried {} terminals",
                group.attempt.count,
                terminals.len()
            )
            .into());
        }
        let call_start_ns = group.attempt.admission.call_start_ns();
        for (sequence, terminal) in sequences(&group.attempt).zip(terminals) {
            let intended_ns = intended_ns(self.rate, sequence, call_start_ns)?;
            self.intended_to_terminal
                .record(terminal_ns.saturating_sub(intended_ns));
            self.accepted_to_terminal
                .record(terminal_ns.saturating_sub(group.accepted_ns));
            match terminal {
                Terminal::Acknowledged => {
                    self.outcomes.acknowledged = self.outcomes.acknowledged.saturating_add(1);
                }
                Terminal::Failed => {
                    self.outcomes.failed = self.outcomes.failed.saturating_add(1);
                }
                Terminal::TimedOut => {
                    self.outcomes.timed_out = self.outcomes.timed_out.saturating_add(1);
                }
            }
        }
        Ok(())
    }

    /// Keeps the first failure's diagnosis, so an invalid run says why.
    ///
    /// Only the first is kept. A run that fails ten thousand records the same
    /// way does not need ten thousand strings to explain itself, and a growing
    /// list of them would be another way for evidence to scale with the run.
    pub(super) fn note_failure(&mut self, sequence: u64, error: &KafkaError) {
        if self.first_failure.is_some() {
            return;
        }
        self.first_failure = Some(format!(
            "sequence {sequence} kind={:?} delivery={:?} broker_code={:?} retry={:?} \
             fatal={} message={error}",
            error.kind(),
            error.delivery_status(),
            error.broker_code(),
            error.retry_advice(),
            error.is_fatal(),
        ));
    }

    /// Records offers the client owned that never reached a terminal.
    pub(super) fn record_unknown(&mut self, count: u64) {
        self.outcomes.unknown = self.outcomes.unknown.saturating_add(count);
    }

    /// Absorbs another caller's measurement of the same phase.
    pub(super) fn merge(&mut self, other: &Self) {
        self.intended_to_terminal.merge(&other.intended_to_terminal);
        self.accepted_to_terminal.merge(&other.accepted_to_terminal);
        self.call_start_to_accepted
            .merge(&other.call_start_to_accepted);
        if let (Some(mine), Some(theirs)) = (
            self.intended_to_call_start.as_mut(),
            other.intended_to_call_start.as_ref(),
        ) {
            mine.merge(theirs);
        }
        let outcomes = &mut self.outcomes;
        outcomes.offered = outcomes.offered.saturating_add(other.outcomes.offered);
        outcomes.accepted = outcomes.accepted.saturating_add(other.outcomes.accepted);
        outcomes.acknowledged = outcomes
            .acknowledged
            .saturating_add(other.outcomes.acknowledged);
        outcomes.failed = outcomes.failed.saturating_add(other.outcomes.failed);
        outcomes.timed_out = outcomes.timed_out.saturating_add(other.outcomes.timed_out);
        outcomes.unknown = outcomes.unknown.saturating_add(other.outcomes.unknown);
        self.admission_attempts = self
            .admission_attempts
            .saturating_add(other.admission_attempts);
        if self.first_failure.is_none() {
            self.first_failure.clone_from(&other.first_failure);
        }
    }

    /// Where every offer ended.
    pub(super) const fn outcomes(&self) -> OfferOutcomes {
        self.outcomes
    }

    /// Public-API attempts made, including the accepted one per group.
    pub(super) const fn admission_attempts(&self) -> u64 {
        self.admission_attempts
    }

    /// The first failure this phase saw, kept so an invalid run says why.
    pub(super) fn first_failure(&self) -> Option<&str> {
        self.first_failure.as_deref()
    }

    /// The serialized distributions.
    pub(super) fn timing(&self) -> OfferTiming {
        OfferTiming {
            clock: CLOCK.to_owned(),
            intended_to_terminal: self.intended_to_terminal.encode(),
            accepted_to_terminal: self.accepted_to_terminal.encode(),
            call_start_to_accepted: self.call_start_to_accepted.encode(),
            intended_to_call_start: self.intended_to_call_start.as_ref().map(Histogram::encode),
        }
    }
}

/// The sequences one group carries.
fn sequences(attempt: &OfferAttempt) -> impl Iterator<Item = u64> {
    (attempt.first_sequence..).take(usize::try_from(attempt.count).unwrap_or(usize::MAX))
}

/// When the schedule wanted an offer made.
///
/// A scheduled phase reads the exact legacy per-record schedule; a closed-loop
/// phase has no schedule at all, and its `intended` is its `call_start`, which
/// makes `intended_to_terminal` the honest end-to-end latency of a caller that
/// offers as fast as it is allowed to.
fn intended_ns(
    rate: Option<u64>,
    sequence: u64,
    call_start_ns: u64,
) -> Result<u64, Box<dyn Error>> {
    match rate {
        Some(rate) => schedule::intended_offset_ns(sequence, rate),
        None => Ok(call_start_ns),
    }
}
