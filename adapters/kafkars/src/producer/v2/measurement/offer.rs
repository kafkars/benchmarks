//! The vocabulary of one offer group: where it ended, what it was when its
//! public call began, and what the client took.
//!
//! These are the three values the engine and the measurement pass between
//! them. They carry no behavior beyond reading a client delivery, so that the
//! accounting built on them can be exercised without a broker and without
//! fabricating client types a test has no way to build.

use kafkars::{ErrorKind, KafkaError, RecordMetadata};

use crate::producer::v2::admission::AdmissionClock;

/// Where one offer ended.
///
/// The engine reads this off a client delivery; everything downstream counts
/// `Terminal`s, so the accounting can be exercised without a broker and
/// without fabricating client types a test has no way to build.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::producer::v2) enum Terminal {
    /// The broker acknowledged the record.
    Acknowledged,
    /// The record reached a failure terminal.
    Failed,
    /// The record's delivery deadline elapsed.
    TimedOut,
}

impl Terminal {
    /// Reads one delivery outcome.
    pub(in crate::producer::v2) fn of(delivery: &Result<RecordMetadata, KafkaError>) -> Self {
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
pub(in crate::producer::v2) struct OfferAttempt {
    /// Sequence of the first offer in the group.
    pub(in crate::producer::v2) first_sequence: u64,
    /// Offers the group carries.
    pub(in crate::producer::v2) count: u64,
    /// The admission clock, started at the first attempt and never restarted.
    pub(in crate::producer::v2) admission: AdmissionClock,
}

/// One offer group the client took, and when it took it.
#[derive(Clone, Copy, Debug)]
pub(in crate::producer::v2) struct OfferGroup {
    /// The identity and admission clock the attempt began with.
    pub(in crate::producer::v2) attempt: OfferAttempt,
    /// When the call that transferred ownership returned.
    pub(in crate::producer::v2) accepted_ns: u64,
}
