//! What the client just said: one poll of a parked group, and one resolved
//! batch result read as an ownership answer.
//!
//! Both live here because they are the same question asked at two moments —
//! did the client take these bytes, and is it finished with them — and because
//! the admission path and the completion path both have to ask it.

use std::{
    error::Error,
    future::Future,
    sync::{Arc, atomic::Ordering},
    task::{Context, Poll, Wake, Waker},
};

use kafkars::{ErrorKind, KafkaError, Record, RecordMetadata, SendBatchResult};

use super::{GroupWake, Slot};

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

/// What one resolved `send_batch` said about ownership.
pub(super) enum Admission {
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
pub(super) fn classify(result: SendBatchResult, count: u64) -> Result<Admission, Box<dyn Error>> {
    let (deliveries, rejection) = result.into_parts();
    let accepted = u64::try_from(deliveries.len())?;
    match rejection {
        None if accepted == count => Ok(Admission::Accepted(deliveries)),
        None => Err(format!(
            "an offer group of {count} reported {accepted} terminals and no rejection"
        )
        .into()),
        Some(rejection) if deliveries.is_empty() => {
            let (records, error) = rejection.into_parts();
            if error.kind() == ErrorKind::Backpressure {
                Ok(Admission::Refused(records))
            } else {
                Err(wholly_refused(count, &error).into())
            }
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

/// Names a refusal the retry loop has no way to answer.
///
/// Backpressure is the only refusal an offer can wait out: the client is full
/// now and may not be a moment from now, so [`OfferEngine::admit`] presents the
/// same records again. Every other refusal returns the identical "nothing accepted,
/// everything handed back" shape while meaning the opposite — the client will
/// never take this record — and reporting it as a *partial* admission named the
/// one thing that demonstrably did not happen, hid the client's own error, and
/// left a reader to guess whether their run had been reordered. The kind is the
/// only thing that separates the two, so it is what this says first.
///
/// [`OfferEngine::admit`]: super::OfferEngine::admit
pub(in crate::producer::v2) fn wholly_refused(count: u64, error: &KafkaError) -> String {
    format!(
        "the client accepted no record of an offer group of {count}, for a reason no retry \
         can clear: kind={:?} fatal={} delivery={:?} message={error}",
        error.kind(),
        error.is_fatal(),
        error.delivery_status(),
    )
}

/// What one poll of a parked group said.
pub(super) enum SlotPoll {
    /// The client still owns the group.
    Parked(Slot),
    /// The group resolved.
    Ready(SendBatchResult),
}

/// Polls one group against its own waker.
pub(super) fn poll_slot(mut slot: Slot) -> SlotPoll {
    slot.wake.queued.store(false, Ordering::Release);
    let waker = Waker::from(Arc::clone(&slot.wake));
    let mut context = Context::from_waker(&waker);
    match slot.operation.as_mut().poll(&mut context) {
        Poll::Pending => SlotPoll::Parked(slot),
        Poll::Ready(result) => SlotPoll::Ready(result),
    }
}
