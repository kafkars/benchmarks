//! A refusal the retry loop cannot answer must say so, and say why.

use kafkars::{ErrorKind, KafkaError};

use super::engine::wholly_refused;

#[test]
fn a_fenced_producer_is_reported_as_a_refusal_rather_than_a_partial_admission() {
    // The shape this exists for: a client whose producer admission has closed
    // hands back every record and accepts none — byte for byte the same result
    // a full queue produces, and the exact opposite meaning. The old wording
    // called it "partially admitted: 0 accepted", which named an admission that
    // did not happen, implied the run might have been reordered, and buried the
    // one field that separates the two cases.
    let error = KafkaError::new(ErrorKind::State, "producer admission is closed");

    let message = wholly_refused(1, &error);

    assert!(
        message.contains("kind=State"),
        "the kind is what separates a waitable refusal from a final one: {message}"
    );
    assert!(
        message.contains("producer admission is closed"),
        "the client's own words must survive into the adapter's failure: {message}"
    );
    assert!(
        !message.contains("partially admitted"),
        "nothing was admitted, so nothing may be described as partially admitted: {message}"
    );
}

#[test]
fn the_refused_group_size_is_reported() {
    let error = KafkaError::new(ErrorKind::InvalidRecord, "batch limit exceeded");

    let message = wholly_refused(256, &error);

    assert!(
        message.contains("offer group of 256"),
        "a reader needs the size of the group that was turned away: {message}"
    );
    assert!(
        message.contains("no retry can clear"),
        "the message must say the engine stopped retrying on purpose: {message}"
    );
}
