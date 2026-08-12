//! The pool must produce the payload bytes the conformance goldens pin.
#![expect(clippy::unwrap_used, reason = "test fixtures are exact")]

use std::sync::Arc;

use crate::payload;

use super::pool::{PayloadPool, records};

const RUN_ID: &str = "0123456789abcdef";

#[test]
fn pooled_payloads_are_the_bytes_the_identity_function_produces() {
    // The whole prebuilt-pool argument rests on the filler repeating every
    // sixteen sequences. This asserts it against the function of record, over
    // the boundaries the committed vectors pin plus a full period and both
    // ends of the range, rather than trusting the modular arithmetic.
    let sizes = [64usize, 100, 1_024];
    let sequences = (0u64..40)
        .chain([42, 127, 128, 255, 256, 4_095, 1_000_000])
        .chain([u64::MAX - 1, u64::MAX]);
    for size in sizes {
        let pool = PayloadPool::build(RUN_ID, size).unwrap();
        let topic = Arc::<str>::from("t");
        for sequence in sequences.clone() {
            let expected = payload::make(RUN_ID, sequence, size);
            let built = records(&pool, &topic, sequence, 1, 1).unwrap();
            let value = built[0].value_bytes().unwrap();

            assert_eq!(
                value.as_ref(),
                expected.as_slice(),
                "pooled payload for sequence {sequence} at {size} bytes"
            );
        }
    }
}

#[test]
fn a_batch_carries_consecutive_sequences_keys_and_partitions() {
    let pool = PayloadPool::build(RUN_ID, 64).unwrap();
    let topic = Arc::<str>::from("measured");

    let built = records(&pool, &topic, 30, 5, 4).unwrap();

    assert_eq!(built.len(), 5);
    for (index, record) in built.iter().enumerate() {
        let sequence = 30 + u64::try_from(index).unwrap();
        assert_eq!(record.topic(), "measured");
        assert_eq!(
            record.key_bytes().unwrap().as_ref(),
            sequence.to_be_bytes().as_slice()
        );
        assert_eq!(
            record.explicit_partition(),
            Some(i32::try_from(sequence % 4).unwrap()),
            "partitions are assigned round robin by sequence"
        );
        assert_eq!(
            record.value_bytes().unwrap().as_ref(),
            payload::make(RUN_ID, sequence, 64).as_slice()
        );
    }
}

#[test]
fn a_payload_too_small_for_the_identity_envelope_is_refused() {
    let error = PayloadPool::build(RUN_ID, 32).unwrap_err().to_string();

    assert!(error.contains("identity envelope"), "{error}");
}
