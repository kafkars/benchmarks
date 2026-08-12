//! Batch warmup admission-limit contract tests.

use super::{active_limit, result::records};
use crate::producer::phase::PhaseSpec;

#[test]
fn partition_priming_admits_one_record_until_every_partition_is_touched() {
    let spec = phase(true);
    assert_eq!(active_limit(spec, 12, 0), 1);
    assert_eq!(active_limit(spec, 12, 11), 1);
    assert_eq!(active_limit(spec, 12, 12), 8_192);
}

#[test]
fn measured_batch_phase_uses_the_complete_application_window() {
    assert_eq!(active_limit(phase(false), 12, 0), 8_192);
}

#[test]
fn records_share_contiguous_key_and_payload_slabs() {
    let records = records(
        10,
        13,
        PhaseSpec {
            topic: "topic",
            run_id: "0123456789abcdef",
            records: 3,
            payload_bytes: 64,
            partitions: 12,
            max_outstanding: 8_192,
            prime_partitions: false,
        },
        12,
    )
    .unwrap_or_else(|error| panic!("batch records should build: {error}"));

    let first_key = records[0]
        .key_bytes()
        .unwrap_or_else(|| panic!("record key should exist"));
    let second_key = records[1]
        .key_bytes()
        .unwrap_or_else(|| panic!("record key should exist"));
    let first_value = records[0]
        .value_bytes()
        .unwrap_or_else(|| panic!("record value should exist"));
    let second_value = records[1]
        .value_bytes()
        .unwrap_or_else(|| panic!("record value should exist"));

    assert_eq!(
        second_key.as_ptr() as usize - first_key.as_ptr() as usize,
        8
    );
    assert_eq!(
        second_value.as_ptr() as usize - first_value.as_ptr() as usize,
        64,
    );
    assert_eq!(first_key.as_ref(), 10_u64.to_be_bytes());
    assert_eq!(records[0].topic(), "topic");
}

fn phase(prime_partitions: bool) -> PhaseSpec<'static> {
    PhaseSpec {
        topic: "topic",
        run_id: "run",
        records: 10_000,
        payload_bytes: 1_024,
        partitions: 12,
        max_outstanding: 8_192,
        prime_partitions,
    }
}
