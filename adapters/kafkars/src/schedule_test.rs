//! Contract tests for deterministic open-loop batch scheduling.

use std::error::Error;

use super::schedule::{ScheduledBatch, batches, intended_offset_ns};

#[test]
fn record_schedule_uses_exact_integer_nanoseconds() -> Result<(), Box<dyn Error>> {
    assert_eq!(intended_offset_ns(0, 100_000)?, 0);
    assert_eq!(intended_offset_ns(1, 100_000)?, 10_000);
    assert_eq!(intended_offset_ns(99_999, 100_000)?, 999_990_000);
    assert!(intended_offset_ns(1, 0).is_err());
    assert!(intended_offset_ns(1, 1_000_000_001).is_err());
    Ok(())
}

#[test]
fn batch_is_due_with_its_last_record_and_round_robins_callers() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        batches(1_000, 10, 4, 2)?,
        vec![
            ScheduledBatch {
                index: 0,
                caller: 0,
                first_sequence: 0,
                count: 4,
                intended_ns: 3_000_000,
            },
            ScheduledBatch {
                index: 1,
                caller: 1,
                first_sequence: 4,
                count: 4,
                intended_ns: 7_000_000,
            },
            ScheduledBatch {
                index: 2,
                caller: 0,
                first_sequence: 8,
                count: 2,
                intended_ns: 9_000_000,
            },
        ]
    );
    Ok(())
}
