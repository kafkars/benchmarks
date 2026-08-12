//! Contract tests for deterministic open-loop batch scheduling.

use std::error::Error;

use super::schedule::{Schedule, ScheduledBatch, batches, intended_offset_ns};

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

#[test]
fn asking_for_one_batch_at_a_time_yields_the_materialized_schedule() -> Result<(), Box<dyn Error>> {
    // The v2 fixed-rate path never materializes the schedule, because a vector
    // of batches grows with the run. That is only safe while the lazy answer
    // is the same answer, so the two are compared over the cases the committed
    // conformance vectors pin — a clean multiple, a ragged tail, and a rate
    // whose offsets land in the sub-microsecond range.
    for (rate, records, batch_records, callers) in [
        (100_000u64, 1_000u64, 256u64, 4u64),
        (3, 17, 4, 2),
        (1_000_000_000, 513, 256, 4),
    ] {
        let materialized = batches(rate, records, batch_records, callers)?;
        let schedule = Schedule::new(rate, records, batch_records, callers)?;

        assert_eq!(
            schedule.batch_count(),
            u64::try_from(materialized.len())?,
            "batch count for {rate}/{records}/{batch_records}/{callers}"
        );
        for (index, expected) in materialized.iter().enumerate() {
            assert_eq!(
                &schedule.batch(u64::try_from(index)?)?,
                expected,
                "batch {index} of {rate}/{records}/{batch_records}/{callers}"
            );
        }
        assert!(
            schedule.batch(schedule.batch_count()).is_err(),
            "a batch past the end of the run is not a batch"
        );
    }
    Ok(())
}
