//! The admission clock's one promise: a queue-full retry does not restart it.

use super::admission::AdmissionClock;

#[test]
fn a_clean_admission_measures_the_single_attempt() {
    let clock = AdmissionClock::start(1_000);

    assert_eq!(clock.call_start_ns(), 1_000);
    assert_eq!(clock.attempts(), 1);
    assert_eq!(clock.wait_ns(1_400), 400);
}

#[test]
fn queue_full_retries_keep_the_original_call_start() {
    // One offer, three attempts: refused at 1_500 and at 3_000, taken at
    // 5_000. The first attempt began at 1_000, so the admission wait is
    // 4_000ns — the whole time the application spent trying to hand this
    // record over, not the 2_000ns of the attempt that happened to succeed.
    let mut clock = AdmissionClock::start(1_000);
    clock.rejected();
    clock.rejected();

    assert_eq!(
        clock.call_start_ns(),
        1_000,
        "a refused attempt must not move the offer's call_start"
    );
    assert_eq!(clock.attempts(), 3);
    assert_eq!(
        clock.wait_ns(5_000),
        4_000,
        "the admission wait must span every attempt of the same offer"
    );
    assert!(
        clock.wait_ns(5_000) > 5_000 - 3_000,
        "the wait must exceed the last attempt alone, which is the defect this \
         type exists to prevent"
    );
}

#[test]
fn the_wait_saturates_rather_than_wrapping() {
    let clock = AdmissionClock::start(9_000);

    assert_eq!(
        clock.wait_ns(8_000),
        0,
        "a reading before call_start is impossible on one monotonic clock, and \
         reads as zero rather than as five centuries"
    );
}
