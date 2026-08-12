//! The interrupt latch: it starts down, stays up once raised, and is shared by
//! every clone.
//!
//! The signal path itself is exercised end to end by the integration wave,
//! which sends a real SIGINT to the `benchctl` binary. What is unit-testable
//! here is the latch's contract, which is what the supervisor polls.

use crate::interrupt::{InterruptFlag, process_latch};

#[test]
fn an_unarmed_flag_starts_down() {
    let flag = InterruptFlag::unarmed();
    assert!(!flag.is_set());
}

#[test]
fn raising_the_latch_is_visible_to_every_clone() {
    let flag = InterruptFlag::unarmed();
    let observer = flag.clone();
    assert!(!observer.is_set());
    flag.raise();
    assert!(observer.is_set(), "clones must share one latch");
    flag.raise();
    assert!(observer.is_set(), "the latch never falls back down");
}

#[test]
fn the_process_latch_is_installed_once_and_shared() {
    let first = process_latch();
    let second = process_latch();
    assert_eq!(first.is_set(), second.is_set());
}
