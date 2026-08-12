//! The execution-order rule: that it is a pure function of the repetition
//! index, that it is a permutation every time, and that the lead position is
//! shared out within one across any number of repetitions.
//!
//! The balance property is the reason the rule exists, so it is asserted over a
//! range of subject counts and repetition counts rather than on one example: a
//! rule that balances three subjects over six repetitions and quietly favours
//! the first of four over ten would be worse than no rule, because the report
//! would still claim the positions were shared.
#![expect(clippy::unwrap_used, reason = "test assertions may unwrap")]

use std::collections::BTreeMap;

use crate::suite::{MINIMUM_REPETITIONS, repetition_order};

/// `count` subject names, `s0`, `s1`, ….
fn subjects(count: usize) -> Vec<String> {
    (0..count).map(|index| format!("s{index}")).collect()
}

/// How many times each subject led, over `repetitions` repetitions.
fn lead_counts(base: &[String], repetitions: u32) -> BTreeMap<String, u32> {
    let mut counts: BTreeMap<String, u32> = base.iter().map(|name| (name.clone(), 0)).collect();
    for repetition in 0..repetitions {
        let order = repetition_order(base, repetition);
        *counts.get_mut(&order[0]).unwrap() += 1;
    }
    counts
}

#[test]
fn the_first_repetition_runs_the_subjects_in_the_order_they_were_given() {
    let base = subjects(3);
    assert_eq!(repetition_order(&base, 0), base);
}

#[test]
fn each_repetition_rotates_the_list_by_one() {
    let base = subjects(3);
    assert_eq!(repetition_order(&base, 1), vec!["s1", "s2", "s0"]);
    assert_eq!(repetition_order(&base, 2), vec!["s2", "s0", "s1"]);
}

#[test]
fn the_second_block_of_repetitions_is_reversed() {
    let base = subjects(3);
    // Block 1 (repetitions 3..6) is the rotation reversed, which both keeps the
    // lead moving and inverts every adjacency the first block established.
    assert_eq!(repetition_order(&base, 3), vec!["s2", "s1", "s0"]);
    assert_eq!(repetition_order(&base, 4), vec!["s0", "s2", "s1"]);
    assert_eq!(repetition_order(&base, 5), vec!["s1", "s0", "s2"]);
    assert_eq!(
        repetition_order(&base, 6),
        base,
        "block 2 is even, so the rotation is upright again"
    );
}

#[test]
fn every_repetition_runs_every_subject_exactly_once() {
    for count in 1..=6 {
        let base = subjects(count);
        for repetition in 0..24 {
            let mut order = repetition_order(&base, repetition);
            assert_eq!(order.len(), count);
            order.sort();
            let mut expected = base.clone();
            expected.sort();
            assert_eq!(order, expected, "{count} subjects, repetition {repetition}");
        }
    }
}

#[test]
fn over_n_repetitions_each_subject_leads_within_one_of_every_other() {
    for count in 1..=6 {
        let base = subjects(count);
        for repetitions in MINIMUM_REPETITIONS..=24 {
            let counts = lead_counts(&base, repetitions);
            let low = *counts.values().min().unwrap();
            let high = *counts.values().max().unwrap();
            assert!(
                high - low <= 1,
                "{count} subjects over {repetitions} repetitions led {counts:?}"
            );
            assert_eq!(
                counts.values().sum::<u32>(),
                repetitions,
                "every repetition has exactly one leader"
            );
        }
    }
}

#[test]
fn a_whole_number_of_blocks_gives_every_subject_the_lead_equally_often() {
    for count in 1..=6 {
        let base = subjects(count);
        let repetitions = u32::try_from(count).unwrap() * 4;
        let counts = lead_counts(&base, repetitions);
        assert!(
            counts.values().all(|led| *led == 4),
            "{count} subjects over {repetitions} repetitions led {counts:?}"
        );
    }
}

#[test]
fn a_single_subject_always_runs_alone_and_first() {
    let base = subjects(1);
    for repetition in 0..5 {
        assert_eq!(repetition_order(&base, repetition), base);
    }
}

#[test]
fn no_subjects_is_an_empty_order_rather_than_a_panic() {
    assert!(repetition_order(&[], 3).is_empty());
}
