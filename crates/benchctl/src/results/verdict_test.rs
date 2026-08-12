//! The two verdict documents: what an attempt's classification says, and the
//! rules that decide whether two subjects may be compared at all.
#![expect(
    clippy::unwrap_used,
    reason = "an evidence fixture that cannot be written must fail the test immediately"
)]

use crate::results::{SubjectOutcome, classify, compare};
use crate::seal_test::workspace;

use super::evidence_test::{healthy, measurement_with_an_impossible_bucket, subject_with};

#[test]
fn an_attempt_with_no_subjects_is_never_valid() {
    let classification = classify(&[], &[]);
    assert!(!classification.run_valid);
    assert_eq!(
        classification.reasons,
        vec!["the attempt ran no subjects".to_owned()]
    );
}

#[test]
fn two_producer_results_compare_as_candidate_over_baseline() {
    let baseline = healthy("librdkafka-c", 100_000.0, 2_000_000);
    let candidate = healthy("kafkars", 150_000.0, 1_000_000);
    let order = vec!["librdkafka-c".to_owned(), "kafkars".to_owned()];
    let comparison = compare(&order, &[candidate, baseline]);
    assert!(comparison.comparable);
    assert!(comparison.reasons.is_empty());
    assert_eq!(comparison.pairs.len(), 1);
    let pair = &comparison.pairs[0];
    assert_eq!(pair.baseline, "librdkafka-c");
    assert_eq!(pair.candidate, "kafkars");
    assert_eq!(pair.acknowledged_goodput_ratio, Some(1.5));
    assert_eq!(
        pair.p99_latency_ratio,
        Some(0.5),
        "the ratio is over intended-to-terminal, decoded from both histograms"
    );
}

#[test]
fn a_subject_without_a_producer_result_makes_the_attempt_incomparable() {
    let baseline = healthy("librdkafka-c", 100_000.0, 2_000_000);
    let candidate = SubjectOutcome::skipped("kafkars");
    let order = vec!["librdkafka-c".to_owned(), "kafkars".to_owned()];
    let comparison = compare(&order, &[baseline, candidate]);
    assert!(!comparison.comparable);
    assert!(comparison.pairs.is_empty());
    assert!(
        comparison
            .reasons
            .iter()
            .any(|r| r.starts_with("kafkars produced no result")),
        "{:?}",
        comparison.reasons
    );
}

#[test]
fn one_subject_is_not_a_comparison() {
    let only = healthy("kafkars", 100_000.0, 1_000_000);
    let comparison = compare(&["kafkars".to_owned()], &[only]);
    assert!(!comparison.comparable);
    assert_eq!(
        comparison.reasons,
        vec!["a comparison needs at least two subjects that ran".to_owned()]
    );
}

#[test]
fn a_hand_edited_classification_is_not_a_readable_one() {
    // The read side of the same gate the seal applies on the way out. A bundle
    // whose `classification.json` grants itself claim eligibility, or declares
    // the run invalid without saying why, is not an answer to "may this be
    // believed" — and a reader that shrugged and used it would be treating a
    // hand edit as a verdict.
    let (results_root, paths) = workspace("classification-edited");
    let honest = classify(&[healthy("kafkars", 100_000.0, 1_000_000)], &[]);
    std::fs::write(
        paths.classification_json(),
        bench_schema::pretty_bytes(&honest).unwrap(),
    )
    .unwrap();
    assert!(
        crate::pipeline::read_classification(&paths).is_some_and(|read| read == honest),
        "an honest classification reads back unchanged"
    );

    for edit in [
        |document: &mut bench_schema::Classification| document.claim_eligible = true,
        |document: &mut bench_schema::Classification| {
            document.run_valid = false;
            document.reasons.clear();
        },
        |document: &mut bench_schema::Classification| {
            "kafkars.comparison.v1".clone_into(&mut document.schema);
        },
    ] {
        let mut edited = honest.clone();
        edit(&mut edited);
        std::fs::write(
            paths.classification_json(),
            bench_schema::pretty_bytes(&edited).unwrap(),
        )
        .unwrap();
        assert!(
            crate::pipeline::read_classification(&paths).is_none(),
            "an edited classification must not read as a verdict: {edited:?}"
        );
    }
    std::fs::remove_dir_all(&results_root).unwrap();
}

#[test]
fn an_impossible_bucket_classifies_and_compares_without_panicking() {
    // Before the index bound was enforced, this document parsed, and reading a
    // percentile out of it shifted a `u64` by 64 bits inside `compare`. The
    // property under test is that a hostile histogram costs the run its
    // validity, never the control plane its stack.
    let hostile = subject_with("kafkars", &measurement_with_an_impossible_bucket());
    let baseline = healthy("librdkafka-c", 100_000.0, 2_000_000);
    assert_eq!(
        hostile.evidence.intended_to_terminal_p99_ns(),
        None,
        "there is no percentile to read out of a rejected histogram"
    );

    let validity = hostile.validity();
    assert!(!validity.valid);
    assert!(
        validity
            .reasons
            .iter()
            .any(|reason| reason.contains("is not a readable")),
        "{:?}",
        validity.reasons
    );

    let order = vec!["librdkafka-c".to_owned(), "kafkars".to_owned()];
    let comparison = compare(&order, &[baseline.clone(), hostile.clone()]);
    assert!(!comparison.comparable);
    assert!(comparison.pairs.is_empty());
    assert!(
        comparison
            .reasons
            .iter()
            .any(|reason| reason.starts_with("kafkars produced an unreadable result")),
        "{:?}",
        comparison.reasons
    );
    assert!(!classify(&[baseline, hostile], &[]).run_valid);
}

/// One subject with a declared role, otherwise healthy.
fn with_role(name: &str, role: &str, goodput: f64, p99: u64) -> SubjectOutcome {
    let mut subject = healthy(name, goodput, p99);
    subject.role = Some(role.to_owned());
    subject
}

#[test]
fn a_three_subject_rotation_always_divides_by_the_base() {
    // `benchctl suite` alternates which subject runs first across repetitions,
    // which is the whole point of paired blocking. Under an execution-order
    // baseline the same suite would seal `head/base` in one repetition and
    // `base/head` in the next, and a reader comparing two comparison.json files
    // from one suite would see the ratio invert for no reason the documents
    // explain. This is the reproduction of that, in both directions.
    let subjects = || {
        vec![
            with_role("librdkafka-c", "base", 100_000.0, 2_000_000),
            with_role("kafkars", "head", 150_000.0, 1_000_000),
            with_role("anchor-c", "anchor", 90_000.0, 2_500_000),
        ]
    };
    let rotations = [
        vec![
            "librdkafka-c".to_owned(),
            "kafkars".to_owned(),
            "anchor-c".to_owned(),
        ],
        vec![
            "kafkars".to_owned(),
            "anchor-c".to_owned(),
            "librdkafka-c".to_owned(),
        ],
        vec![
            "anchor-c".to_owned(),
            "librdkafka-c".to_owned(),
            "kafkars".to_owned(),
        ],
    ];

    for order in rotations {
        let comparison = compare(&order, &subjects());

        assert!(comparison.comparable, "{:?}", comparison.reasons);
        assert_eq!(comparison.pairs.len(), 2, "{order:?}");
        for pair in &comparison.pairs {
            assert_eq!(
                pair.baseline, "librdkafka-c",
                "the denominator is the declared base whatever ran first: {order:?}"
            );
        }
        let head = comparison
            .pairs
            .iter()
            .find(|pair| pair.candidate == "kafkars")
            .unwrap_or_else(|| panic!("{order:?} lost the head pair"));
        assert_eq!(
            head.acknowledged_goodput_ratio,
            Some(1.5),
            "the ratio never inverts across the rotation: {order:?}"
        );
        // The anchor says whether the machine moved; it is never the thing a
        // ratio divides by when a base exists.
        assert!(
            comparison
                .pairs
                .iter()
                .all(|pair| pair.baseline != "anchor-c")
        );
    }
}

#[test]
fn an_unlabeled_subject_list_still_falls_back_to_execution_order() {
    let order = vec!["first".to_owned(), "second".to_owned()];
    let comparison = compare(
        &order,
        &[
            healthy("second", 150_000.0, 1_000_000),
            healthy("first", 100_000.0, 2_000_000),
        ],
    );

    assert_eq!(comparison.pairs.len(), 1);
    assert_eq!(comparison.pairs[0].baseline, "first");
}

#[test]
fn the_two_unevaluated_slo_objectives_are_named_as_deferred() {
    let classification = classify(&[healthy("kafkars", 100_000.0, 1_000_000)], &[]);

    for check in ["slo-drain-tail", "slo-queue-growth-slope"] {
        assert!(
            classification.deferred_checks.contains(&check.to_owned()),
            "{:?}",
            classification.deferred_checks
        );
    }
}
