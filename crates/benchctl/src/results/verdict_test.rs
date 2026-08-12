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
