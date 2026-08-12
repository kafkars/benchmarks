//! Tests for the pairing: which subject ends up over which.
#![expect(
    clippy::unwrap_used,
    reason = "test fixtures assert exact outcomes and must fail immediately"
)]

use std::path::PathBuf;

use crate::fixture::{BundleFixture, ResultFixture, scratch_directory};

use super::fixture::{options, roles_suite};
use super::summarize_suite;

#[test]
fn roles_decide_the_pairing() {
    let roots = roles_suite("suite-roles", 5, 1.20);

    let summary = summarize_suite(&roots, &options()).unwrap();

    assert!(!summary.pairs.is_empty());
    for pair in &summary.pairs {
        assert_eq!(pair.numerator_subject, "head");
        assert_eq!(pair.denominator_subject, "base");
    }
}

#[test]
fn without_roles_every_subject_is_compared_against_the_first() {
    let root = scratch_directory("suite-no-roles");
    let roots: Vec<PathBuf> = (0..5)
        .map(|index| {
            BundleFixture::new(&format!("attempt-{index}"))
                .subject("alpha", None, ResultFixture::default())
                .subject("beta", None, ResultFixture::default())
                .subject("gamma", None, ResultFixture::default())
                .write(&root)
        })
        .collect();

    let summary = summarize_suite(&roots, &options()).unwrap();

    let numerators: Vec<&str> = summary
        .pairs
        .iter()
        .map(|pair| pair.numerator_subject.as_str())
        .collect();
    assert!(numerators.contains(&"beta"));
    assert!(numerators.contains(&"gamma"));
    assert!(
        summary
            .pairs
            .iter()
            .all(|pair| pair.denominator_subject == "alpha")
    );
}
