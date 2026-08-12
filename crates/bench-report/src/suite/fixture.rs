//! Test-only suite builders shared by this module's tests.
//!
//! A suite fixture is several bundles that differ only in the numbers under
//! test, so a test can state the one thing it is about — the head subject is
//! twenty percent faster, the machine drifted — and nothing else.

use std::path::PathBuf;

use crate::fixture::{BundleFixture, ResultFixture, scratch_directory};

use super::SuiteOptions;

/// A resample count that keeps the suite fast without changing any semantics.
pub(super) fn options() -> SuiteOptions {
    SuiteOptions {
        seed: 1_234,
        resamples: 500,
        practical_threshold: 0.05,
    }
}

/// Writes `count` attempts where the head subject is faster by `head_gain`.
pub(super) fn roles_suite(name: &str, count: usize, head_gain: f64) -> Vec<PathBuf> {
    let root = scratch_directory(name);
    (0..count)
        .map(|index| {
            #[expect(
                clippy::cast_precision_loss,
                reason = "the index is a small loop counter"
            )]
            let jitter = 1.0 + (index as f64) * 0.001;
            BundleFixture::new(&format!("attempt-{index}"))
                .subject(
                    "base",
                    Some("base"),
                    ResultFixture {
                        goodput: 100_000.0 * jitter,
                        terminal_ns: 10_000_000,
                        ..ResultFixture::default()
                    },
                )
                .subject(
                    "head",
                    Some("head"),
                    ResultFixture {
                        goodput: 100_000.0 * head_gain * jitter,
                        terminal_ns: 8_000_000,
                        ..ResultFixture::default()
                    },
                )
                .write(&root)
        })
        .collect()
}
