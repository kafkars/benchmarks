//! The subject list, and the identity tokens an experiment id may be built
//! from.
#![expect(clippy::unwrap_used, reason = "test assertions may unwrap")]

use crate::error::CtlErrorKind;
use crate::resolve::resolve_experiment;

use super::experiment_test::inputs;

#[test]
fn an_empty_subject_list_measures_nothing_and_is_refused() {
    let mut inputs = inputs();
    inputs.subjects.clear();

    let error = resolve_experiment(&inputs).unwrap_err();

    assert_eq!(error.kind(), CtlErrorKind::InvalidExperiment);
}

#[test]
fn an_adapter_identity_that_would_be_machine_specific_is_refused() {
    // Both fields are hashed into the experiment id. A version stamped with the
    // build directory it was compiled in — the shape a `cc` line or a
    // `CARGO_MANIFEST_DIR` leaks into a hand-pinned constant — would give the
    // same experiment a different identity on every checkout, and the suite
    // that aggregates repetitions would find one attempt each.
    for (label, version) in [
        ("a build path", "2.15.0-/Users/somebody/src/librdkafka"),
        ("a windows build path", "2.15.0-C:\\build\\librdkafka"),
        ("a compiler line", "2.15.0 (built with cc -O2)"),
        ("a trailing newline", "2.15.0\n"),
        ("nothing at all", ""),
    ] {
        let mut inputs = inputs();
        inputs.subjects[1].describe.version = version.to_owned();

        let error = resolve_experiment(&inputs)
            .err()
            .unwrap_or_else(|| panic!("{label} was accepted into an experiment id"));

        assert_eq!(error.kind(), crate::error::CtlErrorKind::InvalidExperiment);
        assert!(
            error.message().contains("librdkafka-c") && error.message().contains("adapter_version"),
            "{label}: {error}"
        );
    }
}

#[test]
fn an_adapter_name_with_a_path_separator_is_refused() {
    let mut inputs = inputs();
    inputs.subjects[0].describe.name = "adapters/kafkars".to_owned();

    let error = resolve_experiment(&inputs).unwrap_err();

    assert_eq!(error.kind(), crate::error::CtlErrorKind::InvalidExperiment);
    assert!(error.message().contains("adapter_name"), "{error}");
    assert!(error.message().contains('/'), "{error}");
}

#[test]
fn ordinary_adapter_identities_are_still_accepted() {
    // The rule must not reject the versions the two real adapters report.
    for version in ["0.1.0", "2.15.0", "2.3.0-RC3", "1.9.2+build.7"] {
        let mut inputs = inputs();
        inputs.subjects[1].describe.version = version.to_owned();
        assert!(
            resolve_experiment(&inputs).is_ok(),
            "{version} is an ordinary version"
        );
    }
}
