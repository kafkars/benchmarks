//! Tests pinning the subject rules and the runtime binding's rules.
#![expect(
    clippy::unwrap_used,
    reason = "experiment fixtures are exact; a bad one must fail the test immediately"
)]

use crate::experiment_test::sample_experiment;
use crate::{
    MAX_SUBJECT_NAME_LENGTH, ResolvedExperiment, SUBJECT_ROLE_ANCHOR, SUBJECT_ROLE_BASE,
    SUBJECT_ROLE_HEAD, SUBJECT_ROLES, SchemaErrorKind, canonical_bytes, is_subject_role,
    parse_json_slice,
};

#[test]
fn an_experiment_without_subjects_is_refused() {
    let mut experiment = sample_experiment();
    experiment.subjects.clear();
    experiment.runtime = None;

    assert!(experiment.validate().is_err());
}

#[test]
fn duplicate_subject_names_are_refused() {
    let mut experiment = sample_experiment();
    experiment.subjects[1].name = "kafkars".to_owned();
    experiment.runtime = None;

    let error = experiment.validate().unwrap_err();

    assert!(error.context().contains("more than once"), "{error}");
}

#[test]
fn a_subject_name_that_cannot_become_a_topic_is_refused() {
    for name in ["kafka rs", "kafkars/1", "", "..", "kafkars:1"] {
        let mut experiment = sample_experiment();
        experiment.subjects[0].name = name.to_owned();
        experiment.runtime = None;

        assert!(
            experiment.validate().is_err(),
            "{name:?} should not be a legal subject name"
        );
    }
}

#[test]
fn an_over_long_subject_name_is_refused() {
    let mut experiment = sample_experiment();
    experiment.subjects[0].name = "a".repeat(MAX_SUBJECT_NAME_LENGTH + 1);
    experiment.runtime = None;

    assert!(experiment.validate().is_err());
}

#[test]
fn a_subject_without_a_command_is_refused() {
    let mut experiment = sample_experiment();
    experiment.subjects[0].command.clear();

    assert!(experiment.validate().is_err());
}

#[test]
fn a_runtime_binding_must_name_topics_for_every_subject() {
    let mut experiment = sample_experiment();
    if let Some(runtime) = experiment.runtime.as_mut() {
        runtime.topics.remove("librdkafka-c");
    }

    assert!(experiment.validate().is_err());
}

#[test]
fn a_runtime_binding_must_not_reuse_one_topic_twice() {
    let mut experiment = sample_experiment();
    if let Some(runtime) = experiment.runtime.as_mut() {
        if let Some(topics) = runtime.topics.get_mut("kafkars") {
            topics.warmup = topics.measured.clone();
        }
    }

    assert!(experiment.validate().is_err());
}

#[test]
fn a_run_id_must_be_sixteen_lowercase_hex_characters() {
    for run_id in ["0123456789ABCDEF", "0123456789abcde", "0123456789abcdefg"] {
        let mut experiment = sample_experiment();
        if let Some(runtime) = experiment.runtime.as_mut() {
            run_id.clone_into(&mut runtime.run_id);
        }

        assert!(
            experiment.validate().is_err(),
            "{run_id:?} should not be a legal run id"
        );
    }
}

#[test]
fn an_execution_order_must_be_a_permutation_of_the_subjects() {
    let mut short = sample_experiment();
    if let Some(runtime) = short.runtime.as_mut() {
        runtime.execution_order = vec!["kafkars".to_owned()];
    }
    assert!(short.validate().is_err());

    let mut repeated = sample_experiment();
    if let Some(runtime) = repeated.runtime.as_mut() {
        runtime.execution_order = vec!["kafkars".to_owned(), "kafkars".to_owned()];
    }
    assert!(repeated.validate().is_err());

    let mut unknown = sample_experiment();
    if let Some(runtime) = unknown.runtime.as_mut() {
        runtime.execution_order = vec!["kafkars".to_owned(), "nobody".to_owned()];
    }
    assert!(unknown.validate().is_err());
}

#[test]
fn an_experiment_without_a_binding_is_still_valid() {
    let mut experiment = sample_experiment();
    experiment.runtime = None;

    assert!(experiment.validate().is_ok());
}

#[test]
fn the_three_subject_roles_are_accepted() {
    for role in SUBJECT_ROLES {
        let mut experiment = sample_experiment();
        experiment.subjects[0].role = Some(role.to_owned());

        assert!(
            experiment.validate().is_ok(),
            "{role:?} should be a legal subject role"
        );
        assert!(is_subject_role(role));
    }
    assert_eq!(SUBJECT_ROLES, ["base", "head", "anchor"]);
    assert_eq!(SUBJECT_ROLE_BASE, "base");
    assert_eq!(SUBJECT_ROLE_HEAD, "head");
    assert_eq!(SUBJECT_ROLE_ANCHOR, "anchor");
}

#[test]
fn an_unknown_subject_role_is_refused() {
    for role in ["baseline", "Base", "", "control"] {
        let mut experiment = sample_experiment();
        experiment.subjects[1].role = Some(role.to_owned());

        let error = experiment.validate().unwrap_err();

        assert_eq!(error.kind(), SchemaErrorKind::InvalidField);
        assert!(error.context().starts_with("subjects[1].role"), "{error}");
        assert!(!is_subject_role(role));
    }
}

#[test]
fn an_absent_role_leaves_the_bytes_untouched() {
    let unlabeled = canonical_bytes(&sample_experiment()).unwrap();

    assert!(
        !String::from_utf8(unlabeled.clone())
            .unwrap()
            .contains("role"),
        "an unlabeled subject list must not mention roles at all"
    );

    let mut labeled = sample_experiment();
    labeled.subjects[0].role = Some(SUBJECT_ROLE_BASE.to_owned());

    assert_ne!(
        canonical_bytes(&labeled).unwrap(),
        unlabeled,
        "a role is identity-relevant when present"
    );
}

#[test]
fn a_role_survives_a_round_trip_through_json() {
    let mut labeled = sample_experiment();
    labeled.subjects[0].role = Some(SUBJECT_ROLE_BASE.to_owned());
    labeled.subjects[1].role = Some(SUBJECT_ROLE_HEAD.to_owned());

    let bytes = serde_json::to_vec(&labeled).unwrap();
    let parsed: ResolvedExperiment = parse_json_slice(&bytes).unwrap();

    assert_eq!(parsed, labeled);
    assert_eq!(parsed.subjects[1].role.as_deref(), Some("head"));
}
