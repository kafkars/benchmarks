//! The attempt binding: the run id, the topics it names, and the execution
//! order.
#![expect(clippy::unwrap_used, reason = "test assertions may unwrap")]

use bench_schema::experiment_id;

use crate::error::CtlErrorKind;
use crate::resolve::{derive_run_id, resolve_experiment};

use super::experiment_test::{ATTEMPT, inputs};

#[test]
fn the_topics_are_named_from_the_run_id_and_the_subject() {
    let resolved = resolve_experiment(&inputs()).unwrap();

    let runtime = resolved.runtime.clone().unwrap();
    assert_eq!(runtime.topic_prefix, format!("kfb-{}", runtime.run_id));
    let topics = runtime.topics.get("kafkars").unwrap();
    assert_eq!(topics.measured, format!("kfb-{}-kafkars", runtime.run_id));
    assert_eq!(
        topics.warmup,
        format!("kfb-{}-kafkars-warmup", runtime.run_id)
    );
    assert_eq!(runtime.bootstrap, "127.0.0.1:39092,127.0.0.1:39093");
}

#[test]
fn two_attempts_of_one_experiment_share_an_id_and_differ_in_run_id() {
    let first = resolve_experiment(&inputs()).unwrap();
    let mut later_inputs = inputs();
    later_inputs.runtime.attempt_id = "20260812T140900Z-99887766".to_owned();
    let second = resolve_experiment(&later_inputs).unwrap();

    assert_eq!(
        experiment_id(&first).unwrap(),
        experiment_id(&second).unwrap(),
        "the attempt must not change what experiment this is"
    );
    assert_ne!(
        first.runtime.unwrap().run_id,
        second.runtime.unwrap().run_id,
        "two attempts must not write records under one run id"
    );
}

#[test]
fn the_run_id_is_sixteen_lowercase_hex_characters_of_a_derived_digest() {
    let resolved = resolve_experiment(&inputs()).unwrap();
    let identity = experiment_id(&resolved).unwrap();

    let run_id = resolved.runtime.unwrap().run_id;

    assert_eq!(run_id.len(), 16);
    assert!(
        run_id
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    );
    assert_eq!(run_id, derive_run_id(identity.as_str(), ATTEMPT));
}

#[test]
fn binding_the_runtime_does_not_move_the_experiment_id() {
    let bound = resolve_experiment(&inputs()).unwrap();
    let mut unbound = bound.clone();
    unbound.runtime = None;

    assert_eq!(
        experiment_id(&bound).unwrap(),
        experiment_id(&unbound).unwrap()
    );
}

#[test]
fn the_execution_order_defaults_to_the_subjects_file_order() {
    let resolved = resolve_experiment(&inputs()).unwrap();

    assert_eq!(
        resolved.runtime.unwrap().execution_order,
        vec!["kafkars".to_owned(), "librdkafka-c".to_owned()]
    );
}

#[test]
fn a_requested_order_is_recorded_verbatim() {
    let mut inputs = inputs();
    inputs.runtime.order = Some(vec!["librdkafka-c".to_owned(), "kafkars".to_owned()]);

    let resolved = resolve_experiment(&inputs).unwrap();

    assert_eq!(
        resolved.runtime.unwrap().execution_order,
        vec!["librdkafka-c".to_owned(), "kafkars".to_owned()]
    );
}

#[test]
fn an_order_that_is_not_a_permutation_of_the_subjects_is_refused() {
    for requested in [
        vec!["kafkars".to_owned()],
        vec!["kafkars".to_owned(), "kafkars".to_owned()],
        vec!["kafkars".to_owned(), "sarama".to_owned()],
    ] {
        let mut inputs = inputs();
        inputs.runtime.order = Some(requested.clone());

        let error = resolve_experiment(&inputs).unwrap_err();

        assert_eq!(
            error.kind(),
            CtlErrorKind::InvalidExperiment,
            "{requested:?} was accepted"
        );
    }
}

#[test]
fn an_attempt_with_no_bootstrap_is_refused() {
    let mut inputs = inputs();
    inputs.runtime.bootstrap = "   ".to_owned();

    let error = resolve_experiment(&inputs).unwrap_err();

    assert_eq!(error.kind(), CtlErrorKind::InvalidExperiment);
}

#[test]
fn the_run_id_is_a_function_of_both_identities() {
    let experiment = "d2932f88ad348028796b43b929f2fca826b683d19f13c0d1ad30d676b25dac15";

    assert_eq!(
        derive_run_id(experiment, ATTEMPT),
        derive_run_id(experiment, ATTEMPT),
        "derivation must be a function, not a sample"
    );
    assert_ne!(
        derive_run_id(experiment, ATTEMPT),
        derive_run_id(experiment, "20260812T140900Z-99887766")
    );
    assert_ne!(
        derive_run_id(experiment, ATTEMPT),
        derive_run_id(&"0".repeat(64), ATTEMPT)
    );
}
