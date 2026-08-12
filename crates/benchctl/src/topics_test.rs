//! The topic tool argv, which is a legacy interface preserved exactly, and the
//! order the topics are listed in.
#![expect(
    clippy::unwrap_used,
    reason = "a topic fixture that cannot be built must fail the test immediately"
)]

use bench_schema::ClusterTools;

use crate::interrupt::InterruptFlag;
use crate::seal_test::{RUN_ID, experiment, shell, workspace};
use crate::topics::{attempt_topics, create, create_argv, delete, delete_argv};

fn prefix() -> Vec<String> {
    vec!["/opt/bin/adapter".to_owned(), "topics-create".to_owned()]
}

#[test]
fn every_subject_contributes_its_warmup_then_its_measured_topic() {
    let resolved = experiment(&[
        ("kafkars", shell("exit 0")),
        ("librdkafka-c", shell("exit 0")),
    ]);
    assert_eq!(
        attempt_topics(&resolved),
        vec![
            format!("kfb-{RUN_ID}-kafkars-warmup"),
            format!("kfb-{RUN_ID}-kafkars"),
            format!("kfb-{RUN_ID}-librdkafka-c-warmup"),
            format!("kfb-{RUN_ID}-librdkafka-c"),
        ]
    );
}

#[test]
fn an_experiment_without_a_runtime_binding_owns_no_topics() {
    let mut resolved = experiment(&[("only", shell("exit 0"))]);
    resolved.runtime = None;
    assert!(attempt_topics(&resolved).is_empty());
}

#[test]
fn the_create_argv_is_the_legacy_positional_shape() {
    let argv = create_argv(
        &prefix(),
        "127.0.0.1:39092,127.0.0.1:39093",
        12,
        3,
        &["kfb-a".to_owned(), "kfb-b".to_owned()],
    );
    assert_eq!(
        argv,
        vec![
            "/opt/bin/adapter",
            "topics-create",
            "127.0.0.1:39092,127.0.0.1:39093",
            "12",
            "3",
            "kfb-a",
            "kfb-b",
        ]
    );
}

#[test]
fn the_delete_argv_takes_only_the_bootstrap_and_the_topics() {
    let argv = delete_argv(
        &["/opt/bin/adapter".to_owned(), "topics-delete".to_owned()],
        "127.0.0.1:39092",
        &["kfb-a".to_owned()],
    );
    assert_eq!(
        argv,
        vec![
            "/opt/bin/adapter",
            "topics-delete",
            "127.0.0.1:39092",
            "kfb-a",
        ]
    );
}

#[test]
fn creation_and_cleanup_log_beside_the_bundle_under_the_legacy_names() {
    let (results_root, paths) = workspace("topics");
    let resolved = experiment(&[("only", shell("exit 0"))]);
    let tools = ClusterTools {
        topic_create: shell("echo created; echo noisy >&2"),
        topic_delete: shell("exit 7"),
        verify: Vec::new(),
    };
    let interrupt = InterruptFlag::unarmed();

    let created = create(&paths, &tools, &resolved, &interrupt).unwrap();
    assert!(created.succeeded());
    assert_eq!(
        std::fs::read_to_string(paths.root().join("topic-create.stdout.log")).unwrap(),
        "created\n"
    );
    assert_eq!(
        std::fs::read_to_string(paths.root().join("topic-create.stderr.log")).unwrap(),
        "noisy\n"
    );

    let deleted = delete(&paths, &tools, &resolved, &interrupt).unwrap();
    assert!(
        !deleted.succeeded(),
        "a failing cleanup is reported, not hidden"
    );
    assert_eq!(deleted.exit.exit_code, Some(7));
    assert!(paths.root().join("topic-cleanup.stdout.log").is_file());
    std::fs::remove_dir_all(&results_root).unwrap();
}

#[test]
fn a_missing_runtime_binding_is_refused_rather_than_guessed() {
    let (results_root, paths) = workspace("topics-unbound");
    let mut resolved = experiment(&[("only", shell("exit 0"))]);
    resolved.runtime = None;
    let tools = ClusterTools {
        topic_create: shell("exit 0"),
        ..ClusterTools::default()
    };
    let error = create(&paths, &tools, &resolved, &InterruptFlag::unarmed()).unwrap_err();
    assert_eq!(error.kind(), crate::error::CtlErrorKind::InvalidExperiment);
    std::fs::remove_dir_all(&results_root).unwrap();
}
