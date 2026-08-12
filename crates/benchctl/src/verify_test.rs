//! The verifier argv, the report gate, and the difference between a verifier
//! that broke and a verifier that disagreed.
#![expect(
    clippy::unwrap_used,
    reason = "a verification fixture that cannot be built must fail the test immediately"
)]

use bench_schema::ClusterTools;

use crate::interrupt::InterruptFlag;
use crate::seal_test::{RUN_ID, experiment, shell, workspace};
use crate::verify::{VerificationPhase, verify, verify_argv};

/// A `/bin/sh` verifier that prints `document` and exits zero.
fn verifier(document: &str) -> Vec<String> {
    shell(&format!("cat <<'REPORT'\n{document}\nREPORT"))
}

/// A passing report for `topic` over `records` records and twelve partitions.
fn passing_report(topic: &str, records: u64) -> String {
    format!(
        r#"{{"schema":"kafkars.producer-verification.v1","topic":"{topic}",
        "expected_records":{records},"verified_records":{records},"duplicates":0,
        "missing_records":0,"corrupt":0,"unexpected":0,"eof_partitions":12,"valid":true}}"#
    )
}

#[test]
fn the_verifier_argv_is_the_legacy_six_argument_shape() {
    let argv = verify_argv(
        &["/opt/bin/verifier".to_owned()],
        "127.0.0.1:39092",
        "kfb-topic",
        RUN_ID,
        10_000,
        1_024,
        12,
    );
    assert_eq!(
        argv,
        vec![
            "/opt/bin/verifier",
            "127.0.0.1:39092",
            "kfb-topic",
            RUN_ID,
            "10000",
            "1024",
            "12",
        ]
    );
}

#[test]
fn a_passing_report_about_the_right_topic_satisfies_the_contract() {
    let (results_root, paths) = workspace("verify-pass");
    let resolved = experiment(&[("only", shell("exit 0"))]);
    let topic = format!("kfb-{RUN_ID}-only");
    let tools = ClusterTools {
        verify: verifier(&passing_report(&topic, 1_000)),
        ..ClusterTools::default()
    };
    let run = verify(
        &paths,
        &tools,
        &resolved,
        "only",
        VerificationPhase::Measured,
        &InterruptFlag::unarmed(),
    )
    .unwrap();
    assert!(run.tool_succeeded);
    assert!(run.satisfies_contract);
    assert_eq!(run.outcome.valid, Some(true));
    assert_eq!(run.outcome.exit_code, Some(0));
    assert!(paths.verification_json("only", "measured").is_file());
    std::fs::remove_dir_all(&results_root).unwrap();
}

#[test]
fn the_warmup_phase_expects_the_warmup_record_count() {
    let (results_root, paths) = workspace("verify-warmup");
    let resolved = experiment(&[("only", shell("exit 0"))]);
    let warmup_topic = format!("kfb-{RUN_ID}-only-warmup");
    // The report claims the measured count, which is wrong for the warmup topic.
    let tools = ClusterTools {
        verify: verifier(&passing_report(&warmup_topic, 1_000)),
        ..ClusterTools::default()
    };
    let run = verify(
        &paths,
        &tools,
        &resolved,
        "only",
        VerificationPhase::Warmup,
        &InterruptFlag::unarmed(),
    )
    .unwrap();
    assert!(run.tool_succeeded);
    assert!(
        !run.satisfies_contract,
        "the warmup contract is about the warmup count, not the measured one"
    );

    let tools = ClusterTools {
        verify: verifier(&passing_report(&warmup_topic, 100)),
        ..ClusterTools::default()
    };
    let run = verify(
        &paths,
        &tools,
        &resolved,
        "only",
        VerificationPhase::Warmup,
        &InterruptFlag::unarmed(),
    )
    .unwrap();
    assert!(run.satisfies_contract);
    std::fs::remove_dir_all(&results_root).unwrap();
}

#[test]
fn a_report_about_another_topic_never_satisfies_the_contract() {
    let (results_root, paths) = workspace("verify-wrong-topic");
    let resolved = experiment(&[("only", shell("exit 0"))]);
    let tools = ClusterTools {
        verify: verifier(&passing_report("somebody-elses-topic", 1_000)),
        ..ClusterTools::default()
    };
    let run = verify(
        &paths,
        &tools,
        &resolved,
        "only",
        VerificationPhase::Measured,
        &InterruptFlag::unarmed(),
    )
    .unwrap();
    assert_eq!(
        run.outcome.valid,
        Some(true),
        "the verifier liked what it saw"
    );
    assert!(
        !run.satisfies_contract,
        "a verifier does not know which topic was meant, so its flag is not enough"
    );
    std::fs::remove_dir_all(&results_root).unwrap();
}

#[test]
fn a_verifier_that_fails_to_run_is_told_apart_from_one_that_disagrees() {
    let (results_root, paths) = workspace("verify-broken");
    let resolved = experiment(&[("only", shell("exit 0"))]);
    let tools = ClusterTools {
        verify: shell("echo broken >&2; exit 2"),
        ..ClusterTools::default()
    };
    let run = verify(
        &paths,
        &tools,
        &resolved,
        "only",
        VerificationPhase::Measured,
        &InterruptFlag::unarmed(),
    )
    .unwrap();
    assert!(!run.tool_succeeded);
    assert!(!run.satisfies_contract);
    assert_eq!(run.outcome.exit_code, Some(2));
    assert!(run.outcome.ran, "the tool did run; it just failed");
    std::fs::remove_dir_all(&results_root).unwrap();
}

#[test]
fn an_unknown_subject_is_refused_rather_than_verified_against_nothing() {
    let (results_root, paths) = workspace("verify-unknown");
    let resolved = experiment(&[("only", shell("exit 0"))]);
    let tools = ClusterTools {
        verify: verifier("{}"),
        ..ClusterTools::default()
    };
    let error = verify(
        &paths,
        &tools,
        &resolved,
        "nobody",
        VerificationPhase::Measured,
        &InterruptFlag::unarmed(),
    )
    .unwrap_err();
    assert_eq!(error.kind(), crate::error::CtlErrorKind::InvalidExperiment);
    std::fs::remove_dir_all(&results_root).unwrap();
}
