//! The suite verb, offline: a three-repetition suite that writes a summary,
//! both renderings, and a packet, and what a suite does when a repetition
//! cannot be sealed at all.
//!
//! Everything here runs the real `benchctl` binary against the `fake-adapter`
//! fixture playing every subject and all three cluster tools, so these are
//! end-to-end checks of the verb surface with no Kafka, no network, and no
//! clock-dependent behavior. The fixture's numbers are a pure function of the
//! resolved experiment, which is what makes the sharpest property below
//! assertable: a suite summary that is identical across two independent runs.
#![expect(
    clippy::unwrap_used,
    reason = "a fixture that cannot be built must fail the test immediately"
)]

mod common;

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use bench_schema::{AnalysisPacket, SuiteSummary};

use common::harness::{
    benchctl_output, command_toml, common_arguments, only_report_directory, repo_root, run_suite,
    scratch, without_attempt_identity,
};

#[test]
fn a_suite_writes_a_summary_both_renderings_and_a_packet() {
    let dir = scratch("reports");
    let (code, reports) = run_suite(&dir, "once");
    assert_eq!(code, 0, "every attempt completed and enough were valid");

    for name in [
        "suite-summary.json",
        "report.md",
        "report.html",
        "analysis-packet.json",
    ] {
        assert!(reports.join(name).is_file(), "{name} is missing");
    }

    let summary =
        SuiteSummary::from_slice(&std::fs::read(reports.join("suite-summary.json")).unwrap())
            .unwrap();
    assert_eq!(summary.repetitions, 3);
    assert_eq!(summary.attempts.len(), 3);
    assert!(!summary.claim_eligible, "never in this milestone");
    assert!(
        summary.attempts.iter().all(|attempt| attempt.run_valid),
        "the fixture's attempts are all valid: {:?}",
        summary.attempts
    );
    assert_eq!(
        summary.medians.len(),
        2,
        "one median row per subject: {:?}",
        summary.medians
    );

    let packet =
        AnalysisPacket::from_slice(&std::fs::read(reports.join("analysis-packet.json")).unwrap())
            .unwrap();
    assert_eq!(packet.validity.runs_total, 3);
    assert_eq!(packet.validity.runs_valid, 3);
    assert!(
        !packet.metrics.is_empty(),
        "a packet without metrics cites nothing"
    );

    let markdown = std::fs::read_to_string(reports.join("report.md")).unwrap();
    assert!(markdown.contains("kafkars"), "{markdown}");
    let html = std::fs::read_to_string(reports.join("report.html")).unwrap();
    assert!(html.contains("<table") || html.contains("<h1"), "{html}");

    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn two_independent_suites_summarize_to_the_same_bytes() {
    let dir = scratch("deterministic");
    let (first_code, first) = run_suite(&dir, "first");
    let (second_code, second) = run_suite(&dir, "second");
    assert_eq!(first_code, 0);
    assert_eq!(second_code, 0);
    assert_ne!(first, second, "the two suites wrote to different roots");

    let left = without_attempt_identity(&std::fs::read(first.join("suite-summary.json")).unwrap());
    let right =
        without_attempt_identity(&std::fs::read(second.join("suite-summary.json")).unwrap());
    assert_eq!(
        String::from_utf8(left).unwrap(),
        String::from_utf8(right).unwrap(),
        "the same experiment, repeated the same way, must summarize identically"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Writes the fixtures for a suite whose third repetition cannot be sealed.
///
/// The topic-cleanup tool is the sabotage: it runs once at the end of every
/// attempt, so on its second run it replaces `results/pending` — the directory
/// every attempt workspace is born in — with a regular file. The next
/// repetition therefore has nowhere to seal into, which is the one failure
/// `AttemptEnd::Unsealable` exists for, and the only one a suite cannot record
/// in a bundle.
fn write_fixtures_that_break_after_two_attempts(
    dir: &Path,
    results: &Path,
    subjects: &[&str],
) -> (PathBuf, PathBuf) {
    let adapter = common::adapter("ok");
    let tool = |verb: &str| {
        let mut argv = adapter.clone();
        argv.push(verb.to_owned());
        command_toml(&argv)
    };
    let marker = dir.join("cleanup-ran-once");
    let sabotage = command_toml(&[
        "/bin/sh".to_owned(),
        "-c".to_owned(),
        format!(
            "if [ -e {marker} ]; then rm -rf {pending}; : > {pending}; fi; : > {marker}; exit 0",
            marker = marker.display(),
            pending = results.join("pending").display(),
        ),
    ]);
    let mut subjects_toml = String::new();
    for name in subjects {
        let _ = writeln!(
            subjects_toml,
            "[[subjects]]\nname = \"{name}\"\ncommand = {}\n",
            command_toml(&adapter)
        );
    }
    let subjects_path = dir.join("subjects.toml");
    std::fs::write(&subjects_path, subjects_toml).unwrap();
    let cluster_path = dir.join("cluster.toml");
    std::fs::write(
        &cluster_path,
        format!(
            "name = \"offline\"\n\
             bootstrap = \"127.0.0.1:39092,127.0.0.1:39093,127.0.0.1:39094\"\n\
             broker_version = \"4.3.1\"\nbrokers = 3\nsecurity = \"plaintext\"\n\
             lifecycle = \"externally managed by the caller\"\n\n\
             [tools]\ntopic_create = {create}\ntopic_delete = {sabotage}\nverify = {verify}\n",
            create = tool("topics-create"),
            verify = tool("verify"),
        ),
    )
    .unwrap();
    (subjects_path, cluster_path)
}

#[test]
fn a_repetition_that_cannot_be_sealed_stops_the_suite_and_is_the_exit_code() {
    let dir = scratch("unsealable");
    let results = dir.join("results");
    let reports = dir.join("reports");
    let (subjects, cluster) =
        write_fixtures_that_break_after_two_attempts(&dir, &results, &["kafkars", "librdkafka-c"]);
    let mut arguments = common_arguments(
        "suite",
        &repo_root().join("scenarios/producer/legacy-balanced-1k.toml"),
        &subjects,
        &cluster,
        &results,
        &reports,
    );
    arguments.push("--repetitions".to_owned());
    arguments.push("3".to_owned());

    let (code, stdout, stderr) = benchctl_output(&arguments);

    assert_eq!(
        code, 70,
        "the exit code is the unsealable repetition's, not the verdict over the two that \
         sealed; stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("the suite stopped after 2 of 3 repetitions"),
        "the stop has to be said, not only exited: {stderr}"
    );
    assert!(
        stderr.contains("cover only the 2 attempts that sealed"),
        "the reports must not be presented as the whole suite: {stderr}"
    );

    // The two attempts that did seal are still summarized: they happened, and a
    // suite that threw them away would be destroying evidence over a workspace
    // it could not claim.
    let directory = only_report_directory(&reports);
    let summary =
        SuiteSummary::from_slice(&std::fs::read(directory.join("suite-summary.json")).unwrap())
            .unwrap();
    assert_eq!(summary.repetitions, 2, "two bundles were summarized");
    assert_eq!(summary.attempts.len(), 2);
    assert!(summary.attempts.iter().all(|attempt| attempt.run_valid));
    assert!(
        stdout.contains("suite-summary.json"),
        "the report paths are still printed: {stdout}"
    );

    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn two_matched_fake_subjects_pass_the_execution_surface_gate_and_stay_inconclusive() {
    // The offline proof of the wave's P0. Both subjects are the same fake
    // adapter, so they declare the same measured work and the gate must pass —
    // the negative direction, a fixture whose declarations disagree, is
    // asserted in bench-report's `surface_test`.
    let dir = scratch("matched-surface");
    let results = dir.join("results");
    let reports = dir.join("reports");
    let (subjects, cluster) =
        common::harness::write_fixtures(&dir, "ok", &["kafkars", "librdkafka-c"]);
    let mut arguments = common_arguments(
        "suite",
        &repo_root().join("scenarios/producer/legacy-balanced-1k.toml"),
        &subjects,
        &cluster,
        &results,
        &reports,
    );
    arguments.push("--repetitions".to_owned());
    arguments.push("2".to_owned());

    let (code, _stdout, stderr) = benchctl_output(&arguments);
    assert_eq!(code, 0, "stderr:\n{stderr}");

    let directory = only_report_directory(&reports);
    let summary =
        SuiteSummary::from_slice(&std::fs::read(directory.join("suite-summary.json")).unwrap())
            .unwrap();
    let gate = summary
        .gates
        .iter()
        .find(|gate| gate.name == "matched-execution-surface")
        .unwrap_or_else(|| panic!("no execution-surface gate: {:?}", summary.gates));
    assert!(gate.passed, "{}", gate.detail);
    assert!(
        !summary
            .notes
            .iter()
            .any(|note| note.contains("product-surface difference")),
        "two identical adapters have no product-surface difference: {:?}",
        summary.notes
    );
    // Neither attribution metric may carry a pass/fail gate.
    for field in [
        "p99_accepted_to_terminal_ns",
        "p99_intended_to_call_start_ns",
    ] {
        assert!(
            !summary.gates.iter().any(|gate| gate.name.contains(field)),
            "{field} carries a gate it must not"
        );
    }

    // The new observation fields reach the sealed document.
    let observation = summary.attempts[0]
        .subjects
        .first()
        .unwrap_or_else(|| panic!("no subject observation"));
    assert!(observation.declared.is_some());
    assert!(observation.p99_accepted_to_terminal_ns > 0);
    assert!(
        observation
            .cpu_core_seconds_per_million_acknowledged
            .is_some_and(|value| value > 0.0)
    );

    // Two repetitions is below the paired-repetition minimum, so the packet is
    // inconclusive and says which way it leaned instead of asserting it.
    let packet =
        AnalysisPacket::from_slice(&std::fs::read(directory.join("analysis-packet.json")).unwrap())
            .unwrap();
    assert_eq!(packet.verdict, bench_schema::Verdict::Inconclusive);

    std::fs::remove_dir_all(&dir).unwrap();
}
