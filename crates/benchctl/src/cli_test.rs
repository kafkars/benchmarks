//! The argv parse table: every flag, every default, and every way a command
//! line can be wrong.
//!
//! Parsing is the only part of the command surface that can be tested without a
//! cluster, and it is also the part a person hits first, so the table below is
//! exhaustive by intent. Every failure asserts the *kind* rather than the
//! message, because the kind is what becomes the exit code.
#![expect(clippy::unwrap_used, reason = "test assertions may unwrap")]

use std::path::PathBuf;

use bench_schema::BudgetSpec;

use crate::cli::{Command, DEFAULT_RESULTS_ROOT, parse};
use crate::error::CtlErrorKind;
use crate::suite::DEFAULT_REPORTS_ROOT;

fn arguments(line: &str) -> Vec<String> {
    line.split_whitespace().map(str::to_owned).collect()
}

fn resolve_of(line: &str) -> crate::cli::ResolveCommand {
    match parse(&arguments(line)).unwrap() {
        Command::Resolve(command) => command,
        other => panic!("expected a resolve command, got {other:?}"),
    }
}

fn run_of(line: &str) -> crate::cli::RunCommand {
    match parse(&arguments(line)).unwrap() {
        Command::Run(command) => command,
        other => panic!("expected a run command, got {other:?}"),
    }
}

fn suite_of(line: &str) -> crate::suite::SuiteCommand {
    match parse(&arguments(line)).unwrap() {
        Command::Suite(command) => command,
        other => panic!("expected a suite command, got {other:?}"),
    }
}

fn pack_of(line: &str) -> crate::pack::PackCommand {
    match parse(&arguments(line)).unwrap() {
        Command::Pack(command) => command,
        other => panic!("expected a pack command, got {other:?}"),
    }
}

fn capacity_of(line: &str) -> crate::capacity::CapacityCommand {
    match parse(&arguments(line)).unwrap() {
        Command::Capacity(command) => command,
        other => panic!("expected a capacity command, got {other:?}"),
    }
}

fn usage_kind(line: &str) -> CtlErrorKind {
    parse(&arguments(line)).unwrap_err().kind()
}

const MINIMAL_RESOLVE: &str = "resolve --experiment scenario.toml --subjects subjects.toml \
     --cluster cluster.toml --bootstrap localhost:9092";
const MINIMAL_RUN: &str = "run --experiment scenario.toml --subjects subjects.toml \
     --cluster cluster.toml --bootstrap localhost:9092";
const MINIMAL_SUITE: &str = "suite --experiment scenario.toml --subjects subjects.toml \
     --cluster cluster.toml --bootstrap localhost:9092 --repetitions 3";
const MINIMAL_CAPACITY: &str = "capacity --experiment scenario.toml --subjects subjects.toml \
     --cluster cluster.toml --bootstrap localhost:9092";

#[test]
fn the_minimal_resolve_command_names_its_three_documents() {
    let command = resolve_of(MINIMAL_RESOLVE);

    assert_eq!(command.common.experiment, PathBuf::from("scenario.toml"));
    assert_eq!(command.common.subjects, PathBuf::from("subjects.toml"));
    assert_eq!(command.common.cluster, PathBuf::from("cluster.toml"));
    assert_eq!(command.common.bootstrap, "localhost:9092");
    assert_eq!(command.common.seed, None);
    assert_eq!(command.common.order, None);
    assert_eq!(command.out, None);
}

#[test]
fn resolve_takes_a_seed_an_order_and_an_output_path() {
    let command = resolve_of(&format!(
        "{MINIMAL_RESOLVE} --seed 7 --order a,b --out /tmp/out.json"
    ));

    assert_eq!(command.common.seed, Some(7));
    assert_eq!(
        command.common.order,
        Some(vec!["a".to_owned(), "b".to_owned()])
    );
    assert_eq!(command.out, Some(PathBuf::from("/tmp/out.json")));
}

#[test]
fn run_defaults_its_results_root_and_its_budget() {
    let command = run_of(MINIMAL_RUN);

    assert_eq!(command.results_root, PathBuf::from(DEFAULT_RESULTS_ROOT));
    assert_eq!(command.budget, BudgetSpec::default());
}

#[test]
fn run_takes_a_results_root_and_all_three_timeouts() {
    let command = run_of(&format!(
        "{MINIMAL_RUN} --results /tmp/evidence --run-timeout-secs 900 \
         --tool-timeout-secs 30 --probe-timeout-secs 5"
    ));

    assert_eq!(command.results_root, PathBuf::from("/tmp/evidence"));
    assert_eq!(command.budget.run_timeout_seconds, 900);
    assert_eq!(command.budget.tool_timeout_seconds, 30);
    assert_eq!(command.budget.probe_timeout_seconds, 5);
    assert_eq!(
        command.budget.max_captured_output_bytes,
        BudgetSpec::default().max_captured_output_bytes,
        "the capture cap is not a flag and must keep its default"
    );
}

#[test]
fn a_flag_may_carry_its_value_after_an_equals_sign() {
    let command = resolve_of(
        "resolve --experiment=scenario.toml --subjects=subjects.toml \
         --cluster=cluster.toml --bootstrap=a:1,b:2 --seed=99",
    );

    assert_eq!(command.common.bootstrap, "a:1,b:2");
    assert_eq!(command.common.seed, Some(99));
}

#[test]
fn help_is_a_verb_and_a_flag() {
    for line in ["help", "--help", "-h"] {
        assert_eq!(parse(&arguments(line)).unwrap(), Command::Help);
    }
}

#[test]
fn an_empty_command_line_is_a_usage_error() {
    assert_eq!(usage_kind(""), CtlErrorKind::Usage);
}

#[test]
fn an_unknown_verb_is_a_usage_error() {
    assert_eq!(usage_kind("seal --experiment a"), CtlErrorKind::Usage);
}

/// Returns the command line with one flag and its value removed.
fn without(line: &str, flag: &str) -> Vec<String> {
    let mut words = arguments(line);
    let position = words.iter().position(|word| word == flag).unwrap();
    words.drain(position..=position + 1);
    words
}

#[test]
fn every_required_flag_is_required() {
    for missing in ["--experiment", "--subjects", "--cluster", "--bootstrap"] {
        let error = parse(&without(MINIMAL_RUN, missing)).unwrap_err();

        assert_eq!(
            error.kind(),
            CtlErrorKind::Usage,
            "{missing} was not required"
        );
        assert!(error.message().contains(missing), "{}", error.message());
    }
}

#[test]
fn an_unknown_flag_is_a_usage_error() {
    let error = parse(&arguments(&format!("{MINIMAL_RUN} --turbo yes"))).unwrap_err();

    assert_eq!(error.kind(), CtlErrorKind::Usage);
    assert!(error.message().contains("--turbo"), "{}", error.message());
}

#[test]
fn a_repeated_flag_is_a_usage_error() {
    let error = parse(&arguments(&format!("{MINIMAL_RUN} --bootstrap other:9092"))).unwrap_err();

    assert_eq!(error.kind(), CtlErrorKind::Usage);
    assert!(
        error.message().contains("more than once"),
        "{}",
        error.message()
    );
}

#[test]
fn a_flag_without_a_value_is_a_usage_error() {
    assert_eq!(
        usage_kind(&format!("{MINIMAL_RUN} --seed")),
        CtlErrorKind::Usage
    );
}

#[test]
fn a_bare_positional_argument_is_a_usage_error() {
    assert_eq!(
        usage_kind(&format!("{MINIMAL_RUN} extra")),
        CtlErrorKind::Usage
    );
}

#[test]
fn a_seed_that_is_not_a_number_is_a_usage_error() {
    assert_eq!(
        usage_kind(&format!("{MINIMAL_RUN} --seed twelve")),
        CtlErrorKind::Usage
    );
}

#[test]
fn an_order_with_an_empty_name_is_a_usage_error() {
    assert_eq!(
        usage_kind(&format!("{MINIMAL_RUN} --order kafkars,")),
        CtlErrorKind::Usage
    );
}

#[test]
fn a_zero_timeout_is_a_usage_error() {
    for flag in [
        "--run-timeout-secs",
        "--tool-timeout-secs",
        "--probe-timeout-secs",
    ] {
        assert_eq!(
            usage_kind(&format!("{MINIMAL_RUN} {flag} 0")),
            CtlErrorKind::Usage,
            "{flag} accepted zero"
        );
    }
}

#[test]
fn an_order_is_recorded_in_the_order_it_was_given() {
    let command = run_of(&format!("{MINIMAL_RUN} --order librdkafka-c,kafkars"));

    assert_eq!(
        command.common.order,
        Some(vec!["librdkafka-c".to_owned(), "kafkars".to_owned()])
    );
}

#[test]
fn suite_takes_the_run_flags_plus_repetitions_and_a_reports_root() {
    let command = suite_of(&format!(
        "{MINIMAL_SUITE} --results /tmp/evidence --reports /tmp/reports --run-timeout-secs 900"
    ));

    assert_eq!(command.repetitions, 3);
    assert_eq!(command.results_root, PathBuf::from("/tmp/evidence"));
    assert_eq!(command.reports_root, PathBuf::from("/tmp/reports"));
    assert_eq!(command.budget.run_timeout_seconds, 900);
    assert_eq!(command.common.bootstrap, "localhost:9092");
}

#[test]
fn suite_defaults_its_reports_root() {
    assert_eq!(
        suite_of(MINIMAL_SUITE).reports_root,
        PathBuf::from(DEFAULT_REPORTS_ROOT)
    );
}

#[test]
fn a_suite_of_fewer_than_two_repetitions_is_a_usage_error() {
    for count in ["0", "1"] {
        let line = MINIMAL_SUITE.replace("--repetitions 3", &format!("--repetitions {count}"));
        assert_eq!(usage_kind(&line), CtlErrorKind::Usage, "accepted {count}");
    }
}

#[test]
fn a_suite_without_a_repetition_count_is_a_usage_error() {
    let error = parse(&without(MINIMAL_SUITE, "--repetitions")).unwrap_err();

    assert_eq!(error.kind(), CtlErrorKind::Usage);
    assert!(
        error.message().contains("--repetitions"),
        "{}",
        error.message()
    );
}

#[test]
fn capacity_takes_the_run_flags_and_a_reports_root_but_no_repetitions() {
    let command = capacity_of(&format!("{MINIMAL_CAPACITY} --reports /tmp/reports"));

    assert_eq!(command.reports_root, PathBuf::from("/tmp/reports"));
    assert_eq!(command.results_root, PathBuf::from(DEFAULT_RESULTS_ROOT));
    assert_eq!(
        usage_kind(&format!("{MINIMAL_CAPACITY} --repetitions 3")),
        CtlErrorKind::Usage,
        "the scenario's repetitions_per_rate is the confirmation count"
    );
}

#[test]
fn report_names_a_bundle_and_optionally_a_destination() {
    match parse(&arguments(
        "report --bundle /tmp/bundle --out /tmp/report.md",
    ))
    .unwrap()
    {
        Command::Report(command) => {
            assert_eq!(command.bundle, PathBuf::from("/tmp/bundle"));
            assert_eq!(command.out, Some(PathBuf::from("/tmp/report.md")));
        }
        other => panic!("expected a report command, got {other:?}"),
    }
    assert_eq!(usage_kind("report"), CtlErrorKind::Usage);
}

#[test]
fn packet_names_both_documents_and_requires_both() {
    match parse(&arguments(
        "packet --suite /tmp/suite-summary.json --llm-summary /tmp/written.json",
    ))
    .unwrap()
    {
        Command::Packet(command) => {
            assert_eq!(command.suite, PathBuf::from("/tmp/suite-summary.json"));
            assert_eq!(command.llm_summary, PathBuf::from("/tmp/written.json"));
        }
        other => panic!("expected a packet command, got {other:?}"),
    }
    assert_eq!(
        usage_kind("packet --suite /tmp/suite-summary.json"),
        CtlErrorKind::Usage
    );
}

#[test]
fn pack_takes_a_manifest_instead_of_an_experiment() {
    // The scenario flag is absent on purpose: a pack's entries name the
    // scenarios, and accepting both would leave two answers to "what ran".
    let command = pack_of(
        "pack --manifest scenarios/packs/pr.toml --subjects /tmp/s.toml \
         --cluster /tmp/c.toml --bootstrap 127.0.0.1:9092",
    );
    assert_eq!(command.manifest, PathBuf::from("scenarios/packs/pr.toml"));
    assert_eq!(command.subjects, PathBuf::from("/tmp/s.toml"));
    assert_eq!(command.cluster, PathBuf::from("/tmp/c.toml"));
    assert_eq!(command.bootstrap, "127.0.0.1:9092");
    assert_eq!(command.results_root, PathBuf::from(DEFAULT_RESULTS_ROOT));
    assert_eq!(command.reports_root, PathBuf::from(DEFAULT_REPORTS_ROOT));
    assert_eq!(command.seed, None);
    assert_eq!(command.budget, BudgetSpec::default());
    assert_eq!(
        usage_kind(
            "pack --manifest scenarios/packs/pr.toml --subjects /tmp/s.toml \
             --cluster /tmp/c.toml --bootstrap 127.0.0.1:9092 \
             --experiment /tmp/e.toml"
        ),
        CtlErrorKind::Usage
    );
}

#[test]
fn pack_takes_the_roots_the_seed_and_every_timeout() {
    let command = pack_of(
        "pack --manifest scenarios/packs/nightly.toml --subjects /tmp/s.toml \
         --cluster /tmp/c.toml --bootstrap 127.0.0.1:9092 --results /tmp/r \
         --reports /tmp/rep --seed 7 --run-timeout-secs 900 \
         --tool-timeout-secs 60 --probe-timeout-secs 30",
    );
    assert_eq!(command.results_root, PathBuf::from("/tmp/r"));
    assert_eq!(command.reports_root, PathBuf::from("/tmp/rep"));
    assert_eq!(command.seed, Some(7));
    assert_eq!(command.budget.run_timeout_seconds, 900);
    assert_eq!(command.budget.tool_timeout_seconds, 60);
    assert_eq!(command.budget.probe_timeout_seconds, 30);
}

#[test]
fn pack_requires_the_manifest_the_subjects_the_cluster_and_the_bootstrap() {
    for line in [
        "pack --subjects /tmp/s.toml --cluster /tmp/c.toml --bootstrap 127.0.0.1:9092",
        "pack --manifest /tmp/p.toml --cluster /tmp/c.toml --bootstrap 127.0.0.1:9092",
        "pack --manifest /tmp/p.toml --subjects /tmp/s.toml --bootstrap 127.0.0.1:9092",
        "pack --manifest /tmp/p.toml --subjects /tmp/s.toml --cluster /tmp/c.toml",
    ] {
        assert_eq!(usage_kind(line), CtlErrorKind::Usage, "{line}");
    }
}

#[test]
fn the_usage_text_names_every_verb() {
    for verb in [
        "resolve", "run", "suite", "capacity", "pack", "report", "packet",
    ] {
        assert!(
            crate::cli::USAGE.contains(verb),
            "the usage text does not mention {verb}"
        );
    }
}

#[test]
fn an_unknown_flag_is_named_before_anything_looks_for_its_value() {
    // `--turbo` with nothing after it used to report that it needed a value,
    // which tells a reader the option exists and they got the syntax wrong.
    let error = parse(&arguments(&format!("{MINIMAL_RUN} --turbo"))).unwrap_err();

    assert_eq!(error.kind(), CtlErrorKind::Usage);
    assert!(
        error.message().starts_with("unknown option --turbo"),
        "{error}"
    );
    assert!(
        !error.message().contains("needs a value"),
        "an option that does not exist cannot be missing one: {error}"
    );
    // The same is true when a value *was* supplied.
    assert!(
        parse(&arguments(&format!("{MINIMAL_RUN} --turbo yes")))
            .unwrap_err()
            .message()
            .starts_with("unknown option --turbo")
    );
    // And the message says what this verb does take, so the next attempt is a
    // correction rather than another guess.
    assert!(
        parse(&arguments(&format!("{MINIMAL_RUN} --turbo yes")))
            .unwrap_err()
            .message()
            .contains("--experiment")
    );
}

#[test]
fn a_bootstrap_endpoint_that_cannot_be_one_is_refused_at_parse_time() {
    // A typo here is sealed into the runtime binding and the environment
    // document of every bundle the attempt writes, and a sealed bundle is
    // immutable. One line of stderr now costs less than an immutable record of
    // a cluster nobody ran against.
    for bad in [
        "localhost",
        "localhost:",
        "localhost:not-a-port",
        ":9092",
        "127.0.0.1:9092,localhost",
        "127.0.0.1:9092,127.0.0.1:99999",
    ] {
        let line =
            format!("run --experiment s.toml --subjects s.toml --cluster c.toml --bootstrap {bad}");
        assert_eq!(usage_kind(&line), CtlErrorKind::Usage, "{bad}");
    }
    for good in [
        "localhost:9092",
        "127.0.0.1:39092,127.0.0.1:39093,127.0.0.1:39094",
        "broker.internal:9093",
        "[::1]:9092",
    ] {
        let line = format!(
            "run --experiment s.toml --subjects s.toml --cluster c.toml --bootstrap {good}"
        );
        assert_eq!(run_of(&line).common.bootstrap, good);
    }
    // `pack` binds the same endpoints and checks them the same way.
    assert_eq!(
        usage_kind(
            "pack --manifest p.toml --subjects s.toml --cluster c.toml --bootstrap localhost"
        ),
        CtlErrorKind::Usage
    );
}
