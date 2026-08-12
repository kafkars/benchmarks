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

fn usage_kind(line: &str) -> CtlErrorKind {
    parse(&arguments(line)).unwrap_err().kind()
}

const MINIMAL_RESOLVE: &str = "resolve --experiment scenario.toml --subjects subjects.toml \
     --cluster cluster.toml --bootstrap localhost:9092";
const MINIMAL_RUN: &str = "run --experiment scenario.toml --subjects subjects.toml \
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
