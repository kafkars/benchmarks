//! One parser per verb, over the flags the argument vector carried.
//!
//! `suite` and `capacity` are both loops over `run`. They take exactly the same
//! inputs, because a repetition and a probe must be the same kind of run as the
//! one a person would have made by hand — see [`crate::suite`] for the
//! execution-order rule and [`crate::capacity`] for the ladder.
//!
//! `pack` is a loop over those three. It takes no `--experiment`, because a
//! pack manifest names the scenarios; everything else a run needs is passed
//! once and shared by every entry, which is what lets the same reviewed pack
//! describe a laptop and a nightly runner. See [`crate::pack`] for the dispatch
//! rule. `report` and `packet` run nothing at all: they read sealed evidence
//! back out, and check prose written over it.
//!
//! # Flags that differ from the design plan
//!
//! The plan named one `--experiment` input. This surface has three, because the
//! schema crate models three separate deny-unknown-fields documents: a scenario
//! is reviewed and stable, a subject list says which binaries exist on this
//! machine today, and a cluster profile says where the brokers are and which
//! tools reach them. Merging them would mean one file whose keys are read by
//! three different audiences.
//!
//! `--bootstrap` is required even though a cluster profile also carries one:
//! the profile's value is the checked-in default for a named cluster, and the
//! flag is the endpoint this attempt actually binds to. The flag wins, and the
//! bound value is what the runtime binding and the environment document record.
//!
//! `--out` is a convenience for `resolve` and `report`; without it the document
//! goes to stdout, which is what a shell pipeline wants.
//!
//! The three timeout flags exist on every verb that runs an attempt. They land
//! in the experiment's budget, which is part of the hashed intent, so a
//! `resolve` — which reports the default budget — and a `run` with overridden
//! timeouts describe genuinely different experiments and correctly get different
//! ids.
//!
//! `--repetitions` is required on `suite` and must be at least two: one
//! repetition is a run, and calling it a suite would put an empty dispersion
//! column next to a single measurement. `capacity` takes no repetition flag
//! because the scenario's `repetitions_per_rate` already says how many times a
//! candidate rate must be confirmed.
//!
//! Every verb declares its own option names as a constant and hands them to
//! [`collect_flags`], which is what lets an unknown name be reported as one
//! before anything looks for its value.

use std::collections::BTreeMap;
use std::path::PathBuf;

use bench_schema::BudgetSpec;

use crate::capacity::CapacityCommand;
use crate::error::{CtlError, CtlResult};
use crate::pack::PackCommand;
use crate::pipeline::CommonArguments;
use crate::report::{PacketCommand, ReportCommand};
use crate::suite::{DEFAULT_REPORTS_ROOT, MINIMUM_REPETITIONS, SuiteCommand};

use super::command::{Command, ResolveCommand, RunCommand};
use super::flags::{bootstrap, collect_flags, order, required, seconds, unsigned};
use super::usage::DEFAULT_RESULTS_ROOT;

/// Parses the argument vector *after* the program name.
///
/// # Errors
///
/// Returns a usage error for an unknown verb, an unknown or repeated flag, a
/// flag missing its value, or a missing required flag.
pub fn parse(arguments: &[String]) -> CtlResult<Command> {
    let Some((verb, rest)) = arguments.split_first() else {
        return Err(CtlError::usage(
            "no verb; expected one of resolve, run, \
                                    suite, capacity, pack, report, packet",
        ));
    };
    match verb.as_str() {
        "help" | "--help" | "-h" => Ok(Command::Help),
        "resolve" => parse_resolve(rest),
        "run" => parse_run(rest),
        "suite" => parse_suite(rest),
        "capacity" => parse_capacity(rest),
        "pack" => parse_pack(rest),
        "report" => parse_report(rest),
        "packet" => parse_packet(rest),
        other => Err(CtlError::usage(format!(
            "unknown verb {other:?}; expected one of resolve, run, suite, \
             capacity, pack, report, packet"
        ))),
    }
}

/// The inputs every attempt-making verb shares.
const COMMON: [&str; 6] = [
    "experiment",
    "subjects",
    "cluster",
    "bootstrap",
    "seed",
    "order",
];

/// The three timeout flags every attempt-making verb shares.
const BUDGET: [&str; 3] = [
    "run-timeout-secs",
    "tool-timeout-secs",
    "probe-timeout-secs",
];

/// This verb's option names: the shared sets plus whatever it adds.
fn options(extra: &[&'static str]) -> Vec<&'static str> {
    let mut names: Vec<&'static str> = COMMON.to_vec();
    names.extend(BUDGET);
    names.extend(extra);
    names
}

fn parse_resolve(arguments: &[String]) -> CtlResult<Command> {
    let mut flags = collect_flags(arguments, &options(&["out"]))?;
    let common = parse_common(&mut flags)?;
    let out = flags.remove("out").map(PathBuf::from);
    Ok(Command::Resolve(ResolveCommand { common, out }))
}

fn parse_run(arguments: &[String]) -> CtlResult<Command> {
    let mut flags = collect_flags(arguments, &options(&["results"]))?;
    let common = parse_common(&mut flags)?;
    let results_root = results_root(&mut flags);
    let budget = parse_budget(&mut flags)?;
    Ok(Command::Run(RunCommand {
        common,
        results_root,
        budget,
    }))
}

fn parse_suite(arguments: &[String]) -> CtlResult<Command> {
    let mut flags = collect_flags(arguments, &options(&["results", "reports", "repetitions"]))?;
    let common = parse_common(&mut flags)?;
    let results_root = results_root(&mut flags);
    let reports_root = reports_root(&mut flags);
    let budget = parse_budget(&mut flags)?;
    let value = flags
        .remove("repetitions")
        .ok_or_else(|| CtlError::usage("--repetitions is required"))?;
    let repetitions = u32::try_from(unsigned("repetitions", &value)?)
        .map_err(|_| CtlError::usage("--repetitions is implausibly large"))?;
    if repetitions < MINIMUM_REPETITIONS {
        return Err(CtlError::usage(format!(
            "--repetitions must be at least {MINIMUM_REPETITIONS}; one repetition is a run"
        )));
    }
    Ok(Command::Suite(SuiteCommand {
        common,
        results_root,
        budget,
        repetitions,
        reports_root,
    }))
}

fn parse_capacity(arguments: &[String]) -> CtlResult<Command> {
    let mut flags = collect_flags(arguments, &options(&["results", "reports"]))?;
    let common = parse_common(&mut flags)?;
    let results_root = results_root(&mut flags);
    let reports_root = reports_root(&mut flags);
    let budget = parse_budget(&mut flags)?;
    Ok(Command::Capacity(CapacityCommand {
        common,
        results_root,
        budget,
        reports_root,
    }))
}

/// `pack` shares every input a run takes except the scenario, which each
/// manifest entry names for itself.
fn parse_pack(arguments: &[String]) -> CtlResult<Command> {
    let mut names: Vec<&'static str> = vec![
        "manifest",
        "subjects",
        "cluster",
        "bootstrap",
        "seed",
        "results",
        "reports",
    ];
    names.extend(BUDGET);
    let mut flags = collect_flags(arguments, &names)?;
    let manifest = PathBuf::from(required(&mut flags, "manifest")?);
    let subjects = PathBuf::from(required(&mut flags, "subjects")?);
    let cluster = PathBuf::from(required(&mut flags, "cluster")?);
    let bootstrap = bootstrap(&mut flags)?;
    let seed = match flags.remove("seed") {
        None => None,
        Some(value) => Some(unsigned("seed", &value)?),
    };
    let results_root = results_root(&mut flags);
    let reports_root = reports_root(&mut flags);
    let budget = parse_budget(&mut flags)?;
    Ok(Command::Pack(PackCommand {
        manifest,
        subjects,
        cluster,
        bootstrap,
        results_root,
        reports_root,
        budget,
        seed,
    }))
}

fn parse_report(arguments: &[String]) -> CtlResult<Command> {
    let mut flags = collect_flags(arguments, &["bundle", "out"])?;
    let bundle = PathBuf::from(required(&mut flags, "bundle")?);
    let out = flags.remove("out").map(PathBuf::from);
    Ok(Command::Report(ReportCommand { bundle, out }))
}

fn parse_packet(arguments: &[String]) -> CtlResult<Command> {
    let mut flags = collect_flags(arguments, &["suite", "llm-summary"])?;
    let suite = PathBuf::from(required(&mut flags, "suite")?);
    let llm_summary = PathBuf::from(required(&mut flags, "llm-summary")?);
    Ok(Command::Packet(PacketCommand { suite, llm_summary }))
}

fn parse_common(flags: &mut BTreeMap<String, String>) -> CtlResult<CommonArguments> {
    Ok(CommonArguments {
        experiment: PathBuf::from(required(flags, "experiment")?),
        subjects: PathBuf::from(required(flags, "subjects")?),
        cluster: PathBuf::from(required(flags, "cluster")?),
        bootstrap: bootstrap(flags)?,
        seed: match flags.remove("seed") {
            None => None,
            Some(value) => Some(unsigned("seed", &value)?),
        },
        order: match flags.remove("order") {
            None => None,
            Some(value) => Some(order(&value)?),
        },
    })
}

/// The three timeout flags every attempt-making verb shares.
fn parse_budget(flags: &mut BTreeMap<String, String>) -> CtlResult<BudgetSpec> {
    let defaults = BudgetSpec::default();
    Ok(BudgetSpec {
        run_timeout_seconds: seconds(flags, "run-timeout-secs", defaults.run_timeout_seconds)?,
        tool_timeout_seconds: seconds(flags, "tool-timeout-secs", defaults.tool_timeout_seconds)?,
        probe_timeout_seconds: seconds(
            flags,
            "probe-timeout-secs",
            defaults.probe_timeout_seconds,
        )?,
        max_captured_output_bytes: defaults.max_captured_output_bytes,
    })
}

fn results_root(flags: &mut BTreeMap<String, String>) -> PathBuf {
    flags
        .remove("results")
        .map_or_else(|| PathBuf::from(DEFAULT_RESULTS_ROOT), PathBuf::from)
}

fn reports_root(flags: &mut BTreeMap<String, String>) -> PathBuf {
    flags
        .remove("reports")
        .map_or_else(|| PathBuf::from(DEFAULT_REPORTS_ROOT), PathBuf::from)
}
