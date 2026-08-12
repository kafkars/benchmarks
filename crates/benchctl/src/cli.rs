//! Command-line surface: hand-rolled argv parsing for every verb, the flag set
//! fixed by the design plan, and the dispatch into the pipeline, the suite, the
//! capacity search, and the reporting verbs.
//!
//! # The six verbs
//!
//! ```text
//! benchctl resolve  --experiment <toml> --subjects <toml> --cluster <toml>
//!                   --bootstrap <host:port,...> [--seed <n>] [--order <a,b>]
//!                   [--out <path>]
//!
//! benchctl run      <resolve flags> [--results <dir>] [--run-timeout-secs <n>]
//!                   [--tool-timeout-secs <n>] [--probe-timeout-secs <n>]
//!
//! benchctl suite    <run flags> --repetitions <n> [--reports <dir>]
//!
//! benchctl capacity <run flags> [--reports <dir>]
//!
//! benchctl report   --bundle <dir> [--out <path>]
//!
//! benchctl packet   --suite <suite-summary.json> --llm-summary <file>
//! ```
//!
//! `resolve` probes the subjects and prints the resolved experiment. It creates
//! no attempt workspace and seals nothing, so it is the safe way to ask "what
//! would this run, and under what experiment id" before spending a cluster on
//! it. `run` does everything `resolve` does and then hands the attempt to the
//! sealing supervisor, which is the only thing that writes into the bundle.
//!
//! `suite` and `capacity` are both loops over `run`. They take exactly the same
//! inputs, because a repetition and a probe must be the same kind of run as the
//! one a person would have made by hand — see [`crate::suite`] for the
//! execution-order rule and [`crate::capacity`] for the ladder.
//!
//! `report` and `packet` run nothing at all: they read sealed evidence back out,
//! and check prose written over it.
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
//! # Exit discipline
//!
//! Before an attempt workspace exists there is nowhere to seal, so failures
//! exit with the error's own code: 64 for a malformed command line, 65 for
//! unreadable or incoherent inputs, 73 for an attempt directory already
//! claimed. Once the workspace exists, *every* ending goes through the sealer:
//! a control-plane error seals a partial bundle and exits 20, a panic is caught
//! and seals a crashed bundle and exits 70, and a sealed attempt exits with the
//! code for its execution status. Only a failure to seal escapes as 74.
//!
//! The looping verbs keep the same two ranges. A suite exits 0 only when every
//! attempt sealed `complete` and at least two classified valid, and 20
//! otherwise; a capacity search exits 0 when it converged and 20 when it did
//! not. An unconverged search is a sealed outcome, not a fault.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::time::SystemTime;

use bench_schema::{BudgetSpec, pretty_bytes};

use crate::attempt::AttemptId;
use crate::capacity::CapacityCommand;
use crate::error::{CtlError, CtlResult, EXIT_SEALED_COMPLETE};
use crate::pipeline::{self, AttemptEnd, CommonArguments};
use crate::report::{PacketCommand, ReportCommand};
use crate::suite::{DEFAULT_REPORTS_ROOT, MINIMUM_REPETITIONS, SuiteCommand};
use crate::{capacity, report, suite};

/// The full usage text, printed for `--help` and quoted on a usage error.
pub const USAGE: &str = "\
usage:
  benchctl resolve  --experiment <toml> --subjects <toml> --cluster <toml>
                    --bootstrap <host:port,...> [--seed <n>] [--order <a,b>]
                    [--out <path>]
  benchctl run      <resolve flags> [--results <dir>] [--run-timeout-secs <n>]
                    [--tool-timeout-secs <n>] [--probe-timeout-secs <n>]
  benchctl suite    <run flags> --repetitions <n> [--reports <dir>]
  benchctl capacity <run flags> [--reports <dir>]
  benchctl report   --bundle <dir> [--out <path>]
  benchctl packet   --suite <suite-summary.json> --llm-summary <file>

  resolve   probe the subjects and print the resolved experiment; seals nothing
  run       resolve, execute every subject, and always seal an evidence bundle
  suite     run the same experiment N times and summarize the repetitions
  capacity  search for the highest offered rate that still meets the objectives
  report    render one sealed bundle as markdown
  packet    check an LLM summary against the packet derived from a suite";

/// Default directory the evidence tree is created under.
pub const DEFAULT_RESULTS_ROOT: &str = "results";

/// `benchctl resolve`: probe and resolve, write the document, seal nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveCommand {
    /// Shared inputs.
    pub common: CommonArguments,
    /// Where the resolved document goes; `None` means stdout.
    pub out: Option<PathBuf>,
}

/// `benchctl run`: the whole pipeline, ending in a sealed bundle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunCommand {
    /// Shared inputs.
    pub common: CommonArguments,
    /// Root of the evidence tree.
    pub results_root: PathBuf,
    /// Ceilings this attempt declares for itself.
    pub budget: BudgetSpec,
}

/// A parsed command line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Print [`USAGE`] and exit successfully.
    Help,
    /// Resolve and print.
    Resolve(ResolveCommand),
    /// Resolve, run, and seal.
    Run(RunCommand),
    /// Repeat one experiment and summarize the repetitions.
    Suite(SuiteCommand),
    /// Search for the highest satisfying offered rate.
    Capacity(CapacityCommand),
    /// Render one sealed bundle.
    Report(ReportCommand),
    /// Check an LLM summary against its packet.
    Packet(PacketCommand),
}

/// Runs `benchctl` and returns the process exit code.
///
/// `argv` is the full argument vector including the program name, so that
/// `main` can hand over `std::env::args()` unchanged.
#[must_use]
pub fn run(argv: &[String]) -> i32 {
    match parse(argv.get(1..).unwrap_or_default()) {
        Ok(Command::Help) => {
            println!("{USAGE}");
            EXIT_SEALED_COMPLETE
        }
        Ok(Command::Resolve(command)) => match execute_resolve(&command) {
            Ok(()) => EXIT_SEALED_COMPLETE,
            Err(error) => report_error(&error),
        },
        Ok(Command::Run(command)) => execute_run(&command),
        Ok(Command::Suite(command)) => suite::execute(&command),
        Ok(Command::Capacity(command)) => capacity::execute(&command),
        Ok(Command::Report(command)) => report::execute_report(&command),
        Ok(Command::Packet(command)) => report::execute_packet(&command),
        Err(error) => {
            eprintln!("{USAGE}");
            report_error(&error)
        }
    }
}

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
                                    suite, capacity, report, packet",
        ));
    };
    match verb.as_str() {
        "help" | "--help" | "-h" => Ok(Command::Help),
        "resolve" => parse_resolve(rest),
        "run" => parse_run(rest),
        "suite" => parse_suite(rest),
        "capacity" => parse_capacity(rest),
        "report" => parse_report(rest),
        "packet" => parse_packet(rest),
        other => Err(CtlError::usage(format!(
            "unknown verb {other:?}; expected one of resolve, run, suite, \
             capacity, report, packet"
        ))),
    }
}

fn parse_resolve(arguments: &[String]) -> CtlResult<Command> {
    let mut flags = collect_flags(arguments)?;
    let common = parse_common(&mut flags)?;
    let out = flags.remove("out").map(PathBuf::from);
    reject_unknown(&flags)?;
    Ok(Command::Resolve(ResolveCommand { common, out }))
}

fn parse_run(arguments: &[String]) -> CtlResult<Command> {
    let mut flags = collect_flags(arguments)?;
    let common = parse_common(&mut flags)?;
    let results_root = results_root(&mut flags);
    let budget = parse_budget(&mut flags)?;
    reject_unknown(&flags)?;
    Ok(Command::Run(RunCommand {
        common,
        results_root,
        budget,
    }))
}

fn parse_suite(arguments: &[String]) -> CtlResult<Command> {
    let mut flags = collect_flags(arguments)?;
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
    reject_unknown(&flags)?;
    Ok(Command::Suite(SuiteCommand {
        common,
        results_root,
        budget,
        repetitions,
        reports_root,
    }))
}

fn parse_capacity(arguments: &[String]) -> CtlResult<Command> {
    let mut flags = collect_flags(arguments)?;
    let common = parse_common(&mut flags)?;
    let results_root = results_root(&mut flags);
    let reports_root = reports_root(&mut flags);
    let budget = parse_budget(&mut flags)?;
    reject_unknown(&flags)?;
    Ok(Command::Capacity(CapacityCommand {
        common,
        results_root,
        budget,
        reports_root,
    }))
}

fn parse_report(arguments: &[String]) -> CtlResult<Command> {
    let mut flags = collect_flags(arguments)?;
    let bundle = PathBuf::from(required(&mut flags, "bundle")?);
    let out = flags.remove("out").map(PathBuf::from);
    reject_unknown(&flags)?;
    Ok(Command::Report(ReportCommand { bundle, out }))
}

fn parse_packet(arguments: &[String]) -> CtlResult<Command> {
    let mut flags = collect_flags(arguments)?;
    let suite = PathBuf::from(required(&mut flags, "suite")?);
    let llm_summary = PathBuf::from(required(&mut flags, "llm-summary")?);
    reject_unknown(&flags)?;
    Ok(Command::Packet(PacketCommand { suite, llm_summary }))
}

fn parse_common(flags: &mut BTreeMap<String, String>) -> CtlResult<CommonArguments> {
    Ok(CommonArguments {
        experiment: PathBuf::from(required(flags, "experiment")?),
        subjects: PathBuf::from(required(flags, "subjects")?),
        cluster: PathBuf::from(required(flags, "cluster")?),
        bootstrap: required(flags, "bootstrap")?,
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

/// Splits `--name value` and `--name=value` pairs, refusing repeats.
fn collect_flags(arguments: &[String]) -> CtlResult<BTreeMap<String, String>> {
    let mut flags = BTreeMap::new();
    let mut index = 0;
    while index < arguments.len() {
        let argument = &arguments[index];
        let Some(flag) = argument.strip_prefix("--") else {
            return Err(CtlError::usage(format!(
                "unexpected argument {argument:?}; every option starts with --"
            )));
        };
        let (name, value) = if let Some((name, value)) = flag.split_once('=') {
            index += 1;
            (name.to_owned(), value.to_owned())
        } else {
            let value = arguments.get(index + 1).ok_or_else(|| {
                CtlError::usage(format!("--{flag} needs a value, and none followed it"))
            })?;
            index += 2;
            (flag.to_owned(), value.clone())
        };
        if name.is_empty() {
            return Err(CtlError::usage("-- is not an option"));
        }
        if flags.insert(name.clone(), value).is_some() {
            return Err(CtlError::usage(format!(
                "--{name} was given more than once"
            )));
        }
    }
    Ok(flags)
}

fn required(flags: &mut BTreeMap<String, String>, name: &str) -> CtlResult<String> {
    flags
        .remove(name)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| CtlError::usage(format!("--{name} is required")))
}

fn seconds(flags: &mut BTreeMap<String, String>, name: &str, fallback: u64) -> CtlResult<u64> {
    match flags.remove(name) {
        None => Ok(fallback),
        Some(value) => {
            let parsed = unsigned(name, &value)?;
            if parsed == 0 {
                return Err(CtlError::usage(format!("--{name} must be positive")));
            }
            Ok(parsed)
        }
    }
}

fn unsigned(name: &str, value: &str) -> CtlResult<u64> {
    value
        .parse()
        .map_err(|_| CtlError::usage(format!("--{name} expects a whole number, found {value:?}")))
}

fn order(value: &str) -> CtlResult<Vec<String>> {
    let names: Vec<String> = value.split(',').map(str::trim).map(str::to_owned).collect();
    if names.iter().any(String::is_empty) {
        return Err(CtlError::usage(format!(
            "--order expects comma-separated subject names, found {value:?}"
        )));
    }
    Ok(names)
}

fn reject_unknown(flags: &BTreeMap<String, String>) -> CtlResult<()> {
    if let Some(name) = flags.keys().next() {
        return Err(CtlError::usage(format!("unknown option --{name}")));
    }
    Ok(())
}

fn report_error(error: &CtlError) -> i32 {
    eprintln!("benchctl: {error}");
    error.exit_code()
}

fn execute_resolve(command: &ResolveCommand) -> CtlResult<()> {
    let inputs = pipeline::load(&command.common)?;
    let id = AttemptId::generate(SystemTime::now());
    let scratch = pipeline::scratch_directory(&id)?;
    let resolved = pipeline::probe_and_resolve(
        &inputs,
        &command.common,
        BudgetSpec::default(),
        &id,
        &scratch,
    )
    .map(|(resolved, _lock)| resolved);
    let _ = std::fs::remove_dir_all(&scratch);
    let bytes = pretty_bytes(&resolved?)?;
    match &command.out {
        Some(path) => std::fs::write(path, bytes)
            .map_err(|error| CtlError::internal(format!("write {}: {error}", path.display()))),
        None => std::io::stdout()
            .write_all(&bytes)
            .map_err(|error| CtlError::internal(format!("write stdout: {error}"))),
    }
}

fn execute_run(command: &RunCommand) -> i32 {
    let inputs = match pipeline::load(&command.common) {
        Ok(inputs) => inputs,
        Err(error) => return report_error(&error),
    };
    match pipeline::attempt(
        &inputs,
        &command.common,
        &command.results_root,
        command.budget,
    ) {
        end @ AttemptEnd::Sealed(_) => end.exit_code(),
        AttemptEnd::Unsealable(error) => error.exit_code(),
    }
}
