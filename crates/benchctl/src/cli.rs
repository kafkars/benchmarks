//! Command-line surface: hand-rolled argv parsing for `benchctl resolve` and
//! `benchctl run`, the flag set fixed by the design plan, and the mapping from
//! parsed commands to the resolve → supervise → seal pipeline.
//!
//! # The two verbs
//!
//! ```text
//! benchctl resolve --experiment <toml> --subjects <toml> --cluster <toml>
//!                  --bootstrap <host:port,...> [--seed <n>] [--order <a,b>]
//!                  [--out <path>]
//!
//! benchctl run     --experiment <toml> --subjects <toml> --cluster <toml>
//!                  --bootstrap <host:port,...> [--seed <n>] [--order <a,b>]
//!                  [--results <dir>] [--run-timeout-secs <n>]
//!                  [--tool-timeout-secs <n>] [--probe-timeout-secs <n>]
//! ```
//!
//! `resolve` probes the subjects and prints the resolved experiment. It creates
//! no attempt workspace and seals nothing, so it is the safe way to ask "what
//! would this run, and under what experiment id" before spending a cluster on
//! it. `run` does everything `resolve` does and then hands the attempt to the
//! sealing supervisor, which is the only thing that writes into the bundle.
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
//! `--out` is a convenience for `resolve` alone; without it the document goes to
//! stdout, which is what a shell pipeline wants.
//!
//! The three timeout flags exist only on `run`. They land in the experiment's
//! budget, which is part of the hashed intent, so a `resolve` run — which
//! reports the default budget — and a `run` with overridden timeouts describe
//! genuinely different experiments and correctly get different ids.
//!
//! # Ordering, and why validate runs after a first resolution
//!
//! `validate` takes a resolved experiment *as a file*, so the document must
//! exist before any adapter can be asked about it. The pipeline therefore
//! resolves twice: once from the describe documents alone, to have something to
//! hand to `validate`, and once with the verdicts in hand to build the subjects
//! lock. The resolution is pure and deterministic, so the second pass reproduces
//! the first byte for byte; the provisional copy is written to a scratch
//! directory outside the results tree, never into the bundle, because the bundle
//! is written by the sealing pass alone.
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

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::Write;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use bench_schema::{
    BudgetSpec, ClusterProfile, ExecutionStatus, ExperimentId, ResolvedExperiment,
    SourceExperiment, SubjectsFile, SubjectsLock, experiment_id, pretty_bytes, sha256_hex,
};

use crate::attempt::{AttemptId, AttemptPaths};
use crate::error::{
    CtlError, CtlResult, EXIT_INTERNAL, EXIT_SEALED_COMPLETE, exit_code_for_status,
};
use crate::resolve::{ResolveInputs, RuntimeInputs, SubjectProbe};
use crate::seal::AttemptRequest;
use crate::{environment, probe, resolve, seal};

/// The full usage text, printed for `--help` and quoted on a usage error.
pub const USAGE: &str = "\
usage:
  benchctl resolve --experiment <toml> --subjects <toml> --cluster <toml>
                   --bootstrap <host:port,...> [--seed <n>] [--order <a,b>]
                   [--out <path>]
  benchctl run     --experiment <toml> --subjects <toml> --cluster <toml>
                   --bootstrap <host:port,...> [--seed <n>] [--order <a,b>]
                   [--results <dir>] [--run-timeout-secs <n>]
                   [--tool-timeout-secs <n>] [--probe-timeout-secs <n>]

  resolve  probe the subjects and print the resolved experiment; seals nothing
  run      resolve, execute every subject, and always seal an evidence bundle";

/// Default directory the evidence tree is created under.
pub const DEFAULT_RESULTS_ROOT: &str = "results";

/// The inputs both verbs share.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommonArguments {
    /// Path to the scenario TOML.
    pub experiment: PathBuf,
    /// Path to the subject list TOML.
    pub subjects: PathBuf,
    /// Path to the cluster profile TOML.
    pub cluster: PathBuf,
    /// Bootstrap servers this attempt binds to.
    pub bootstrap: String,
    /// Seed override; `None` keeps the scenario's payload seed.
    pub seed: Option<u64>,
    /// Execution order override, in the order given.
    pub order: Option<Vec<String>>,
}

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
            Err(error) => report(&error),
        },
        Ok(Command::Run(command)) => execute_run(&command),
        Err(error) => {
            eprintln!("{USAGE}");
            report(&error)
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
        return Err(CtlError::usage("no verb; expected resolve or run"));
    };
    match verb.as_str() {
        "help" | "--help" | "-h" => Ok(Command::Help),
        "resolve" => parse_resolve(rest),
        "run" => parse_run(rest),
        other => Err(CtlError::usage(format!(
            "unknown verb {other:?}; expected resolve or run"
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
    let results_root = flags
        .remove("results")
        .map_or_else(|| PathBuf::from(DEFAULT_RESULTS_ROOT), PathBuf::from);
    let defaults = BudgetSpec::default();
    let budget = BudgetSpec {
        run_timeout_seconds: seconds(&mut flags, "run-timeout-secs", defaults.run_timeout_seconds)?,
        tool_timeout_seconds: seconds(
            &mut flags,
            "tool-timeout-secs",
            defaults.tool_timeout_seconds,
        )?,
        probe_timeout_seconds: seconds(
            &mut flags,
            "probe-timeout-secs",
            defaults.probe_timeout_seconds,
        )?,
        max_captured_output_bytes: defaults.max_captured_output_bytes,
    };
    reject_unknown(&flags)?;
    Ok(Command::Run(RunCommand {
        common,
        results_root,
        budget,
    }))
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

fn report(error: &CtlError) -> i32 {
    eprintln!("benchctl: {error}");
    error.exit_code()
}

/// The three input documents, plus the scenario text that gets sealed verbatim.
#[derive(Debug, Clone)]
struct LoadedInputs {
    source_toml: String,
    source: SourceExperiment,
    subjects: SubjectsFile,
    cluster: ClusterProfile,
}

fn load(common: &CommonArguments) -> CtlResult<LoadedInputs> {
    let source_toml = read(&common.experiment)?;
    let subjects_toml = read(&common.subjects)?;
    let cluster_toml = read(&common.cluster)?;
    Ok(LoadedInputs {
        source: SourceExperiment::from_toml_str(&source_toml)?,
        subjects: SubjectsFile::from_toml_str(&subjects_toml)?,
        cluster: ClusterProfile::from_toml_str(&cluster_toml)?,
        source_toml,
    })
}

fn read(path: &Path) -> CtlResult<String> {
    std::fs::read_to_string(path)
        .map_err(|error| CtlError::invalid(format!("read {}: {error}", path.display())))
}

/// Probes every subject and resolves the experiment, leaving the resolved
/// document, its lock, and the scratch directory the provisional copy lived in.
fn probe_and_resolve(
    inputs: &LoadedInputs,
    common: &CommonArguments,
    budget: BudgetSpec,
    attempt: &AttemptId,
    scratch: &Path,
) -> CtlResult<(ResolvedExperiment, SubjectsLock)> {
    if inputs.subjects.subjects.is_empty() {
        return Err(CtlError::invalid(
            "the subjects file names nothing to measure",
        ));
    }
    let timeout = Duration::from_secs(budget.probe_timeout_seconds);
    let cap = budget.max_captured_output_bytes;
    let mut probes = Vec::with_capacity(inputs.subjects.subjects.len());
    for entry in &inputs.subjects.subjects {
        let describe = probe::describe(&entry.command, timeout, cap)?;
        probes.push(SubjectProbe {
            subject: entry.clone(),
            describe,
            validate: None,
            binary_sha256: binary_digest(&entry.command),
        });
    }

    let mut resolve_inputs = ResolveInputs {
        source: inputs.source.clone(),
        cluster: inputs.cluster.clone(),
        subjects: probes,
        seed: common.seed,
        budget,
        runtime: RuntimeInputs {
            bootstrap: common.bootstrap.clone(),
            attempt_id: attempt.as_str().to_owned(),
            order: common.order.clone(),
        },
    };

    let provisional = resolve::resolve_experiment(&resolve_inputs)?;
    let provisional_path = scratch.join("experiment.resolved.json");
    write_document(&provisional_path, &provisional)?;
    for subject in &mut resolve_inputs.subjects {
        let report = probe::validate(&subject.subject.command, &provisional_path, timeout, cap)?;
        subject.validate = Some(report);
    }
    resolve::resolve_pure(&resolve_inputs)
}

/// Hashes a subject's program file when it is a readable file.
///
/// A subject invoked through a wrapper, a shell builtin, or a bare name found
/// on `PATH` has no digest here, and the lock records that honestly rather than
/// inventing a placeholder.
fn binary_digest(command: &[String]) -> Option<String> {
    let program = command.first()?;
    let bytes = std::fs::read(program).ok()?;
    Some(sha256_hex(&bytes))
}

fn write_document(path: &Path, document: &ResolvedExperiment) -> CtlResult<()> {
    let bytes = pretty_bytes(document)?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| CtlError::internal(format!("create {}: {error}", parent.display())))?;
    }
    std::fs::write(path, bytes)
        .map_err(|error| CtlError::internal(format!("write {}: {error}", path.display())))
}

/// Creates a scratch directory outside the results tree for the provisional
/// resolved document that `validate` is handed.
fn scratch_directory(attempt: &AttemptId) -> CtlResult<PathBuf> {
    let path = std::env::temp_dir().join(format!("benchctl-{}", attempt.as_str()));
    std::fs::create_dir_all(&path)
        .map_err(|error| CtlError::internal(format!("create {}: {error}", path.display())))?;
    Ok(path)
}

fn execute_resolve(command: &ResolveCommand) -> CtlResult<()> {
    let inputs = load(&command.common)?;
    let attempt = AttemptId::generate(SystemTime::now());
    let scratch = scratch_directory(&attempt)?;
    let resolved = probe_and_resolve(
        &inputs,
        &command.common,
        BudgetSpec::default(),
        &attempt,
        &scratch,
    )
    .map(|(resolved, _lock)| resolved);
    let _ = std::fs::remove_dir_all(&scratch);
    let resolved = resolved?;
    let bytes = pretty_bytes(&resolved)?;
    match &command.out {
        Some(path) => std::fs::write(path, bytes)
            .map_err(|error| CtlError::internal(format!("write {}: {error}", path.display()))),
        None => std::io::stdout()
            .write_all(&bytes)
            .map_err(|error| CtlError::internal(format!("write stdout: {error}"))),
    }
}

fn execute_run(command: &RunCommand) -> i32 {
    let inputs = match load(&command.common) {
        Ok(inputs) => inputs,
        Err(error) => return report(&error),
    };
    let started_at = SystemTime::now();
    let attempt = AttemptId::generate(started_at);
    let paths = match AttemptPaths::create_pending(&command.results_root, &attempt) {
        Ok(paths) => paths,
        Err(error) => return report(&error),
    };
    // The workspace moves out of `pending/` the moment the experiment id
    // exists, and a failure after that point has to seal into the new location.
    // The cell is how the sealing arm below learns where the bundle went.
    let paths = RefCell::new(paths);

    let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
        attempt_pipeline(command, &inputs, &attempt, &paths, started_at)
    }));
    let sealed = paths.borrow().clone();
    match outcome {
        Ok(Ok(status)) => exit_code_for_status(status),
        Ok(Err(error)) => {
            report(&error);
            match seal::seal_failure(
                &sealed,
                Some(&inputs.source_toml),
                ExecutionStatus::Partial,
                &error.to_string(),
            ) {
                Ok(status) => exit_code_for_status(status),
                Err(seal_error) => report(&seal_error),
            }
        }
        Err(payload) => {
            let reason = panic_reason(payload.as_ref());
            eprintln!("benchctl: panicked: {reason}");
            match seal::seal_failure(
                &sealed,
                Some(&inputs.source_toml),
                ExecutionStatus::Crashed,
                &reason,
            ) {
                Ok(_) => EXIT_INTERNAL,
                Err(seal_error) => report(&seal_error),
            }
        }
    }
}

/// Everything that happens once the workspace exists, so that one
/// `catch_unwind` covers all of it.
fn attempt_pipeline(
    command: &RunCommand,
    inputs: &LoadedInputs,
    attempt: &AttemptId,
    paths: &RefCell<AttemptPaths>,
    started_at: SystemTime,
) -> CtlResult<ExecutionStatus> {
    let scratch = scratch_directory(attempt)?;
    let resolution = probe_and_resolve(inputs, &command.common, command.budget, attempt, &scratch);
    let _ = std::fs::remove_dir_all(&scratch);
    let (resolved, lock) = resolution?;

    let identity: ExperimentId = experiment_id(&resolved)?;
    let pending = paths.borrow().clone();
    let finalized = pending.finalize(&command.results_root, identity.short())?;
    paths.replace(finalized.clone());

    let repository_root = repository_root();
    let repositories = environment::default_repositories(&repository_root);
    let mut environment =
        environment::capture(&repositories, &command.common.bootstrap, SystemTime::now());
    // The profile is the operator's statement about a cluster this control
    // plane cannot interrogate; taking it here is the only way those two facts
    // reach the sealed environment document.
    if let Some(version) = &inputs.cluster.broker_version {
        environment.broker.version.clone_from(version);
    }
    if let Some(lifecycle) = &inputs.cluster.lifecycle {
        environment.broker.lifecycle.clone_from(lifecycle);
    }

    seal::run_attempt(AttemptRequest {
        resolved,
        lock,
        source_toml: inputs.source_toml.clone(),
        environment,
        tools: inputs.cluster.tools.clone(),
        paths: finalized,
        started_at,
    })
}

/// Where the repository containing this checkout lives, for source capture.
fn repository_root() -> PathBuf {
    std::env::var_os("KAFKA_BENCH_REPO_ROOT").map_or_else(
        || std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        PathBuf::from,
    )
}

/// Extracts a readable reason from a caught panic payload.
fn panic_reason(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "the control plane panicked with an unprintable payload".to_owned()
    }
}
