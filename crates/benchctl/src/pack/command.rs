//! The loop: read the manifest, run every entry in the order it states, and
//! report how each one ended.

use std::path::{Path, PathBuf};

use bench_schema::BudgetSpec;

use crate::capacity::{self, CapacityCommand};
use crate::error::{CtlResult, EXIT_SEALED_COMPLETE, EXIT_SEALED_PARTIAL};
use crate::pipeline::{self, CommonArguments};
use crate::suite::{self, SuiteCommand};

use super::manifest::{PackEntry, PackManifest};
use super::plan::{EntryPlan, plan_for};
use super::report::render_table;

/// `benchctl pack`: run every entry of one manifest, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackCommand {
    /// Path to the pack manifest TOML.
    pub manifest: PathBuf,
    /// Path to the subject list TOML, shared by every entry.
    pub subjects: PathBuf,
    /// Path to the cluster profile TOML, shared by every entry.
    pub cluster: PathBuf,
    /// Bootstrap servers every attempt binds to.
    pub bootstrap: String,
    /// Root of the evidence tree.
    pub results_root: PathBuf,
    /// Root of the report tree.
    pub reports_root: PathBuf,
    /// Ceilings each attempt declares for itself.
    pub budget: BudgetSpec,
    /// Seed override; `None` keeps each scenario's payload seed.
    pub seed: Option<u64>,
}

/// One entry's outcome, as the final table reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryOutcome {
    /// The scenario path the manifest wrote.
    pub scenario: String,
    /// How many attempts the entry asked for.
    pub repetitions: u32,
    /// Which verb it dispatched to, when the scenario could be read at all.
    pub plan: Option<EntryPlan>,
    /// The exit code that verb returned.
    pub exit_code: i32,
}

/// Runs the pack and returns the process exit code.
#[must_use]
pub fn execute(command: &PackCommand) -> i32 {
    let manifest = match read_manifest(&command.manifest) {
        Ok(manifest) => manifest,
        Err(error) => {
            eprintln!("benchctl: {error}");
            return error.exit_code();
        }
    };
    println!(
        "==> pack {} ({}): {} {}",
        manifest.name,
        manifest.cadence,
        manifest.entries.len(),
        if manifest.entries.len() == 1 {
            "entry"
        } else {
            "entries"
        }
    );

    let total = manifest.entries.len();
    let mut outcomes = Vec::with_capacity(total);
    for (index, entry) in manifest.entries.iter().enumerate() {
        outcomes.push(run_entry(command, entry, index + 1, total));
    }
    print!("{}", render_table(&manifest, &outcomes));
    if outcomes.iter().all(|outcome| outcome.exit_code == 0) {
        EXIT_SEALED_COMPLETE
    } else {
        EXIT_SEALED_PARTIAL
    }
}

/// Reads and parses a manifest, naming the file in the error.
fn read_manifest(path: &Path) -> CtlResult<PackManifest> {
    PackManifest::from_toml_str(&pipeline::read(path)?)
}

/// Runs one entry and reports how it ended.
///
/// A scenario that cannot be read is an entry-level failure, not a pack-level
/// one: it is announced, recorded with the invalid-input exit code, and the
/// pack moves on to the next entry.
fn run_entry(
    command: &PackCommand,
    entry: &PackEntry,
    position: usize,
    total: usize,
) -> EntryOutcome {
    let plan = match plan_for(&entry.scenario, entry.repetitions) {
        Ok(plan) => plan,
        Err(error) => {
            eprintln!("benchctl: entry {position} of {total}: {error}");
            println!(
                "<-- entry {position} of {total}: {} could not be planned, exit {}",
                entry.scenario,
                error.exit_code()
            );
            return EntryOutcome {
                scenario: entry.scenario.clone(),
                repetitions: entry.repetitions,
                plan: None,
                exit_code: error.exit_code(),
            };
        }
    };
    println!(
        "==> entry {position} of {total}: {} via {} ({} {})",
        entry.scenario,
        plan.as_str(),
        entry.repetitions,
        if entry.repetitions == 1 {
            "repetition"
        } else {
            "repetitions"
        }
    );
    let common = CommonArguments {
        experiment: PathBuf::from(&entry.scenario),
        subjects: command.subjects.clone(),
        cluster: command.cluster.clone(),
        bootstrap: command.bootstrap.clone(),
        seed: command.seed,
        order: None,
    };
    let exit_code = match plan {
        EntryPlan::Run => run_once(command, &common),
        EntryPlan::Suite => suite::execute(&SuiteCommand {
            common,
            results_root: command.results_root.clone(),
            budget: command.budget,
            repetitions: entry.repetitions,
            reports_root: command.reports_root.clone(),
        }),
        EntryPlan::Capacity => capacity::execute(&CapacityCommand {
            common,
            results_root: command.results_root.clone(),
            budget: command.budget,
            reports_root: command.reports_root.clone(),
        }),
    };
    println!(
        "<-- entry {position} of {total}: {} exited {exit_code}",
        entry.scenario
    );
    EntryOutcome {
        scenario: entry.scenario.clone(),
        repetitions: entry.repetitions,
        plan: Some(plan),
        exit_code,
    }
}

/// Runs a single attempt, the way `benchctl run` does.
fn run_once(command: &PackCommand, common: &CommonArguments) -> i32 {
    let inputs = match pipeline::load(common) {
        Ok(inputs) => inputs,
        Err(error) => {
            eprintln!("benchctl: {error}");
            return error.exit_code();
        }
    };
    pipeline::attempt(&inputs, common, &command.results_root, command.budget).exit_code()
}
