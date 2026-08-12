//! `benchctl pack`: one cadence's scenarios, run in the order a reviewed
//! manifest states.
//!
//! # What a pack adds, and what it deliberately does not
//!
//! A pack is a list. It contributes no measurement, no statistics, and no
//! aggregation of its own: every entry is dispatched to the verb that already
//! knows how to run it, and the evidence that lands on disk is byte-for-byte
//! the evidence those verbs would have written if a person had typed them one
//! at a time. That is the whole design constraint. A runner that summarized
//! *across* entries would be inventing a cross-scenario statistic nobody
//! specified, and a runner that reordered or retried them would make "what the
//! nightly ran" a question only the runner can answer.
//!
//! What it does contribute is that the list is reviewed. `scenarios/packs/`
//! states which scenarios belong to which cadence; this verb is the thing that
//! cannot silently disagree with that file.
//!
//! # The dispatch rule
//!
//! Each entry names a scenario and how many attempts of it the pack asks for.
//! Three cases, decided by [`EntryPlan::decide`] and nothing else:
//!
//! ```text
//! repetitions >= 2                      -> benchctl suite    --repetitions N
//! repetitions == 1 and [search] present -> benchctl capacity
//! repetitions == 1                      -> benchctl run
//! ```
//!
//! The middle case is the only one that reads the scenario, and it reads it for
//! one fact: a `[search]` section is what distinguishes "run this once" from
//! "walk a rate ladder". A capacity search is already repeated internally —
//! `repetitions_per_rate` confirmations at the surviving rate — so asking a
//! pack to repeat one would be asking for a suite over documents that are not
//! attempts of the same experiment. That is why a search entry states one
//! repetition and gets a ladder rather than a loop.
//!
//! Scenario paths are used exactly as the manifest writes them, which the pack
//! files document as repository-root-relative. `benchctl pack` therefore runs
//! from the repository root, and a manifest that moved would not silently
//! resolve against its own directory.
//!
//! # Exit codes
//!
//! `0` only when every entry exited `0`; `20` when any entry did not. The
//! per-entry codes are the ones those verbs already define — a suite's `20` is
//! still "the bundles exist and say why" — and the pack neither widens nor
//! narrows them, it only reports that at least one entry did not end cleanly.
//!
//! A malformed manifest is a different axis: it is refused with `65` before any
//! attempt runs, because a pack whose entries cannot be read has not measured
//! anything to report. A single unreadable *scenario* does not stop the pack:
//! that entry is recorded as failed and the remaining entries still run, so one
//! broken file in a nightly does not cost the evidence from the other five.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use bench_schema::{BudgetSpec, SourceExperiment};
use serde::{Deserialize, Serialize};

use crate::capacity::CapacityCommand;
use crate::error::{CtlError, CtlResult, EXIT_SEALED_COMPLETE, EXIT_SEALED_PARTIAL};
use crate::pipeline::{self, CommonArguments};
use crate::suite::{MINIMUM_REPETITIONS, SuiteCommand};
use crate::{capacity, suite};

/// One entry: which scenario, and how many attempts of it the pack asks for.
///
/// Unknown fields are refused for the same reason every other human-authored
/// document in this repository refuses them: a mistyped key in a reviewed
/// manifest is a scenario somebody believes is in the cadence and that nothing
/// is running.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackEntry {
    /// Path to a scenario TOML, as the manifest writes it.
    pub scenario: String,
    /// How many attempts of that scenario the pack asks for.
    pub repetitions: u32,
}

/// A reviewed statement of which scenarios belong to one cadence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackManifest {
    /// Identifier used in logs and report titles.
    pub name: String,
    /// Which schedule this pack belongs to; prose, not a trigger.
    pub cadence: String,
    /// What a reader should conclude from a complete run of the pack.
    pub description: String,
    /// The entries, in the order they run.
    ///
    /// Defaulted rather than required so that an absent list and an empty one
    /// reach the same explanation in [`PackManifest::validate`]; serde's
    /// "missing field" would be true and unhelpful.
    #[serde(default)]
    pub entries: Vec<PackEntry>,
}

impl PackManifest {
    /// Parses a manifest from its TOML text.
    ///
    /// # Errors
    ///
    /// Returns an invalid-experiment error when the text is not the manifest
    /// shape, or when an entry could not describe work to do.
    pub fn from_toml_str(text: &str) -> CtlResult<Self> {
        let manifest: Self = toml::from_str(text)
            .map_err(|error| CtlError::invalid(format!("pack manifest: {error}")))?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Checks the invariants a manifest can check on its own.
    ///
    /// # Errors
    ///
    /// Returns an error naming the first violated invariant.
    pub fn validate(&self) -> CtlResult<()> {
        if self.entries.is_empty() {
            return Err(CtlError::invalid(
                "a pack with no entries declares a cadence that runs nothing",
            ));
        }
        for (index, entry) in self.entries.iter().enumerate() {
            if entry.scenario.trim().is_empty() {
                return Err(CtlError::invalid(format!(
                    "entries[{index}].scenario is empty, so the entry names nothing to run"
                )));
            }
            if entry.repetitions == 0 {
                return Err(CtlError::invalid(format!(
                    "entries[{index}] asks for zero repetitions of {:?}, which is a way of \
                     writing down a scenario without running it",
                    entry.scenario
                )));
            }
        }
        Ok(())
    }
}

/// Which verb one entry dispatches to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryPlan {
    /// One attempt: `benchctl run`.
    Run,
    /// N attempts of one experiment: `benchctl suite`.
    Suite,
    /// A rate ladder: `benchctl capacity`.
    Capacity,
}

impl EntryPlan {
    /// Decides what an entry runs, from its repetition count and whether its
    /// scenario carries a `[search]` section.
    ///
    /// A pure function of two facts, so the plan for a whole pack can be read
    /// off before a single attempt runs.
    #[must_use]
    pub const fn decide(repetitions: u32, has_search: bool) -> Self {
        if repetitions >= MINIMUM_REPETITIONS {
            Self::Suite
        } else if has_search {
            Self::Capacity
        } else {
            Self::Run
        }
    }

    /// The verb name this plan prints as.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Suite => "suite",
            Self::Capacity => "capacity",
        }
    }
}

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

/// Reads the scenario far enough to decide which verb the entry dispatches to.
///
/// # Errors
///
/// Returns an invalid-experiment error when the scenario cannot be read or does
/// not parse. Only a `repetitions == 1` entry needs the file at all, but it is
/// read for every entry: a suite would fail on the same unreadable scenario a
/// moment later, and failing here means the pack says which entry was broken
/// before it creates a workspace for it.
fn plan_for(scenario: &str, repetitions: u32) -> CtlResult<EntryPlan> {
    let source = SourceExperiment::from_toml_str(&pipeline::read(Path::new(scenario))?)?;
    Ok(EntryPlan::decide(repetitions, source.search.is_some()))
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

/// Renders the closing table: one row per entry, in the order they ran.
#[must_use]
pub fn render_table(manifest: &PackManifest, outcomes: &[EntryOutcome]) -> String {
    // Writing into a `String` cannot fail, so the results are discarded rather
    // than turning a report into a fallible operation.
    let mut text = String::new();
    let failed = outcomes
        .iter()
        .filter(|outcome| outcome.exit_code != 0)
        .count();
    let _ = writeln!(text, "\n## Pack {} ({})\n", manifest.name, manifest.cadence);
    let _ = writeln!(text, "| # | scenario | verb | repetitions | exit |");
    let _ = writeln!(text, "| ---: | --- | --- | ---: | ---: |");
    for (index, outcome) in outcomes.iter().enumerate() {
        let _ = writeln!(
            text,
            "| {} | `{}` | {} | {} | {} |",
            index + 1,
            outcome.scenario,
            outcome.plan.map_or("—", EntryPlan::as_str),
            outcome.repetitions,
            outcome.exit_code
        );
    }
    let _ = writeln!(
        text,
        "\n{} of {} entries exited 0.",
        outcomes.len() - failed,
        outcomes.len()
    );
    text
}
