//! `benchctl suite`: N full attempts of one experiment, then one summary over
//! the bundles they sealed.
//!
//! # Why repetitions are attempts, not phases
//!
//! A repetition here is an entire `benchctl run`: fresh attempt id, fresh
//! probes, fresh topics, fresh resolution, its own sealed bundle. Nothing is
//! reused between repetitions except the three input documents, because
//! everything that *is* reused is a way for the second measurement to inherit
//! the first one's state. The run ids differ by construction — a run id is the
//! digest of the experiment id and the attempt id — so no two repetitions can
//! write records the verifier could confuse.
//!
//! The experiment id is identical across repetitions, because the runtime
//! binding is excluded from it. That is what makes the summary legitimate: every
//! bundle it reads is an attempt at the same intent.
//!
//! # The execution-order rule, and why it has no seed
//!
//! Subjects run one after another, so the one that runs first meets a colder
//! page cache, a colder JIT, and an emptier broker. Over repetitions that
//! position has to be shared out, and the sharing has to be reproducible from
//! the repetition index alone — a seeded shuffle would make "which subject led
//! repetition 3" a question only the seed can answer.
//!
//! Repetition `i` over `n` subjects therefore uses:
//!
//! ```text
//! order(i) = rotate_left(subjects, i mod n), reversed when (i / n) is odd
//! ```
//!
//! Each block of `n` repetitions gives every subject the lead exactly once, so
//! across `N` repetitions each subject leads either `floor(N / n)` or
//! `ceil(N / n)` times — balanced within one, always, with no seed involved. The
//! reversal on alternate blocks additionally balances *adjacency*: a subject that
//! always ran immediately after another one would inherit its tail effects.
//!
//! An explicit `--order` does not disable the rotation; it chooses the base list
//! the rotation is applied to.
//!
//! # Exit codes
//!
//! `0` when every attempt sealed `complete` **and** at least two attempts
//! classified as valid — the fewest a paired comparison can be computed from.
//! `20` otherwise: the bundles exist and say why, which is the same contract
//! `benchctl run` has for a partial attempt. A failure to *write* the reports is
//! a different axis and exits `70`, because that is a broken control plane
//! rather than a disappointing measurement.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use bench_report::{DEFAULT_BOOTSTRAP_RESAMPLES, DEFAULT_PRACTICAL_THRESHOLD, SuiteOptions};
use bench_schema::{BudgetSpec, ExecutionStatus, SuiteSummary, pretty_bytes};

use crate::error::{CtlError, CtlResult, EXIT_INTERNAL, EXIT_SEALED_COMPLETE, EXIT_SEALED_PARTIAL};
use crate::pipeline::{self, AttemptEnd, CommonArguments, SealedAttempt};
use crate::time::utc_compact_seconds;

/// Default directory the report tree is created under.
pub const DEFAULT_REPORTS_ROOT: &str = "reports";

/// Fewest repetitions a suite may be asked for.
///
/// One repetition is a run, and calling it a suite would put a dispersion
/// column with nothing in it next to a single measurement.
pub const MINIMUM_REPETITIONS: u32 = 2;

/// Fewest valid attempts a suite needs before it exits successfully.
pub const MINIMUM_VALID_ATTEMPTS: usize = 2;

/// Directory name suffix distinguishing a suite's reports from a capacity
/// search's.
pub const SUITE_REPORT_SUFFIX: &str = "suite";

/// Directory name used when no attempt got far enough to have an experiment id.
pub const UNRESOLVED_REPORT_DIR: &str = "unresolved";

/// `benchctl suite`: repeat one experiment and summarize the repetitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuiteCommand {
    /// Shared inputs.
    pub common: CommonArguments,
    /// Root of the evidence tree.
    pub results_root: PathBuf,
    /// Ceilings each attempt declares for itself.
    pub budget: BudgetSpec,
    /// How many attempts to make; at least [`MINIMUM_REPETITIONS`].
    pub repetitions: u32,
    /// Root of the report tree.
    pub reports_root: PathBuf,
}

/// Returns the subject order repetition `repetition` runs in.
///
/// The rule is the one the module contract states: rotate the base list left by
/// `repetition mod n`, and reverse it when `repetition / n` is odd. Both halves
/// are pure functions of the index, so the plan for a suite can be read off
/// before a single attempt runs.
#[must_use]
pub fn repetition_order(base: &[String], repetition: u32) -> Vec<String> {
    let count = base.len();
    if count == 0 {
        return Vec::new();
    }
    let index = repetition as usize;
    let rotation = index % count;
    let mut order: Vec<String> = base[rotation..]
        .iter()
        .chain(&base[..rotation])
        .cloned()
        .collect();
    if (index / count) % 2 == 1 {
        order.reverse();
    }
    order
}

/// Runs the suite and returns the process exit code.
#[must_use]
pub fn execute(command: &SuiteCommand) -> i32 {
    let inputs = match pipeline::load(&command.common) {
        Ok(inputs) => inputs,
        Err(error) => {
            eprintln!("benchctl: {error}");
            return error.exit_code();
        }
    };
    let base: Vec<String> = command.common.order.clone().unwrap_or_else(|| {
        inputs
            .subjects
            .subjects
            .iter()
            .map(|entry| entry.name.clone())
            .collect()
    });

    let mut sealed = Vec::with_capacity(command.repetitions as usize);
    let mut unsealable: Option<CtlError> = None;
    for repetition in 0..command.repetitions {
        let order = repetition_order(&base, repetition);
        println!(
            "==> repetition {} of {}: {}",
            repetition + 1,
            command.repetitions,
            order.join(", ")
        );
        let common = CommonArguments {
            order: Some(order),
            ..command.common.clone()
        };
        match pipeline::attempt(&inputs, &common, &command.results_root, command.budget) {
            AttemptEnd::Sealed(attempt) => sealed.push(attempt),
            AttemptEnd::Unsealable(error) => {
                unsealable = Some(error);
                break;
            }
        }
    }

    if let Some(error) = unsealable {
        eprintln!("benchctl: the suite stopped: {error}");
        if sealed.is_empty() {
            return error.exit_code();
        }
    }
    let seed = command.common.seed.unwrap_or(inputs.source.payload.seed);
    match summarize(command, &sealed, seed) {
        Ok(()) => suite_exit_code(&sealed),
        Err(error) => {
            eprintln!("benchctl: {error}");
            EXIT_INTERNAL
        }
    }
}

/// The suite's exit code: success only when every attempt completed and enough
/// of them are believable.
fn suite_exit_code(sealed: &[SealedAttempt]) -> i32 {
    let all_complete = sealed
        .iter()
        .all(|attempt| attempt.status == ExecutionStatus::Complete);
    let valid = sealed.iter().filter(|attempt| attempt.run_valid()).count();
    if all_complete && valid >= MINIMUM_VALID_ATTEMPTS {
        EXIT_SEALED_COMPLETE
    } else {
        EXIT_SEALED_PARTIAL
    }
}

/// Summarizes the sealed bundles and writes the report set.
///
/// Invalid attempts are handed to the summary along with the valid ones: the
/// reporting layer excludes them from its medians and says so in the document,
/// which is a fact a reader needs. A suite that filtered them here would report
/// a median over a set nobody can reconstruct.
fn summarize(command: &SuiteCommand, sealed: &[SealedAttempt], seed: u64) -> CtlResult<()> {
    if sealed.is_empty() {
        return Err(CtlError::internal(
            "the suite sealed no attempts, so there is nothing to summarize",
        ));
    }
    let roots: Vec<PathBuf> = sealed
        .iter()
        .map(|attempt| attempt.paths.root().to_path_buf())
        .collect();
    // The thresholds are the reporting layer's, not this crate's: a control
    // plane that chose its own resample count or its own idea of a difference
    // worth reporting would be deciding statistics from the process-spawning
    // side of the wall.
    let options = SuiteOptions {
        seed,
        resamples: DEFAULT_BOOTSTRAP_RESAMPLES,
        practical_threshold: DEFAULT_PRACTICAL_THRESHOLD,
    };
    let summary = bench_report::summarize_suite(&roots, &options)
        .map_err(|error| CtlError::internal(format!("summarize the suite: {error}")))?;
    let directory = report_directory(command, sealed)?;
    write_report_set(&directory, &summary)
}

/// Creates `<reports>/<experiment-short>/<utc-compact>-suite/`.
fn report_directory(command: &SuiteCommand, sealed: &[SealedAttempt]) -> CtlResult<PathBuf> {
    let short = sealed
        .iter()
        .find_map(|attempt| attempt.experiment_id.as_ref())
        .map_or_else(
            || UNRESOLVED_REPORT_DIR.to_owned(),
            |identity| identity.short().to_owned(),
        );
    let directory = command.reports_root.join(short).join(format!(
        "{}-{SUITE_REPORT_SUFFIX}",
        utc_compact_seconds(SystemTime::now())
    ));
    std::fs::create_dir_all(&directory)
        .map_err(|error| CtlError::internal(format!("create {}: {error}", directory.display())))?;
    Ok(directory)
}

/// Writes the four documents a suite produces and prints where they went.
fn write_report_set(directory: &Path, summary: &SuiteSummary) -> CtlResult<()> {
    let packet = bench_report::build_packet(summary);
    for (name, bytes) in [
        ("suite-summary.json", pretty_bytes(summary)?),
        (
            "report.md",
            bench_report::render_markdown_suite(summary).into_bytes(),
        ),
        (
            "report.html",
            bench_report::render_html_suite(summary).into_bytes(),
        ),
        ("analysis-packet.json", pretty_bytes(&packet)?),
    ] {
        let path = directory.join(name);
        std::fs::write(&path, &bytes)
            .map_err(|error| CtlError::internal(format!("write {}: {error}", path.display())))?;
        println!("{}", path.display());
    }
    Ok(())
}
