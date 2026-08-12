//! `benchctl capacity`: the highest offered rate at which a subject still meets
//! its objectives, found by probing.
//!
//! # Every probe is an ordinary attempt
//!
//! A probe is a full `benchctl run` at one offered rate: fresh attempt id, fresh
//! topics, its own sealed bundle, its own classification. The search never
//! invents a measurement and never reuses one; the only thing it does that `run`
//! does not is decide which rate to ask for next. That means a capacity search
//! leaves behind exactly the evidence a reader would have collected by hand, and
//! [`CapacitySearch`] records the digest of every bundle so the ladder can be
//! re-walked.
//!
//! The rate override is applied to the *scenario text*, and the patched text is
//! what each probe seals as `experiment.source.toml`. That keeps the sealed
//! input and the sealed resolved experiment in agreement: a reader who re-runs
//! the sealed scenario gets the rate the probe actually offered. Each probe
//! therefore has its own experiment id, which is correct — a different offered
//! rate is a different experiment.
//!
//! # What the search requires
//!
//! A scenario whose load mode resolves to `scheduled-open-loop-fixed-rate`,
//! carrying a `[search]` section and a non-empty `[slo]`. The load mode matters
//! because a closed-loop scenario has no offered rate to bisect on. The
//! objectives matter more: with none declared, every rate satisfies, and the
//! search would climb to its ceiling and report a capacity nobody measured.
//! Both are refused before any topic is created.
//!
//! # The ladder
//!
//! 1. **Bracket.** Probe the initial rate. If it satisfies, multiply by the
//!    growth factor until one does not, or until the configured maximum is
//!    reached. If it does not, divide by the growth factor until one does, or
//!    until the configured minimum is reached.
//! 2. **Bisect.** With a satisfied `low` and an unsatisfied `high`, halve the
//!    bracket until it is narrower than the resolution.
//! 3. **Confirm.** Repeat the surviving rate `repetitions_per_rate` times. A
//!    confirmation that misses turns a converged search into an unconverged one,
//!    because a rate that holds once and fails twice is not a capacity.
//!
//! [`MAX_PROBES`] bounds the whole thing. A pathological objective — one no rate
//! can satisfy, or one every rate can — must end the process, not occupy a
//! cluster indefinitely.
//!
//! # Which subject's capacity
//!
//! Every subject's result is evaluated and every subject's reasons are recorded,
//! but one subject decides. When the subject list names exactly one subject, that
//! is the target. Otherwise the target is the **first subject in each probe's
//! execution order** — the same rule the comparison document uses to pick a
//! baseline, so "capacity" and "baseline" cannot mean two different subjects in
//! one evidence tree.
//!
//! # What "satisfied" means
//!
//! Two gates, both required. The probe's sealed `classification.json` has to say
//! the attempt is believable evidence, *and* the target subject's measurement has
//! to meet the declared objectives. The first gate is not implied by the second:
//! an adapter can exit non-zero, or a read-back verifier can find records
//! missing, while the result document that same attempt wrote reports latencies
//! comfortably inside every ceiling. Bisecting on the objectives alone would
//! climb a ladder built out of runs the bundle itself refuses to vouch for.
//!
//! # Exit codes
//!
//! `0` for a converged search, `20` for an unconverged one. An unconverged
//! search is a legitimate sealed outcome, not a crash: the probes are on disk and
//! the document says where the ladder stopped.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use bench_schema::{
    BudgetSpec, CapacityProbe, CapacitySearch, CapacityStatus, SloSpec, SourceExperiment,
    SourceSearch, pretty_bytes,
};

use crate::attempt::AttemptPaths;
use crate::error::{CtlError, CtlResult, EXIT_SEALED_COMPLETE, EXIT_SEALED_PARTIAL};
use crate::pipeline::{self, AttemptEnd, CommonArguments, LoadedInputs, SealedAttempt};
use crate::results;
use crate::suite::UNRESOLVED_REPORT_DIR;
use crate::time::utc_compact_seconds;

/// Hard ceiling on how many attempts one search may make, confirmations
/// included.
pub const MAX_PROBES: usize = 24;

/// Growth factor used when the scenario states one below two.
///
/// A factor of one never leaves the initial rate, and a factor of zero is not a
/// search at all; doubling is what the migrated legacy scenarios use.
pub const DEFAULT_GROWTH_FACTOR: u64 = 2;

/// Bracket width, as a percentage of the bracket's upper end, used when the
/// scenario states zero.
pub const DEFAULT_RESOLUTION_PERCENT: u64 = 5;

/// Confirmation repetitions used when the scenario states none.
///
/// One: the search has already probed this rate once during the bisection, and a
/// scenario that wants more says so in `repetitions_per_rate`.
pub const DEFAULT_CONFIRMATIONS: u32 = 1;

/// Directory name suffix distinguishing a capacity search's reports.
pub const CAPACITY_REPORT_SUFFIX: &str = "capacity";

/// `benchctl capacity`: search for the highest satisfying offered rate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapacityCommand {
    /// Shared inputs.
    pub common: CommonArguments,
    /// Root of the evidence tree.
    pub results_root: PathBuf,
    /// Ceilings each probe declares for itself.
    pub budget: BudgetSpec,
    /// Root of the report tree.
    pub reports_root: PathBuf,
}

/// The search bounds, with every degenerate value replaced by a documented
/// default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchBounds {
    /// First rate the search offers.
    pub initial: u64,
    /// Lowest rate the search will consider.
    pub minimum: u64,
    /// Highest rate the search will consider.
    pub maximum: u64,
    /// Multiplier applied while the search is still bracketing.
    pub growth_factor: u64,
    /// Bracket width, as a percentage of the bracket's upper end.
    pub resolution_percent: u64,
    /// Repetitions at the surviving rate, after the bracket closes.
    pub confirmations: u32,
}

impl SearchBounds {
    /// Reads the bounds a scenario declares, substituting defaults.
    ///
    /// # Errors
    ///
    /// Returns an invalid-experiment error when the bounds cannot describe a
    /// search: no `[search]` section, or a minimum above the maximum.
    pub fn from_source(source: &SourceExperiment) -> CtlResult<Self> {
        let search: &SourceSearch = source.search.as_ref().ok_or_else(|| {
            CtlError::invalid(
                "a capacity search needs a [search] section stating where to start and stop",
            )
        })?;
        if search.minimum_records_per_second > search.maximum_records_per_second {
            return Err(CtlError::invalid(format!(
                "the search floor {} is above its ceiling {}",
                search.minimum_records_per_second, search.maximum_records_per_second
            )));
        }
        let bounds = Self {
            initial: search.initial_records_per_second.clamp(
                search.minimum_records_per_second.max(1),
                search.maximum_records_per_second.max(1),
            ),
            minimum: search.minimum_records_per_second.max(1),
            maximum: search.maximum_records_per_second.max(1),
            growth_factor: if search.growth_factor < 2 {
                DEFAULT_GROWTH_FACTOR
            } else {
                search.growth_factor
            },
            resolution_percent: if search.resolution_percent == 0 {
                DEFAULT_RESOLUTION_PERCENT
            } else {
                search.resolution_percent
            },
            confirmations: source.repetitions_per_rate.unwrap_or(DEFAULT_CONFIRMATIONS),
        };
        Ok(bounds)
    }

    /// The absolute bracket width the search narrows to, given the upper end it
    /// found.
    ///
    /// The document carries one number, so the percentage is resolved against
    /// the bracket's upper end the moment one exists, and against the initial
    /// rate when the search never bracketed. Never zero: a bracket that narrows
    /// to nothing never closes.
    #[must_use]
    pub fn resolution(&self, upper: Option<u64>) -> u64 {
        let scale = upper.unwrap_or(self.initial);
        (scale.saturating_mul(self.resolution_percent) / 100).max(1)
    }
}

/// One probe's verdict, plus the bundle it came from.
#[derive(Debug, Clone)]
struct ProbeOutcome {
    /// The document entry this probe contributes.
    record: CapacityProbe,
    /// Whether the target subject's result could be read at all.
    readable: bool,
}

/// Runs the search and returns the process exit code.
#[must_use]
pub fn execute(command: &CapacityCommand) -> i32 {
    match search(command) {
        Ok(status) => match status {
            CapacityStatus::Converged => EXIT_SEALED_COMPLETE,
            CapacityStatus::Unconverged | CapacityStatus::Invalid => EXIT_SEALED_PARTIAL,
        },
        Err(error) => {
            eprintln!("benchctl: {error}");
            error.exit_code()
        }
    }
}

/// The whole search, from reading the scenario to writing the two documents.
fn search(command: &CapacityCommand) -> CtlResult<CapacityStatus> {
    let inputs = pipeline::load(&command.common)?;
    let slo = crate::resolve::slo_spec(&inputs.source);
    if slo.is_empty() {
        return Err(CtlError::invalid(
            "a capacity search needs an [slo] section: with no objective declared, \
             every rate satisfies and the search would report a ceiling nobody measured",
        ));
    }
    let bounds = SearchBounds::from_source(&inputs.source)?;
    let mut state = SearchState::new(command, &inputs, slo, bounds);
    let status = state.run()?;
    let directory = state.report_directory()?;
    state.write(&directory, status)?;
    Ok(status)
}

/// Everything one search accumulates.
struct SearchState<'a> {
    command: &'a CapacityCommand,
    inputs: &'a LoadedInputs,
    slo: SloSpec,
    bounds: SearchBounds,
    probes: Vec<CapacityProbe>,
    confirmation: Vec<CapacityProbe>,
    /// Highest rate known to satisfy the objectives.
    low: Option<u64>,
    /// Lowest rate known to violate them.
    high: Option<u64>,
    /// The first sealed attempt, which names the report directory.
    first: Option<SealedAttempt>,
    /// Why the search stopped where it did.
    notes: Vec<String>,
}

impl<'a> SearchState<'a> {
    fn new(
        command: &'a CapacityCommand,
        inputs: &'a LoadedInputs,
        slo: SloSpec,
        bounds: SearchBounds,
    ) -> Self {
        Self {
            command,
            inputs,
            slo,
            bounds,
            probes: Vec::new(),
            confirmation: Vec::new(),
            low: None,
            high: None,
            first: None,
            notes: Vec::new(),
        }
    }

    /// Walks the ladder and returns how it ended.
    fn run(&mut self) -> CtlResult<CapacityStatus> {
        if !self.bracket()? {
            return Ok(CapacityStatus::Invalid);
        }
        if !self.bisect()? {
            return Ok(CapacityStatus::Invalid);
        }
        let Some(candidate) = self.low else {
            self.notes.push(format!(
                "no rate down to the configured floor of {} met the objectives",
                self.bounds.minimum
            ));
            return Ok(CapacityStatus::Unconverged);
        };
        let Some(upper) = self.high else {
            self.notes.push(format!(
                "every rate up to the configured ceiling of {} met the objectives, \
                 so the capacity is at or above it",
                self.bounds.maximum
            ));
            return Ok(CapacityStatus::Unconverged);
        };
        if upper - candidate > self.bounds.resolution(Some(upper)) {
            self.notes.push(format!(
                "the search ran out of probes with the bracket still [{candidate}, {upper}]"
            ));
            return Ok(CapacityStatus::Unconverged);
        }
        self.confirm(candidate)
    }

    /// Phase one: find a satisfied rate below an unsatisfied one.
    ///
    /// Returns false when a probe could not be read, which makes the whole
    /// search invalid rather than merely unconverged.
    fn bracket(&mut self) -> CtlResult<bool> {
        let mut rate = self.bounds.initial;
        let first = self.probe(rate)?;
        if !first.readable {
            return Ok(false);
        }
        let growing = first.record.satisfied;
        if growing {
            self.low = Some(rate);
        } else {
            self.high = Some(rate);
        }
        while self.probes.len() < MAX_PROBES {
            let next = if growing {
                let grown = rate.saturating_mul(self.bounds.growth_factor);
                grown.min(self.bounds.maximum)
            } else {
                (rate / self.bounds.growth_factor).max(self.bounds.minimum)
            };
            if next == rate {
                // The ladder has reached a configured bound and cannot move.
                break;
            }
            rate = next;
            let outcome = self.probe(rate)?;
            if !outcome.readable {
                return Ok(false);
            }
            // The direction is fixed by the first probe, so the first change of
            // answer is the bracket and the loop is done.
            if outcome.record.satisfied {
                self.low = Some(rate);
                if !growing {
                    break;
                }
            } else {
                self.high = Some(rate);
                if growing {
                    break;
                }
            }
        }
        Ok(true)
    }

    /// Phase two: halve the bracket until it is inside the resolution.
    fn bisect(&mut self) -> CtlResult<bool> {
        while let (Some(low), Some(high)) = (self.low, self.high) {
            if high - low <= self.bounds.resolution(Some(high)) || self.probes.len() >= MAX_PROBES {
                break;
            }
            let middle = low + (high - low) / 2;
            if middle == low || middle == high {
                break;
            }
            let outcome = self.probe(middle)?;
            if !outcome.readable {
                return Ok(false);
            }
            if outcome.record.satisfied {
                self.low = Some(middle);
            } else {
                self.high = Some(middle);
            }
        }
        Ok(true)
    }

    /// Phase three: repeat the surviving rate and require every repetition.
    fn confirm(&mut self, candidate: u64) -> CtlResult<CapacityStatus> {
        for repetition in 0..self.bounds.confirmations {
            if self.probes.len() + self.confirmation.len() >= MAX_PROBES {
                self.notes.push(format!(
                    "the probe budget of {MAX_PROBES} ran out after {repetition} confirmations"
                ));
                return Ok(CapacityStatus::Unconverged);
            }
            let outcome = self.run_probe(candidate)?;
            let satisfied = outcome.record.satisfied && outcome.readable;
            self.confirmation.push(outcome.record);
            if !satisfied {
                self.notes.push(format!(
                    "confirmation {} of {} did not hold at {candidate}",
                    repetition + 1,
                    self.bounds.confirmations
                ));
                return Ok(CapacityStatus::Unconverged);
            }
        }
        Ok(CapacityStatus::Converged)
    }

    /// Runs one probe and records it in the ladder.
    fn probe(&mut self, rate: u64) -> CtlResult<ProbeOutcome> {
        let outcome = self.run_probe(rate)?;
        self.probes.push(outcome.record.clone());
        Ok(outcome)
    }

    /// Runs one probe at `rate` without recording it.
    ///
    /// # Errors
    ///
    /// Returns an error only when nothing could be sealed at all; a probe that
    /// sealed a failure is a fact about that rate, and travels back as an
    /// unsatisfied, unreadable outcome.
    fn run_probe(&mut self, rate: u64) -> CtlResult<ProbeOutcome> {
        println!("==> probing {rate} records/s");
        let inputs = probe_inputs(self.inputs, rate)?;
        let end = pipeline::attempt(
            &inputs,
            &self.command.common,
            &self.command.results_root,
            self.command.budget,
        );
        let AttemptEnd::Sealed(sealed) = end else {
            return Err(match end {
                AttemptEnd::Unsealable(error) => error,
                AttemptEnd::Sealed(_) => CtlError::internal("unreachable sealed attempt"),
            });
        };
        if self.first.is_none() {
            self.first = Some(sealed.clone());
        }
        Ok(evaluate(
            &sealed,
            &self.subject_names(&sealed),
            &self.target(&sealed),
            rate,
            &self.slo,
        ))
    }

    /// The subjects a probe ran, preferring what the bundle says it ran.
    ///
    /// A bundle that sealed before its execution order was written still has a
    /// subject list — the one the operator asked for — and judging against that
    /// produces "no readable measurement" rather than a silently empty verdict.
    fn subject_names(&self, sealed: &SealedAttempt) -> Vec<String> {
        let sealed_order = sealed_execution_order(&sealed.paths);
        if sealed_order.is_empty() {
            self.inputs
                .subjects
                .subjects
                .iter()
                .map(|entry| entry.name.clone())
                .collect()
        } else {
            sealed_order
        }
    }

    /// The subject whose verdict decides this search.
    fn target(&self, sealed: &SealedAttempt) -> String {
        if self.inputs.subjects.subjects.len() == 1 {
            return self.inputs.subjects.subjects[0].name.clone();
        }
        self.subject_names(sealed)
            .first()
            .cloned()
            .unwrap_or_default()
    }

    /// Creates `<reports>/<experiment-short>/<utc-compact>-capacity/`.
    ///
    /// The directory is named by the *first* probe's experiment id, which is the
    /// scenario resolved at the search's initial rate. Every later probe has its
    /// own id, because a different offered rate is a different experiment; naming
    /// the tree after one of them would make the search's home depend on where it
    /// happened to stop.
    fn report_directory(&self) -> CtlResult<PathBuf> {
        let short = self
            .first
            .as_ref()
            .and_then(|attempt| attempt.experiment_id.as_ref())
            .map_or_else(
                || UNRESOLVED_REPORT_DIR.to_owned(),
                |identity| identity.short().to_owned(),
            );
        let directory = self.command.reports_root.join(short).join(format!(
            "{}-{CAPACITY_REPORT_SUFFIX}",
            utc_compact_seconds(SystemTime::now())
        ));
        std::fs::create_dir_all(&directory).map_err(|error| {
            CtlError::internal(format!("create {}: {error}", directory.display()))
        })?;
        Ok(directory)
    }

    /// Writes `capacity-search.json` and the small report beside it.
    fn write(&self, directory: &Path, status: CapacityStatus) -> CtlResult<()> {
        let subject = self
            .first
            .as_ref()
            .map_or_else(|| "unknown".to_owned(), |sealed| self.target(sealed));
        let document = CapacitySearch {
            schema: CapacitySearch::SCHEMA.to_owned(),
            subject,
            slo: self.slo,
            probes: self.probes.clone(),
            bracket_low: self.low,
            bracket_high: self.high,
            resolution: self.bounds.resolution(self.high),
            confirmed_rate: match status {
                CapacityStatus::Converged => self.low,
                CapacityStatus::Unconverged | CapacityStatus::Invalid => None,
            },
            confirmation: self.confirmation.clone(),
            status,
        };
        document.validate()?;
        for (name, bytes) in [
            ("capacity-search.json", pretty_bytes(&document)?),
            ("report.md", render(&document, &self.notes).into_bytes()),
        ] {
            let path = directory.join(name);
            std::fs::write(&path, &bytes).map_err(|error| {
                CtlError::internal(format!("write {}: {error}", path.display()))
            })?;
            println!("{}", path.display());
        }
        Ok(())
    }
}

/// Why a probe's own bundle says its evidence may not be believed.
///
/// A probe is a full attempt, and that attempt has already answered this
/// question: sealing wrote `classification.json`. Reading the answer back rather
/// than re-deriving it is what keeps the ladder and the bundles from disagreeing
/// — and the two questions really are different. `evaluate_slo` asks whether the
/// numbers in a result document meet an objective; the classification asks
/// whether those numbers may be believed at all. A probe whose verifier found
/// missing records, or whose adapter exited non-zero, can still carry a result
/// document with a beautiful p99, and treating that as a demonstrated capacity
/// would be reporting a rate nobody measured.
fn validity_reasons(sealed: &SealedAttempt) -> Vec<String> {
    let Some(document) = pipeline::read_classification(&sealed.paths) else {
        return vec!["run validity: the probe sealed no readable classification".to_owned()];
    };
    if document.run_valid {
        return Vec::new();
    }
    if document.reasons.is_empty() {
        return vec!["run validity: classification.json declares the run invalid".to_owned()];
    }
    document
        .reasons
        .iter()
        .map(|reason| format!("run validity: {reason}"))
        .collect()
}

/// Judges one sealed probe.
///
/// Both gates have to hold. The reasons name which one failed, in the order they
/// are asked: whether the attempt is believable at all, then whether the target
/// subject met the objectives, then what every other subject had to say.
fn evaluate(
    sealed: &SealedAttempt,
    subjects: &[String],
    target: &str,
    rate: u64,
    slo: &SloSpec,
) -> ProbeOutcome {
    let invalid = validity_reasons(sealed);
    let believable = invalid.is_empty();
    let mut reasons = Vec::new();
    let mut satisfied = false;
    let mut readable = false;
    for subject in subjects {
        let evidence = results::read_subject(&sealed.paths, subject);
        let Some(measurement) = evidence.result.as_ref() else {
            let reason = format!("{subject}: no readable measurement to judge");
            if subject == target {
                reasons.insert(0, reason);
            } else {
                reasons.push(reason);
            }
            continue;
        };
        let verdict = bench_report::evaluate_slo(measurement, slo);
        if subject == target {
            readable = true;
            satisfied = verdict.satisfied;
            // The target's reasons lead, so the document reads as the story of
            // the subject whose capacity this is.
            let mut theirs = verdict.reasons.clone();
            theirs.append(&mut reasons);
            reasons = theirs;
        } else {
            reasons.extend(
                verdict
                    .reasons
                    .iter()
                    .map(|reason| format!("{subject}: {reason}")),
            );
        }
    }
    let mut all_reasons = invalid;
    all_reasons.append(&mut reasons);
    ProbeOutcome {
        record: CapacityProbe {
            offered_records_per_second: rate,
            attempt_id: attempt_id_of(&sealed.paths),
            bundle_digest: bundle_digest(&sealed.paths).unwrap_or_default(),
            satisfied: satisfied && readable && believable,
            reasons: all_reasons,
        },
        readable,
    }
}

/// The scenario text with `offered_records_per_second` set to `rate`, parsed.
///
/// # Errors
///
/// Returns an invalid-experiment error when the patched text no longer parses,
/// which means the scenario was not the fixed-rate shape a search needs.
fn probe_inputs(inputs: &LoadedInputs, rate: u64) -> CtlResult<LoadedInputs> {
    let source_toml = with_offered_rate(&inputs.source_toml, rate);
    let source = SourceExperiment::from_toml_str(&source_toml)?;
    // The patch is line-oriented (see [`with_offered_rate`]), so it can be
    // defeated by a scenario written in a shape the rewriter does not
    // understand. Re-reading the value it was supposed to set turns that into a
    // refusal here rather than a whole ladder of probes silently offered at the
    // scenario's original rate, sealed under experiment ids that agree with the
    // wrong number.
    if source.offered_records_per_second != Some(rate) {
        return Err(CtlError::invalid(format!(
            "the scenario's offered rate could not be rewritten to {rate}: the patched text \
             parses back as {:?}. The rewriter replaces a whole `offered_records_per_second = \
             <n>` line before the first table header, so a scenario that spreads that key over \
             several lines, or declares it inside a table, cannot be searched over",
            source.offered_records_per_second
        )));
    }
    Ok(LoadedInputs {
        source_toml,
        source,
        subjects: inputs.subjects.clone(),
        cluster: inputs.cluster.clone(),
    })
}

/// Rewrites the top-level `offered_records_per_second` key of a scenario.
///
/// Only the region before the first `[table]` header is touched, because that is
/// where a scenario's top-level keys live; a key of the same name inside a table
/// would belong to that table and is none of this function's business.
///
/// # The line-oriented limitation
///
/// This works on lines, not on a parsed document, because the patched text is
/// what each probe *seals* — round-tripping the operator's scenario through a
/// TOML writer would mean the sealed input is no longer the file they wrote.
/// The price is that "before the first table header" is decided by looking for
/// a line whose first non-space character is `[`, and a line inside a
/// *multi-line value* can look exactly like that. A scenario carrying a
/// multi-line string with a bracketed line in it ends the head region early;
/// the original assignment then lands in the tail, is kept, and the new one is
/// inserted above it — inside the string, where it means nothing.
///
/// The result still parses, and still carries the original rate, which is the
/// worst possible outcome: a whole ladder of probes offering one rate while
/// every sealed document agrees they were varying it. The probe builder above
/// therefore re-parses the patched text and refuses when the rate is not the one
/// asked for, which turns that shape into a message before any topic is created.
#[must_use]
pub fn with_offered_rate(source_toml: &str, rate: u64) -> String {
    const KEY: &str = "offered_records_per_second";
    let mut head = Vec::new();
    let mut tail = Vec::new();
    let mut in_tables = false;
    for line in source_toml.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with('[') {
            in_tables = true;
        }
        if in_tables {
            tail.push(line);
        } else if !starts_with_key(trimmed, KEY) {
            head.push(line);
        }
    }
    head.push("# rewritten by benchctl capacity for this probe");
    let assignment = format!("{KEY} = {rate}");
    head.push(&assignment);
    let mut lines = head;
    lines.extend(tail);
    let mut text = lines.join("\n");
    text.push('\n');
    text
}

/// Reports whether a line assigns `key` at the top level.
fn starts_with_key(line: &str, key: &str) -> bool {
    line.strip_prefix(key)
        .is_some_and(|rest| rest.trim_start().starts_with('='))
}

/// The subject names a sealed bundle says it ran, in execution order.
fn sealed_execution_order(paths: &AttemptPaths) -> Vec<String> {
    let Ok(bytes) = std::fs::read(paths.execution_order_json()) else {
        return Vec::new();
    };
    bench_schema::parse_json_slice::<bench_schema::ExecutionOrder>(&bytes)
        .map(|document| document.order)
        .unwrap_or_default()
}

/// The attempt id, taken from the directory the bundle lives in.
fn attempt_id_of(paths: &AttemptPaths) -> String {
    paths.root().file_name().map_or_else(
        || "unknown".to_owned(),
        |name| name.to_string_lossy().into_owned(),
    )
}

/// The sealed bundle's digest, when the manifest is readable.
fn bundle_digest(paths: &AttemptPaths) -> Option<String> {
    let bytes = std::fs::read(paths.bundle_json()).ok()?;
    bench_schema::parse_json_slice::<bench_schema::BundleManifest>(&bytes)
        .ok()
        .map(|manifest| manifest.bundle_digest)
}

/// Renders the small markdown report that sits beside the document.
///
/// Deliberately small: the ladder is the story, and the document beside it holds
/// every number this restates.
fn render(document: &CapacitySearch, notes: &[String]) -> String {
    // Writing into a `String` cannot fail, so the results are discarded rather
    // than turning a report into a fallible operation.
    let mut text = String::new();
    let _ = writeln!(text, "# Capacity search\n");
    let _ = writeln!(text, "- subject: `{}`", document.subject);
    let _ = writeln!(text, "- status: **{}**", document.status.as_str());
    let _ = writeln!(
        text,
        "- confirmed rate: {}",
        document
            .confirmed_rate
            .map_or_else(|| "none".to_owned(), |rate| format!("{rate} records/s"))
    );
    let _ = writeln!(
        text,
        "- bracket: {} .. {} (resolution {} records/s)\n",
        rate_or_dash(document.bracket_low),
        rate_or_dash(document.bracket_high),
        document.resolution
    );
    let _ = writeln!(text, "## Probes\n");
    let _ = writeln!(
        text,
        "| offered records/s | satisfied | attempt | reasons |"
    );
    let _ = writeln!(text, "| ---: | :---: | --- | --- |");
    for probe in document.probes.iter().chain(&document.confirmation) {
        let _ = writeln!(
            text,
            "| {} | {} | `{}` | {} |",
            probe.offered_records_per_second,
            if probe.satisfied { "yes" } else { "no" },
            probe.attempt_id,
            if probe.reasons.is_empty() {
                "—".to_owned()
            } else {
                probe.reasons.join("; ")
            }
        );
    }
    if !notes.is_empty() {
        let _ = writeln!(text, "\n## Notes\n");
        for note in notes {
            let _ = writeln!(text, "- {note}");
        }
    }
    text
}

/// A rate, or a dash for a bracket end the search never found.
fn rate_or_dash(rate: Option<u64>) -> String {
    rate.map_or_else(|| "-".to_owned(), |value| value.to_string())
}

/// The rate override, tested from inside the module because [`probe_inputs`] is
/// private: the refusal it adds is the point of the test.
#[cfg(test)]
#[path = "capacity_test.rs"]
mod capacity_test;
