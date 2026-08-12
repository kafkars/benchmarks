//! Always-seal: every terminal path of an attempt — success, subject failure,
//! tool failure, timeout, interrupt, panic — converges on exactly one sealing
//! pass that writes status, classification, comparison, execution order,
//! checksums, and the bundle manifest, in that order.
//!
//! # Three layers, because one is not enough
//!
//! 1. **One funnel.** Nothing in [`run_attempt`] returns early on failure.
//!    Every phase records what happened and the attempt continues to the single
//!    `seal` call at the end. A subject that failed is evidence about that
//!    subject, not a reason to discard the evidence about the other one.
//! 2. **A caught panic.** The whole supervision body runs inside
//!    [`catch_unwind`](std::panic::catch_unwind), so a bug in the control plane
//!    becomes a `crashed` bundle carrying everything recorded up to the panic,
//!    rather than a stack trace and an empty directory. Deriving the verdict —
//!    classification, comparison, and the percentiles they read out of the
//!    subjects' own histograms — runs inside that boundary too, because that
//!    derivation is the first code in the attempt to *interpret* bytes an
//!    adapter wrote. A panic there is evidence-triggered, and evidence-triggered
//!    failures are exactly the ones that must still seal.
//! 3. **A drop guard.** A last-resort guard is armed on entry and disarmed only by a
//!    completed seal. If the process unwinds past the funnel for any reason, its
//!    `Drop` writes a minimal `status.json` and a checksum manifest, best
//!    effort, errors swallowed. A last-resort seal that fails loudly would be
//!    worse than one that fails quietly: the original failure is the news.
//!
//! # What ends an attempt, and how it is named
//!
//! Execution status takes the worst of every phase: `timed_out` > `crashed` >
//! `partial` > `complete`. A deadline expiry is a timeout; a child dying on a
//! signal nobody sent it, or a panic in this crate, is a crash; a non-zero
//! adapter exit, a verifier that could not run, an interrupt, or a phase that
//! was skipped is partial.
//!
//! Phase records answer a different question from execution status. A verifier
//! that ran cleanly and found missing records is a *failed phase* — the check
//! did not pass — but not a broken machine, so the execution status stays
//! `complete` and the verdict lands in `classification.json`. Confusing the two
//! is how "the run finished" turns into "the numbers are good".
//!
//! # Order of writes
//!
//! `status.json` is written first so that a seal which dies halfway still says
//! why. `checksums.txt` is written after every other document because it covers
//! them, and `bundle.json` last because it contains the checksum file's digest.
//!
//! That order has a consequence the failure paths must respect: a seal can fail
//! *after* it has already written a rich `status.json`. [`seal_failure`], which
//! the pipeline calls when anything downstream reports a failure, therefore
//! never overwrites a `status.json` that already exists. Replacing a status
//! carrying every subject and phase with a two-line stub would destroy the
//! evidence in the name of recording that something went wrong — the same
//! mistake the last-resort guard already avoids with its `if !exists` check.

use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use bench_schema::{
    BundleManifest, Classification, ClusterTools, Comparison, EnvironmentDocument, ExecutionOrder,
    ExecutionStatus, PhaseOutcome, PhaseRecord, ResolvedExperiment, RunStatus, SchemaResult,
    SubjectsLock, pretty_bytes,
};

use crate::attempt::AttemptPaths;
use crate::checksum::checksum_bundle;
use crate::error::{CtlError, CtlResult};
use crate::interrupt::{self, InterruptFlag};
use crate::results::{self, SubjectOutcome, VerificationVerdict};
use crate::supervise::{SupervisedRun, ToolSpec};
use crate::verify::{self, VerificationPhase};
use crate::{topics, utc_rfc3339_millis};

/// Everything one attempt needs, once resolution and probing have succeeded.
///
/// The workspace is already finalized when this arrives: creating it, claiming
/// it, and moving it under the experiment id are the caller's job, because those
/// are the failures that happen *before* there is anywhere to seal into. From
/// here on, every failure produces a bundle.
#[derive(Debug, Clone)]
pub struct AttemptRequest {
    /// The resolved experiment, with its runtime binding present.
    pub resolved: ResolvedExperiment,
    /// What each subject was at probe time.
    pub lock: SubjectsLock,
    /// The human-authored scenario, sealed verbatim.
    pub source_toml: String,
    /// The machine the attempt is running on.
    pub environment: EnvironmentDocument,
    /// Argument-vector prefixes for the configured cluster tools.
    pub tools: ClusterTools,
    /// The finalized bundle layout.
    pub paths: AttemptPaths,
    /// When the attempt started, for the status document.
    pub started_at: SystemTime,
}

/// Runs one attempt to completion and seals its evidence bundle.
///
/// Returns the sealed execution status, which the caller maps to a process exit
/// code. A returned status of `partial`, `crashed`, or `timed_out` is a
/// successful call: the bundle exists and says what went wrong.
///
/// # Errors
///
/// Returns a [`CtlErrorKind::Seal`](crate::CtlErrorKind::Seal) error only when
/// the bundle could not be written. Everything else that can go wrong is
/// recorded in the bundle instead.
#[expect(
    clippy::needless_pass_by_value,
    reason = "the seam takes the request by value so that nothing can mutate it while the \
              attempt is in flight"
)]
pub fn run_attempt(request: AttemptRequest) -> CtlResult<ExecutionStatus> {
    let paths = request.paths.clone();
    let mut guard = SealOnDrop::arm(&paths);
    let interrupt = interrupt::process_latch();
    let state = Mutex::new(AttemptState::new());
    let panic = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let mut held = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        supervise_attempt(&request, &mut held, &interrupt);
    }))
    .err();
    let mut state = state
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(payload) = panic {
        let reason = describe_panic(&*payload);
        state.record("supervise", PhaseOutcome::Failed, Some(reason.clone()));
        state.degrade(
            ExecutionStatus::Crashed,
            format!("the control plane panicked: {reason}"),
        );
    }
    if interrupt.is_set() {
        state.interrupted = true;
        state.degrade(ExecutionStatus::Partial, "the run was interrupted");
    }
    let plan = derive_plan(&mut state, &request, AttemptState::plan);
    let sealed = seal(&paths, &plan)?;
    guard.disarm();
    Ok(sealed)
}

/// Runs `derive` inside the panic boundary, falling back to a bundle that says
/// the verdict could not be derived.
///
/// [`AttemptState::plan`] — the `derive` every caller but the tests passes — is
/// where the attempt stops recording facts and starts interpreting them: it
/// decodes each subject's histograms to take a percentile, which makes it the
/// first place a document an adapter wrote reaches code that could give up on
/// it. Running it outside the panic boundary would send an evidence-triggered
/// panic all the way out of [`run_attempt`], past the funnel, into a stub.
/// Running it inside means the same panic seals `crashed` with every subject
/// record the attempt collected still in the bundle.
///
/// The derivation arrives as a parameter rather than being called directly so
/// that the boundary can be tested for the property it exists for. A panic here
/// is by definition a bug nobody has written yet, and a guard nobody can
/// exercise is a guard nobody can trust.
fn derive_plan(
    state: &mut AttemptState,
    request: &AttemptRequest,
    derive: fn(&AttemptState, &AttemptRequest) -> SealPlan,
) -> SealPlan {
    match std::panic::catch_unwind(AssertUnwindSafe(|| derive(state, request))) {
        Ok(plan) => plan,
        Err(payload) => {
            let reason = describe_panic(&*payload);
            eprintln!("benchctl: deriving the verdict panicked: {reason}");
            state.record("classify", PhaseOutcome::Failed, Some(reason.clone()));
            state.degrade(
                ExecutionStatus::Crashed,
                format!("deriving the verdict panicked: {reason}"),
            );
            state.undecided_plan(request, &reason)
        }
    }
}

/// Seals a bundle for an attempt that failed before it could run.
///
/// Used by the phases that happen before [`run_attempt`] — reading the sources,
/// probing the subjects, resolving the experiment, capturing the environment —
/// so that a failure there still leaves a bundle behind rather than an empty
/// directory. The source TOML is sealed when the caller managed to read it,
/// because "the file we could not use" is the most useful thing such a bundle
/// can carry.
///
/// # When the bundle is already sealed
///
/// The pipeline also calls this when a failure is reported *after* an attempt
/// ran, including a seal that died partway through its own write order. In that
/// case `status.json` already exists and already says more than this function
/// could: the stub below carries no subjects and one invented phase. It is
/// therefore kept, the reason is recorded beside it in
/// [`seal_failure_txt`](AttemptPaths::seal_failure_txt), and the terminal files
/// are completed over whatever the bundle holds. Overwriting richer evidence
/// with poorer evidence is not a fallback, it is a loss.
///
/// # Errors
///
/// As for [`run_attempt`]: only a failure to write the bundle. The
/// already-sealed path never errors — there is a bundle either way, and the
/// failure that brought us here is the news.
pub fn seal_failure(
    paths: &AttemptPaths,
    source_toml: Option<&str>,
    status: ExecutionStatus,
    failure_reason: &str,
) -> CtlResult<ExecutionStatus> {
    if paths.status_json().exists() {
        return Ok(preserve_sealed_evidence(paths, status, failure_reason));
    }
    let mut guard = SealOnDrop::arm(paths);
    if let Some(text) = source_toml {
        if let Err(error) = std::fs::write(paths.experiment_source_toml(), text.as_bytes()) {
            eprintln!("benchctl: could not seal the source scenario: {error}");
        }
    }
    let plan = SealPlan {
        status: RunStatus {
            schema: RunStatus::SCHEMA.to_owned(),
            experiment_id: None,
            attempt_id: attempt_id_of(paths),
            execution_status: status,
            failure_reason: Some(failure_reason.to_owned()),
            interrupted: false,
            subjects: Vec::new(),
            phases: vec![PhaseRecord {
                name: "pre-attempt".to_owned(),
                outcome: PhaseOutcome::Failed,
                detail: Some(failure_reason.to_owned()),
            }],
        },
        classification: Some(results::classify(&[], &[failure_reason.to_owned()])),
        comparison: None,
        execution_order: None,
    };
    let sealed = seal(paths, &plan)?;
    guard.disarm();
    Ok(sealed)
}

/// Keeps an already-sealed `status.json` and finishes the bundle around it.
///
/// Returns the execution status the bundle itself records, read back from disk
/// rather than remembered, because the document is the evidence. `fallback` is
/// used only when that document cannot be parsed — at which point the caller's
/// idea of how the attempt went is the best answer available.
///
/// Nothing already in the bundle is rewritten, including the sealed scenario: a
/// bundle that got this far sealed its own inputs, and rewriting a covered file
/// would put it at odds with a `checksums.txt` that may already exist. Only the
/// note is new, and it is written *before* the terminal files so that the
/// manifest covers it like everything else.
fn preserve_sealed_evidence(
    paths: &AttemptPaths,
    fallback: ExecutionStatus,
    failure_reason: &str,
) -> ExecutionStatus {
    eprintln!(
        "benchctl: {} is already sealed; keeping its status and recording the later failure \
         beside it",
        paths.root().display()
    );
    let note = format!(
        "sealing did not finish at {}\n{failure_reason}\n",
        utc_rfc3339_millis(SystemTime::now())
    );
    if let Err(error) = std::fs::write(paths.seal_failure_txt(), note.as_bytes()) {
        eprintln!("benchctl: could not record the seal failure in the bundle: {error}");
    }
    write_missing_terminal_files(paths);
    crate::pipeline::read_status(paths).map_or(fallback, |status| status.execution_status)
}

/// Writes `checksums.txt` and `bundle.json` when they are absent, best effort.
///
/// Shared by the last-resort guard and by [`seal_failure`]'s preservation path,
/// which want exactly the same thing: complete the bundle over whatever is
/// there, and never disturb terminal files a successful seal already wrote.
/// Every error is reported and swallowed — this only runs where something else
/// has already failed, and that failure is the one worth exiting on.
fn write_missing_terminal_files(paths: &AttemptPaths) {
    if paths.checksums_txt().exists() && paths.bundle_json().exists() {
        return;
    }
    let manifest = match checksum_bundle(paths.root()) {
        Ok(manifest) => manifest,
        Err(error) => {
            eprintln!("benchctl: the bundle could not be checksummed: {error}");
            return;
        }
    };
    if let Err(error) = std::fs::write(paths.checksums_txt(), manifest.text.as_bytes()) {
        eprintln!("benchctl: the bundle's checksums could not be written: {error}");
        return;
    }
    let bundle =
        BundleManifest::from_checksums_bytes(manifest.text.as_bytes(), manifest.total_bytes);
    match bundle.as_ref().map(bench_schema::pretty_bytes) {
        Ok(Ok(bytes)) => {
            if let Err(error) = std::fs::write(paths.bundle_json(), &bytes) {
                eprintln!("benchctl: the bundle manifest could not be written: {error}");
            }
        }
        Ok(Err(error)) => eprintln!("benchctl: the bundle manifest could not be rendered: {error}"),
        Err(error) => eprintln!("benchctl: the bundle manifest could not be built: {error}"),
    }
}

/// The documents one seal writes, in the order it writes them.
#[derive(Debug)]
struct SealPlan {
    status: RunStatus,
    classification: Option<Classification>,
    comparison: Option<Comparison>,
    execution_order: Option<ExecutionOrder>,
}

/// The single funnel: writes every document, then the checksums, then the
/// manifest.
fn seal(paths: &AttemptPaths, plan: &SealPlan) -> CtlResult<ExecutionStatus> {
    write_rendered(&paths.status_json(), pretty_bytes(&plan.status))?;
    if let Some(classification) = &plan.classification {
        // The verdict document is checked against its own schema before it is
        // written. Everything that builds one here satisfies the invariants by
        // construction, so this can only fire on a bug in this crate — and a bug
        // that seals a self-contradicting verdict is exactly the one worth
        // catching at the moment it would become evidence.
        classification
            .validate()
            .map_err(|error| seal_error(format!("the classification is malformed: {error}")))?;
        write_rendered(&paths.classification_json(), pretty_bytes(classification))?;
    }
    if let Some(comparison) = &plan.comparison {
        write_rendered(&paths.comparison_json(), pretty_bytes(comparison))?;
    }
    if let Some(order) = &plan.execution_order {
        write_rendered(&paths.execution_order_json(), pretty_bytes(order))?;
    }
    let manifest = checksum_bundle(paths.root())?;
    write_bytes(&paths.checksums_txt(), manifest.text.as_bytes())?;
    let bundle =
        BundleManifest::from_checksums_bytes(manifest.text.as_bytes(), manifest.total_bytes)
            .map_err(|error| seal_error(format!("build the bundle manifest: {error}")))?;
    write_rendered(&paths.bundle_json(), pretty_bytes(&bundle))?;
    Ok(plan.status.execution_status)
}

/// Writes an already-rendered document, reporting a render failure as a seal
/// failure.
///
/// The rendering is passed in rather than performed here because this crate does
/// not depend on `serde` directly; `bench_schema` owns the byte form of every
/// document, which is where that decision belongs anyway.
fn write_rendered(path: &Path, rendered: SchemaResult<Vec<u8>>) -> CtlResult<()> {
    let bytes =
        rendered.map_err(|error| seal_error(format!("render {}: {error}", path.display())))?;
    write_bytes(path, &bytes)
}

/// Writes bytes, reporting failure on stderr as well as to the caller.
fn write_bytes(path: &Path, bytes: &[u8]) -> CtlResult<()> {
    std::fs::write(path, bytes)
        .map_err(|error| seal_error(format!("write {}: {error}", path.display())))
}

/// Builds a seal error, announcing it on stderr because the caller may be about
/// to exit with nothing else to say.
fn seal_error(message: String) -> CtlError {
    eprintln!("benchctl: seal failure: {message}");
    CtlError::seal(message)
}

/// The attempt id, taken from the directory the bundle lives in.
fn attempt_id_of(paths: &AttemptPaths) -> String {
    paths.root().file_name().map_or_else(
        || "unknown".to_owned(),
        |name| name.to_string_lossy().into_owned(),
    )
}

/// Extracts a readable reason from a caught panic payload.
fn describe_panic(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|text| (*text).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "a panic with no message".to_owned())
}

/// Everything the attempt has learned so far.
#[derive(Debug)]
struct AttemptState {
    status: ExecutionStatus,
    failure_reason: Option<String>,
    interrupted: bool,
    phases: Vec<PhaseRecord>,
    subjects: Vec<SubjectOutcome>,
    invalidating: Vec<String>,
}

impl AttemptState {
    fn new() -> Self {
        Self {
            status: ExecutionStatus::Complete,
            failure_reason: None,
            interrupted: false,
            phases: Vec::new(),
            subjects: Vec::new(),
            invalidating: Vec::new(),
        }
    }

    /// Records one phase of the state machine.
    fn record(&mut self, name: impl Into<String>, outcome: PhaseOutcome, detail: Option<String>) {
        self.phases.push(PhaseRecord {
            name: name.into(),
            outcome,
            detail,
        });
    }

    /// Worsens the execution status, keeping the reason that caused the worst
    /// status seen so far.
    fn degrade(&mut self, status: ExecutionStatus, reason: impl Into<String>) {
        let worse = status.severity() > self.status.severity();
        if worse || (status == self.status && self.failure_reason.is_none()) {
            self.failure_reason = Some(reason.into());
        }
        self.status = self.status.worst(status);
    }

    /// Records a reason the run's evidence may not be believed, separate from
    /// whether the machinery worked.
    fn invalidate(&mut self, reason: impl Into<String>) {
        self.invalidating.push(reason.into());
    }

    /// The run status this state describes: pure record-keeping, nothing
    /// derived from a subject's own bytes, so nothing here can be made to fail
    /// by the evidence.
    fn run_status(&self, request: &AttemptRequest) -> RunStatus {
        RunStatus {
            schema: RunStatus::SCHEMA.to_owned(),
            experiment_id: bench_schema::experiment_id(&request.resolved).ok(),
            attempt_id: attempt_id_of(&request.paths),
            execution_status: self.status,
            failure_reason: self.failure_reason.clone(),
            interrupted: self.interrupted,
            subjects: self
                .subjects
                .iter()
                .map(SubjectOutcome::execution_record)
                .collect(),
            phases: self.phases.clone(),
        }
    }

    /// Turns the accumulated state into the documents to seal.
    ///
    /// Borrows rather than consumes so that [`derive_plan`] still holds the
    /// state if this panics, and can seal the part of it that never depended on
    /// reading a subject's evidence.
    fn plan(&self, request: &AttemptRequest) -> SealPlan {
        let order = execution_order(&request.resolved);
        SealPlan {
            status: self.run_status(request),
            classification: Some(results::classify(&self.subjects, &self.invalidating)),
            comparison: Some(results::compare(&order, &self.subjects)),
            execution_order: Some(ExecutionOrder::new(
                order,
                "resolved-experiment-runtime-binding",
            )),
        }
    }

    /// The plan for an attempt whose verdict could not be derived at all.
    ///
    /// The status is the full record — every subject, every phase — because
    /// that is precisely the evidence a reader needs when the derivation over it
    /// is the thing that broke. The classification names the panic and refuses
    /// the run; the comparison is absent, because computing it is what failed,
    /// and a comparison invented here would be the fabrication the whole
    /// always-seal design exists to prevent.
    fn undecided_plan(&self, request: &AttemptRequest, reason: &str) -> SealPlan {
        SealPlan {
            status: self.run_status(request),
            classification: Some(Classification {
                schema: Classification::SCHEMA.to_owned(),
                run_valid: false,
                claim_eligible: false,
                subjects: Vec::new(),
                deferred_checks: results::DEFERRED_CHECKS
                    .iter()
                    .map(|check| (*check).to_owned())
                    .collect(),
                reasons: vec![format!(
                    "the attempt's verdict could not be derived: {reason}"
                )],
            }),
            comparison: None,
            execution_order: Some(ExecutionOrder::new(
                execution_order(&request.resolved),
                "resolved-experiment-runtime-binding",
            )),
        }
    }
}

/// The subject order the runtime binding fixed.
fn execution_order(experiment: &ResolvedExperiment) -> Vec<String> {
    experiment
        .runtime
        .as_ref()
        .map(|runtime| runtime.execution_order.clone())
        .unwrap_or_default()
}

/// The phase machine of one attempt. Records everything; returns nothing;
/// never gives up early except where continuing would produce noise instead of
/// evidence.
fn supervise_attempt(
    request: &AttemptRequest,
    state: &mut AttemptState,
    interrupt: &InterruptFlag,
) {
    seal_inputs(request, state);
    let order = execution_order(&request.resolved);
    if order.is_empty() {
        state.record(
            "subjects",
            PhaseOutcome::Skipped,
            Some("the resolved experiment carries no runtime execution order".to_owned()),
        );
        state.degrade(
            ExecutionStatus::Partial,
            "the resolved experiment carries no runtime execution order",
        );
        return;
    }
    if !create_topics(request, state, interrupt) {
        // Topic creation is a precondition, not a phase: subjects run against
        // topics whose geometry the control plane chose, and a subject that
        // produces to a topic nobody created is producing noise. The attempt
        // stops here and seals what it has.
        for name in &order {
            state.subjects.push(SubjectOutcome::skipped(name));
            state.record(
                format!("subject:{name}:run"),
                PhaseOutcome::Skipped,
                Some("topic creation failed".to_owned()),
            );
        }
        state.invalidate("no subject ran because topic creation failed");
        cleanup_topics(request, state, interrupt);
        return;
    }
    for name in &order {
        let outcome = run_subject(request, state, interrupt, name);
        state.subjects.push(outcome);
    }
    cleanup_topics(request, state, interrupt);
}

/// Writes the four sealed inputs: the scenario verbatim, and the three resolved
/// documents.
fn seal_inputs(request: &AttemptRequest, state: &mut AttemptState) {
    let paths = &request.paths;
    let mut failures = Vec::new();
    if let Err(error) = std::fs::write(
        paths.experiment_source_toml(),
        request.source_toml.as_bytes(),
    ) {
        failures.push(format!("experiment.source.toml: {error}"));
    }
    for (path, bytes) in [
        (
            paths.experiment_resolved_json(),
            bench_schema::pretty_bytes(&request.resolved),
        ),
        (
            paths.subjects_lock_json(),
            bench_schema::pretty_bytes(&request.lock),
        ),
        (
            paths.environment_json(),
            bench_schema::pretty_bytes(&request.environment),
        ),
    ] {
        match bytes {
            Ok(bytes) => {
                if let Err(error) = std::fs::write(&path, &bytes) {
                    failures.push(format!("{}: {error}", path.display()));
                }
            }
            Err(error) => failures.push(format!("{}: {error}", path.display())),
        }
    }
    if failures.is_empty() {
        // The run status has no timestamp field and the attempt id carries only
        // whole seconds, so the first phase record is where the attempt's start
        // is stated to the precision it was taken at.
        state.record(
            "seal-inputs",
            PhaseOutcome::Succeeded,
            Some(format!(
                "attempt started at {}",
                utc_rfc3339_millis(request.started_at)
            )),
        );
    } else {
        let detail = failures.join("; ");
        state.record("seal-inputs", PhaseOutcome::Failed, Some(detail.clone()));
        state.degrade(
            ExecutionStatus::Partial,
            format!("sealed inputs incomplete: {detail}"),
        );
        state.invalidate(format!(
            "the attempt could not seal its own inputs: {detail}"
        ));
    }
}

/// Creates every topic. Returns whether the attempt may proceed to its subjects.
fn create_topics(
    request: &AttemptRequest,
    state: &mut AttemptState,
    interrupt: &InterruptFlag,
) -> bool {
    if request.tools.topic_create.is_empty() {
        // An operator who configured no topic tool is opting out, not failing:
        // the topics exist by some other arrangement. The skipped check is
        // recorded, and the run is partial because nothing verified it.
        state.record(
            "topic-create",
            PhaseOutcome::Skipped,
            Some("no topic-create tool is configured".to_owned()),
        );
        state.degrade(
            ExecutionStatus::Partial,
            "topic creation was not configured",
        );
        state.invalidate("topic geometry was not established by the control plane");
        return true;
    }
    match topics::create(&request.paths, &request.tools, &request.resolved, interrupt) {
        Ok(outcome) if outcome.succeeded() => {
            state.record("topic-create", PhaseOutcome::Succeeded, None);
            true
        }
        Ok(outcome) => {
            let detail = format!("the topic tool {}", outcome.describe());
            state.record("topic-create", PhaseOutcome::Failed, Some(detail.clone()));
            state.degrade(
                topic_failure_status(&outcome),
                format!("topic creation failed: {detail}"),
            );
            false
        }
        Err(error) => {
            let detail = error.to_string();
            state.record("topic-create", PhaseOutcome::Failed, Some(detail.clone()));
            state.degrade(
                ExecutionStatus::Partial,
                format!("topic creation failed: {detail}"),
            );
            false
        }
    }
}

/// Deletes every topic, best effort: a failure is recorded and never changes the
/// execution status.
fn cleanup_topics(request: &AttemptRequest, state: &mut AttemptState, interrupt: &InterruptFlag) {
    if request.tools.topic_delete.is_empty() {
        state.record(
            "topic-cleanup",
            PhaseOutcome::Skipped,
            Some("no topic-delete tool is configured".to_owned()),
        );
        return;
    }
    let detail = match topics::delete(&request.paths, &request.tools, &request.resolved, interrupt)
    {
        Ok(outcome) if outcome.succeeded() => {
            state.record("topic-cleanup", PhaseOutcome::Succeeded, None);
            return;
        }
        Ok(outcome) => format!("the topic tool {}", outcome.describe()),
        Err(error) => error.to_string(),
    };
    eprintln!("benchctl: topic cleanup failed, leaving topics behind: {detail}");
    state.record("topic-cleanup", PhaseOutcome::Failed, Some(detail));
}

/// Maps a failed topic tool onto an execution status.
fn topic_failure_status(outcome: &SupervisedRun) -> ExecutionStatus {
    if outcome.exit.timed_out {
        ExecutionStatus::TimedOut
    } else {
        ExecutionStatus::Partial
    }
}

/// Runs one subject and verifies both of its topics.
fn run_subject(
    request: &AttemptRequest,
    state: &mut AttemptState,
    interrupt: &InterruptFlag,
    name: &str,
) -> SubjectOutcome {
    let mut outcome = SubjectOutcome::skipped(name);
    // The objectives travel with the subject because the validity gate reads
    // them.
    outcome.slo = request.resolved.slo;
    // So does the version the adapter claimed at probe time, because the gate
    // compares it against the one the client reports at run time. Taken from the
    // resolved experiment rather than the subjects lock: the resolved document
    // is what the experiment id is computed over, so this is exactly the string
    // the identity was built from.
    if let Some(subject) = request.resolved.subject(name) {
        outcome
            .declared_adapter_version
            .clone_from(&subject.adapter_version);
    }
    let phase = format!("subject:{name}:run");
    if interrupt.is_set() {
        state.record(
            phase,
            PhaseOutcome::Skipped,
            Some("the run was interrupted".to_owned()),
        );
        return outcome;
    }
    let Some(spec) = subject_spec(request, state, name, &phase) else {
        return outcome;
    };
    match crate::supervise::run(&spec, interrupt) {
        Ok(run) => {
            outcome.execution = Some(run.exit);
            outcome.interrupted = run.interrupted;
            record_subject_exit(state, &phase, run);
        }
        Err(error) => {
            let detail = error.to_string();
            state.record(&phase, PhaseOutcome::Failed, Some(detail.clone()));
            state.degrade(
                ExecutionStatus::Partial,
                format!("{name} did not run: {detail}"),
            );
            return outcome;
        }
    }
    outcome.evidence = results::read_subject(&request.paths, name);
    verify_topic(
        request,
        state,
        interrupt,
        &mut outcome,
        VerificationPhase::Measured,
    );
    verify_topic(
        request,
        state,
        interrupt,
        &mut outcome,
        VerificationPhase::Warmup,
    );
    outcome
}

/// Builds the supervision spec for one subject, or records why it cannot.
fn subject_spec(
    request: &AttemptRequest,
    state: &mut AttemptState,
    name: &str,
    phase: &str,
) -> Option<ToolSpec> {
    let paths = &request.paths;
    let directory = paths.adapter_dir(name);
    if let Err(error) = std::fs::create_dir_all(&directory) {
        let detail = format!("create {}: {error}", directory.display());
        state.record(phase, PhaseOutcome::Failed, Some(detail.clone()));
        state.degrade(ExecutionStatus::Partial, detail);
        return None;
    }
    let Some(subject) = request.resolved.subject(name) else {
        let detail = format!("the resolved experiment has no subject named {name}");
        state.record(phase, PhaseOutcome::Failed, Some(detail.clone()));
        state.degrade(ExecutionStatus::Partial, detail);
        return None;
    };
    let mut argv = subject.command.clone();
    argv.push("run".to_owned());
    argv.push("--experiment".to_owned());
    argv.push(path_argument(&paths.experiment_resolved_json()));
    argv.push("--output".to_owned());
    argv.push(path_argument(&directory));
    Some(
        ToolSpec::new(
            argv,
            Duration::from_secs(request.resolved.budget.run_timeout_seconds),
        )
        .with_stdout_file(paths.adapter_stdout_log(name))
        .with_stderr_file(paths.adapter_stderr_log(name)),
    )
}

/// Records how a subject's process ended and worsens the status accordingly.
fn record_subject_exit(state: &mut AttemptState, phase: &str, run: SupervisedRun) {
    let detail = run.describe();
    if run.succeeded() {
        state.record(phase, PhaseOutcome::Succeeded, None);
        return;
    }
    state.record(phase, PhaseOutcome::Failed, Some(detail.clone()));
    let status = if run.exit.timed_out {
        ExecutionStatus::TimedOut
    } else if run.interrupted {
        // Order matters here. A subject that died on `SIGINT` because the
        // terminal signalled the whole process group arrives with a signal set
        // *and* the interrupt flag set, and it is an interruption, not a crash.
        // The supervisor is the one that decides which of those it was, by
        // consulting its latch after the child is reaped; this branch only has
        // to be asked first.
        state.interrupted = true;
        ExecutionStatus::Partial
    } else if run.exit.signal.is_some() {
        // A signal with no interrupt latched is a signal nobody in this process
        // asked for: a crash, not a policy.
        ExecutionStatus::Crashed
    } else {
        ExecutionStatus::Partial
    };
    state.degrade(status, format!("{phase}: {detail}"));
}

/// Verifies one of a subject's topics, recording the phase and the verdict.
fn verify_topic(
    request: &AttemptRequest,
    state: &mut AttemptState,
    interrupt: &InterruptFlag,
    outcome: &mut SubjectOutcome,
    phase: VerificationPhase,
) {
    let name = outcome.name.clone();
    let label = format!("subject:{name}:verify-{}", phase.as_str());
    let verdict = if request.tools.verify.is_empty() {
        state.record(
            &label,
            PhaseOutcome::Skipped,
            Some("no verifier is configured".to_owned()),
        );
        state.degrade(
            ExecutionStatus::Partial,
            "read-back verification was not configured",
        );
        VerificationVerdict::NotRun
    } else if phase == VerificationPhase::Warmup && request.resolved.warmup_records == 0 {
        state.record(
            &label,
            PhaseOutcome::Succeeded,
            Some("the experiment produces no warmup records".to_owned()),
        );
        VerificationVerdict::NotRequired
    } else {
        run_verifier(request, state, interrupt, outcome, phase, &label)
    };
    match phase {
        VerificationPhase::Measured => outcome.measured = verdict,
        VerificationPhase::Warmup => outcome.warmup = verdict,
    }
}

/// Runs the configured verifier for one topic.
fn run_verifier(
    request: &AttemptRequest,
    state: &mut AttemptState,
    interrupt: &InterruptFlag,
    outcome: &mut SubjectOutcome,
    phase: VerificationPhase,
    label: &str,
) -> VerificationVerdict {
    match verify::verify(
        &request.paths,
        &request.tools,
        &request.resolved,
        &outcome.name,
        phase,
        interrupt,
    ) {
        Ok(run) => {
            match phase {
                VerificationPhase::Measured => outcome.verification.measured = Some(run.outcome),
                VerificationPhase::Warmup => outcome.verification.warmup = Some(run.outcome),
            }
            if !run.tool_succeeded {
                state.record(label, PhaseOutcome::Failed, Some(run.detail.clone()));
                state.degrade(ExecutionStatus::Partial, format!("{label}: {}", run.detail));
                return VerificationVerdict::NotRun;
            }
            if run.satisfies_contract {
                state.record(label, PhaseOutcome::Succeeded, Some(run.detail));
                VerificationVerdict::Satisfied
            } else {
                // The machinery worked and the answer was no: a failed check,
                // not a failed run.
                state.record(label, PhaseOutcome::Failed, Some(run.detail));
                VerificationVerdict::Failed
            }
        }
        Err(error) => {
            let detail = error.to_string();
            state.record(label, PhaseOutcome::Failed, Some(detail.clone()));
            state.degrade(ExecutionStatus::Partial, format!("{label}: {detail}"));
            VerificationVerdict::NotRun
        }
    }
}

/// Renders a path as a command-line argument.
fn path_argument(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

/// The last-resort seal: armed when an attempt starts, disarmed by a completed
/// seal, and otherwise writing the least a bundle can carry.
#[derive(Debug)]
struct SealOnDrop {
    paths: AttemptPaths,
    armed: bool,
}

impl SealOnDrop {
    /// Arms the guard for a bundle.
    fn arm(paths: &AttemptPaths) -> Self {
        Self {
            paths: paths.clone(),
            armed: true,
        }
    }

    /// Disarms the guard: the funnel sealed the bundle itself.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SealOnDrop {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        eprintln!(
            "benchctl: sealing {} from the last-resort guard",
            self.paths.root().display()
        );
        if !self.paths.status_json().exists() {
            let status = RunStatus {
                schema: RunStatus::SCHEMA.to_owned(),
                experiment_id: None,
                attempt_id: attempt_id_of(&self.paths),
                execution_status: ExecutionStatus::Crashed,
                failure_reason: Some(format!(
                    "the control plane exited without sealing (guard ran at {})",
                    utc_rfc3339_millis(SystemTime::now())
                )),
                interrupted: false,
                subjects: Vec::new(),
                phases: Vec::new(),
            };
            if let Ok(bytes) = bench_schema::pretty_bytes(&status) {
                if let Err(error) = std::fs::write(self.paths.status_json(), &bytes) {
                    eprintln!("benchctl: the last-resort seal could not write a status: {error}");
                }
            }
        }
        write_missing_terminal_files(&self.paths);
    }
}

/// The panic boundary around the verdict derivation, tested from inside the
/// module because the fallback it produces is private by design.
#[cfg(test)]
#[path = "seal_boundary_test.rs"]
mod seal_boundary_test;
