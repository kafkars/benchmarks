//! Broker-visible verification through the configured verifier tool: one run
//! per subject and phase, stdout captured to the bundle, the report parsed
//! leniently and gated into the classification.
//!
//! An adapter reports what it observed from inside the client. The verifier
//! reports what the cluster actually holds, by reading the topic back. Only the
//! second can catch a client that acknowledged records it never delivered, which
//! is why verification is a configured tool the control plane runs and never
//! part of the adapter protocol.
//!
//! The argv is the legacy six-argument shape, preserved exactly:
//! `<prefix...> <bootstrap> <topic> <run-id> <records> <payload-bytes> <partitions>`.
//! The measured and warmup phases differ only in the topic and the expected
//! record count, exactly as the legacy comparison script called it.
//!
//! Parsing is lenient — [`VerifierReport`] names only the fields the gate needs
//! — but gating is not. A report satisfies the contract only if it is about the
//! topic that was asked about, expects the count that was asked for, verifies
//! all of it, and reports no duplicates, no gaps, no corruption, and every
//! partition read to its end. A verifier's own `valid` flag is necessary and
//! nowhere near sufficient: it does not know which topic the control plane meant.

use std::path::PathBuf;
use std::time::Duration;

use bench_schema::{ClusterTools, ResolvedExperiment, VerificationOutcome, VerifierReport};

use crate::attempt::AttemptPaths;
use crate::error::{CtlError, CtlResult};
use crate::interrupt::InterruptFlag;
use crate::supervise::{SupervisedRun, ToolSpec, run};

/// Which of a subject's two topics a verification covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationPhase {
    /// The topic the measured phase produced to.
    Measured,
    /// The topic the warmup phase produced to.
    Warmup,
}

impl VerificationPhase {
    /// The name this phase uses in file names and phase records.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Measured => "measured",
            Self::Warmup => "warmup",
        }
    }
}

/// Builds the legacy verifier argv from a configured prefix.
#[must_use]
pub fn verify_argv(
    prefix: &[String],
    bootstrap: &str,
    topic: &str,
    run_id: &str,
    records: u64,
    payload_bytes: u32,
    partitions: u32,
) -> Vec<String> {
    let mut argv = prefix.to_vec();
    argv.push(bootstrap.to_owned());
    argv.push(topic.to_owned());
    argv.push(run_id.to_owned());
    argv.push(records.to_string());
    argv.push(payload_bytes.to_string());
    argv.push(partitions.to_string());
    argv
}

/// One verification run, and what it concluded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationRun {
    /// What the run status records about this check.
    pub outcome: VerificationOutcome,
    /// Whether the verifier process itself ran cleanly.
    ///
    /// Separate from the verdict on purpose: a verifier that exits non-zero
    /// broke the machinery and makes the attempt partial, while a verifier that
    /// ran fine and reported missing records leaves the attempt complete and
    /// invalid.
    pub tool_succeeded: bool,
    /// Whether the report satisfies the contract for the topic that was asked
    /// about — not merely whether the verifier liked what it saw.
    pub satisfies_contract: bool,
    /// One line describing what happened, for the phase record.
    pub detail: String,
}

/// Verifies one subject's topic for one phase, writing the report into the
/// bundle.
///
/// # Errors
///
/// Returns an error only when the verifier could not be spawned or reaped, or
/// when the experiment carries no runtime binding. A verifier that ran and
/// reported a problem is a successful run whose [`VerificationRun`] says so.
pub fn verify(
    paths: &AttemptPaths,
    tools: &ClusterTools,
    experiment: &ResolvedExperiment,
    subject: &str,
    phase: VerificationPhase,
    interrupt: &InterruptFlag,
) -> CtlResult<VerificationRun> {
    let runtime = experiment.runtime.as_ref().ok_or_else(|| {
        CtlError::invalid("a resolved experiment without a runtime binding cannot be verified")
    })?;
    let topics = runtime.topics.get(subject).ok_or_else(|| {
        CtlError::invalid(format!("the runtime binding names no topics for {subject}"))
    })?;
    let (topic, expected_records) = match phase {
        VerificationPhase::Measured => (&topics.measured, experiment.records),
        VerificationPhase::Warmup => (&topics.warmup, experiment.warmup_records),
    };
    let argv = verify_argv(
        &tools.verify,
        &runtime.bootstrap,
        topic,
        &runtime.run_id,
        expected_records,
        experiment.payload.bytes,
        experiment.cluster.partitions,
    );
    let report_path = paths.verification_json(subject, phase.as_str());
    let spec = ToolSpec::new(
        argv,
        Duration::from_secs(experiment.budget.tool_timeout_seconds),
    )
    .with_stdout_file(report_path.clone())
    .with_stderr_file(stderr_path(paths, subject, phase));
    let outcome = run(&spec, interrupt)?;
    Ok(conclude(
        &outcome,
        &report_path,
        topic,
        expected_records,
        experiment,
    ))
}

/// Reads the written report and turns the whole run into a verdict.
fn conclude(
    outcome: &SupervisedRun,
    report_path: &std::path::Path,
    topic: &str,
    expected_records: u64,
    experiment: &ResolvedExperiment,
) -> VerificationRun {
    let report = std::fs::read(report_path)
        .ok()
        .and_then(|bytes| VerifierReport::from_slice(&bytes).ok());
    let satisfies_contract = outcome.succeeded()
        && report.as_ref().is_some_and(|report| {
            report.satisfies_contract(
                topic,
                expected_records,
                u64::from(experiment.cluster.partitions),
            )
        });
    let detail = if outcome.succeeded() {
        match &report {
            Some(_) if satisfies_contract => format!("{topic} verified"),
            Some(_) => format!("{topic} did not satisfy the verification contract"),
            None => format!("{topic}: the verifier wrote no readable report"),
        }
    } else {
        format!("{topic}: the verifier {}", outcome.describe())
    };
    VerificationRun {
        outcome: VerificationOutcome {
            ran: true,
            valid: report.as_ref().and_then(|report| report.valid),
            exit_code: outcome.exit.exit_code,
        },
        tool_succeeded: outcome.succeeded(),
        satisfies_contract,
        detail,
    }
}

/// `verification/<subject>-<phase>.stderr.log`.
fn stderr_path(paths: &AttemptPaths, subject: &str, phase: VerificationPhase) -> PathBuf {
    paths
        .verification_dir()
        .join(format!("{subject}-{}.stderr.log", phase.as_str()))
}
