//! Topic lifecycle through the configured tool command: building the legacy
//! `topics-create`/`topics-delete` argv from the resolved experiment and
//! running it under supervision; cleanup is best-effort and recorded.
//!
//! Topic management is deliberately outside the adapter protocol. A client that
//! creates the topic it is about to be measured on is a client that can choose
//! its own partition count, and a comparison in which each subject chose its own
//! geometry is not a comparison. The control plane creates every topic for every
//! subject in one call, before any subject runs, so that no subject can be
//! advantaged by fresher metadata than another.
//!
//! The argv shape is the legacy one, preserved exactly:
//! `<prefix...> <bootstrap> <partitions> <replication-factor> <topic...>` for
//! creation and `<prefix...> <bootstrap> <topic...>` for deletion. The prefix
//! comes from the cluster profile, so no adapter name is compiled in here.
//!
//! Cleanup is best-effort by design. A failed delete leaves topics behind on a
//! cluster the caller owns, which is untidy; a failed delete that changed the
//! attempt's execution status would let a housekeeping problem masquerade as a
//! measurement problem. The failure is recorded as a phase and nothing else.

use std::path::PathBuf;
use std::time::Duration;

use bench_schema::{ClusterTools, ResolvedExperiment};

use crate::attempt::AttemptPaths;
use crate::error::{CtlError, CtlResult};
use crate::interrupt::InterruptFlag;
use crate::supervise::{SupervisedRun, ToolSpec, run};

/// Every topic one attempt owns: warmup then measured, per subject, in
/// execution order.
///
/// The order matters only for reproducibility of the tool invocation; the
/// legacy script listed warmup before measured for each adapter and this
/// mirrors it, so a bundle from either harness shows the same command.
#[must_use]
pub fn attempt_topics(experiment: &ResolvedExperiment) -> Vec<String> {
    let Some(runtime) = &experiment.runtime else {
        return Vec::new();
    };
    let mut topics = Vec::with_capacity(runtime.execution_order.len() * 2);
    for subject in &runtime.execution_order {
        if let Some(pair) = runtime.topics.get(subject) {
            topics.push(pair.warmup.clone());
            topics.push(pair.measured.clone());
        }
    }
    topics
}

/// Builds the legacy topic-creation argv from a configured prefix.
#[must_use]
pub fn create_argv(
    prefix: &[String],
    bootstrap: &str,
    partitions: u32,
    replication_factor: u32,
    topics: &[String],
) -> Vec<String> {
    let mut argv = prefix.to_vec();
    argv.push(bootstrap.to_owned());
    argv.push(partitions.to_string());
    argv.push(replication_factor.to_string());
    argv.extend(topics.iter().cloned());
    argv
}

/// Builds the legacy topic-deletion argv from a configured prefix.
#[must_use]
pub fn delete_argv(prefix: &[String], bootstrap: &str, topics: &[String]) -> Vec<String> {
    let mut argv = prefix.to_vec();
    argv.push(bootstrap.to_owned());
    argv.extend(topics.iter().cloned());
    argv
}

/// Creates every topic the attempt needs, in one invocation.
///
/// # Errors
///
/// Returns an error only when the tool could not be spawned or reaped. A tool
/// that ran and failed is a successful supervision reporting a non-zero exit.
pub fn create(
    paths: &AttemptPaths,
    tools: &ClusterTools,
    experiment: &ResolvedExperiment,
    interrupt: &InterruptFlag,
) -> CtlResult<SupervisedRun> {
    let bootstrap = bootstrap_of(experiment)?;
    let argv = create_argv(
        &tools.topic_create,
        bootstrap,
        experiment.cluster.partitions,
        experiment.cluster.replication_factor,
        &attempt_topics(experiment),
    );
    run(&spec(paths, argv, "topic-create", experiment), interrupt)
}

/// Deletes every topic the attempt created, best effort.
///
/// # Errors
///
/// As for [`create`]: only a spawn or reap failure is an error here.
pub fn delete(
    paths: &AttemptPaths,
    tools: &ClusterTools,
    experiment: &ResolvedExperiment,
    interrupt: &InterruptFlag,
) -> CtlResult<SupervisedRun> {
    let bootstrap = bootstrap_of(experiment)?;
    let argv = delete_argv(&tools.topic_delete, bootstrap, &attempt_topics(experiment));
    run(&spec(paths, argv, "topic-cleanup", experiment), interrupt)
}

/// Returns the bootstrap servers the runtime binding named.
fn bootstrap_of(experiment: &ResolvedExperiment) -> CtlResult<&str> {
    experiment
        .runtime
        .as_ref()
        .map(|runtime| runtime.bootstrap.as_str())
        .ok_or_else(|| {
            CtlError::invalid("a resolved experiment without a runtime binding cannot be run")
        })
}

/// Builds the supervision spec for a topic tool, logging beside the bundle root
/// under the legacy names.
fn spec(
    paths: &AttemptPaths,
    argv: Vec<String>,
    label: &str,
    experiment: &ResolvedExperiment,
) -> ToolSpec {
    ToolSpec::new(
        argv,
        Duration::from_secs(experiment.budget.tool_timeout_seconds),
    )
    .with_stdout_file(log_path(paths, label, "stdout"))
    .with_stderr_file(log_path(paths, label, "stderr"))
}

/// `<bundle>/<label>.<stream>.log`.
fn log_path(paths: &AttemptPaths, label: &str, stream: &str) -> PathBuf {
    paths.root().join(format!("{label}.{stream}.log"))
}
