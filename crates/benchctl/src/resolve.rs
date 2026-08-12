//! Experiment resolution: source TOML plus cluster profile plus subject
//! describe/validate results, deterministically merged into one canonical
//! `kafkars.experiment.v1` document and its subjects lock. The pure core is
//! separated from process-spawning so goldens can pin its bytes.
//!
//! # Purity
//!
//! Nothing here spawns a process, reads a clock, or touches the filesystem.
//! Every fact the resolution needs — what each adapter said about itself, what
//! attempt this is, which bootstrap to bind to — arrives as an argument. That
//! is what makes [`resolve_pure`] golden-testable: the same inputs produce the
//! same bytes forever, and a change to those bytes is a change to what "the
//! same experiment" means.
//!
//! # The run-id circularity, and how it is broken
//!
//! The run id is derived from the experiment id, and the experiment id is the
//! digest of the resolved document — which carries the run id. Read naively
//! that cannot terminate. It does terminate because the identity deliberately
//! excludes two things: the whole `runtime` block, where the run id lives, and
//! every subject's `command`. So the resolution runs in two passes:
//!
//! 1. build the document with `runtime: None` and take its experiment id;
//! 2. derive `run_id` from that id and the attempt id, build the runtime
//!    binding, and attach it.
//!
//! Attaching the binding cannot change the id, and a test asserts exactly that:
//! two attempts of one scenario share an experiment id and differ in run id.
//! That is the property the whole evidence model rests on — an experiment id
//! aggregates repetitions, a run id separates the records they wrote.
//!
//! # Refusals
//!
//! Resolution is where an incoherent request stops, before any topic is
//! created. A capacity search has no implementation in this milestone and is
//! refused rather than approximated. A cluster profile that contradicts the
//! scenario it is being used with — a different broker count, a different
//! transport — is refused rather than silently overriding the scenario, because
//! the scenario is the reviewed document and the cluster shape is part of the
//! experiment identity. A subject whose adapter declined the experiment is
//! refused with the adapter's own reasons, because running the comparison
//! without it would quietly answer a different question.

use std::collections::BTreeMap;

use bench_schema::{
    AdapterDescription, ApplicationSpec, ArrivalModel, BudgetSpec, ClusterProfile, ClusterSpec,
    ExperimentKind, LoadMode, PayloadSpec, ProducerSpec, ResolvedExperiment, RuntimeBinding,
    SloSpec, SourceExperiment, SubjectEntry, SubjectLockEntry, SubjectSpec, SubjectsLock,
    TopicPair, ValidateReport, experiment_id, sha256_hex,
};

use crate::error::{CtlError, CtlResult};

/// Prefix every topic this repository creates starts with.
pub const TOPIC_PREFIX: &str = "kfb";

/// Suffix distinguishing a subject's warmup topic from its measured topic.
pub const WARMUP_TOPIC_SUFFIX: &str = "-warmup";

/// Characters of the run id, which is the head of a sha-256 digest.
pub const RUN_ID_LENGTH: usize = 16;

/// Produce requests in flight per broker when a scenario does not say.
///
/// Five is what both adapters were configured with when the legacy scenarios
/// were written, recorded in their `native_request_concurrency` prose. Making
/// it explicit here means the resolved document never leaves a matched setting
/// implicit.
pub const DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_BROKER: u32 = 5;

/// One subject as the control plane found it: what the operator asked for, and
/// what the adapter said when asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectProbe {
    /// The subjects-file entry verbatim: name and argument vector.
    pub subject: SubjectEntry,
    /// The capability document the adapter printed for `describe`.
    pub describe: AdapterDescription,
    /// The adapter's verdict on this experiment.
    ///
    /// `None` means the subject has not been asked yet. That state is real:
    /// `validate` takes the resolved experiment as a file, so the document has
    /// to exist before the answer does. [`resolve_experiment`] accepts it;
    /// [`resolve_pure`] does not, because a lock without a verdict would record
    /// that nobody checked.
    pub validate: Option<ValidateReport>,
    /// Digest of the program file, when it could be read.
    pub binary_sha256: Option<String>,
}

/// The attempt-specific facts the identity ignores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeInputs {
    /// Bootstrap servers the subjects will connect to.
    pub bootstrap: String,
    /// Attempt id, which together with the experiment id fixes the run id.
    pub attempt_id: String,
    /// Execution order override, subject names in the order the operator asked
    /// for. `None` keeps the subjects-file order.
    pub order: Option<Vec<String>>,
}

/// Everything [`resolve_pure`] needs, and nothing it does not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveInputs {
    /// The scenario as authored.
    pub source: SourceExperiment,
    /// The cluster profile the scenario is being run against.
    pub cluster: ClusterProfile,
    /// Subjects in subjects-file order, each with what it said about itself.
    pub subjects: Vec<SubjectProbe>,
    /// Seed override; `None` keeps the scenario's payload seed.
    pub seed: Option<u64>,
    /// Ceilings the attempt declares for itself.
    pub budget: BudgetSpec,
    /// Attempt binding.
    pub runtime: RuntimeInputs,
}

/// Resolves a scenario into the document that will be run and hashed.
///
/// This is the first of the two passes described in the module contract: it
/// needs the describe documents (for adapter names and versions) but not the
/// validate reports, so its output is what gets handed to `validate` in the
/// first place.
///
/// # Errors
///
/// Returns an invalid-experiment error when the scenario, the cluster profile,
/// the subject list, or the requested order cannot make a coherent experiment.
pub fn resolve_experiment(inputs: &ResolveInputs) -> CtlResult<ResolvedExperiment> {
    let unbound = unbound_experiment(inputs)?;
    let identity = experiment_id(&unbound)?;
    let run_id = derive_run_id(identity.as_str(), &inputs.runtime.attempt_id);
    let binding = runtime_binding(inputs, &unbound, &run_id)?;
    let bound = ResolvedExperiment {
        runtime: Some(binding),
        ..unbound
    };
    bound.validate()?;
    Ok(bound)
}

/// Resolves a scenario and locks what every subject was when it was asked.
///
/// # Errors
///
/// Returns an invalid-experiment error for everything
/// [`resolve_experiment`] refuses, plus a subject that was never asked to
/// validate and a subject whose adapter declined the experiment.
pub fn resolve_pure(inputs: &ResolveInputs) -> CtlResult<(ResolvedExperiment, SubjectsLock)> {
    let resolved = resolve_experiment(inputs)?;
    let mut entries = Vec::with_capacity(inputs.subjects.len());
    let mut declined = Vec::new();
    for probe in &inputs.subjects {
        let Some(report) = probe.validate.clone() else {
            return Err(CtlError::invalid(format!(
                "subject {:?} was never asked to validate this experiment",
                probe.subject.name
            )));
        };
        if !report.supported {
            let reasons = if report.reasons.is_empty() {
                "no reason given".to_owned()
            } else {
                report.reasons.join("; ")
            };
            declined.push(format!("{}: {reasons}", probe.subject.name));
        }
        entries.push(SubjectLockEntry {
            name: probe.subject.name.clone(),
            command: probe.subject.command.clone(),
            binary_sha256: probe.binary_sha256.clone(),
            describe: probe.describe.clone(),
            validate: report,
        });
    }
    if !declined.is_empty() {
        return Err(CtlError::invalid(format!(
            "the experiment was declined by {}",
            declined.join(" | ")
        )));
    }
    Ok((resolved, SubjectsLock::new(entries)))
}

/// Returns the run id for an attempt: the head of the digest of the experiment
/// id followed by the attempt id.
///
/// Both halves matter. The experiment id makes the run id specific to what was
/// measured, and the attempt id makes it specific to *this* measurement, so two
/// repetitions of one experiment never write records the verifier could confuse
/// with each other.
#[must_use]
pub fn derive_run_id(experiment_id_text: &str, attempt_id_text: &str) -> String {
    let mut seed = String::with_capacity(experiment_id_text.len() + attempt_id_text.len());
    seed.push_str(experiment_id_text);
    seed.push_str(attempt_id_text);
    sha256_hex(seed.as_bytes())[..RUN_ID_LENGTH].to_owned()
}

/// Builds the resolved document without its runtime binding.
fn unbound_experiment(inputs: &ResolveInputs) -> CtlResult<ResolvedExperiment> {
    let source = &inputs.source;
    let load_mode = source.load_mode.resolved()?;
    let records = source.records.ok_or_else(|| {
        CtlError::invalid("the scenario states no record count, so it measures nothing")
    })?;
    let (offered, arrival) = offered_rate(source, load_mode)?;
    check_cluster_profile(inputs)?;
    let experiment = ResolvedExperiment {
        schema: ResolvedExperiment::SCHEMA.to_owned(),
        name: source.name.clone(),
        kind: ExperimentKind::Producer,
        profile: source.status.clone(),
        claim_eligible: source.claim_eligible,
        load_mode,
        records,
        warmup_records: source.warmup_records.unwrap_or(0),
        offered_records_per_second: offered,
        arrival,
        seed: inputs.seed.unwrap_or(source.payload.seed),
        application: ApplicationSpec {
            producer_instances: source.application.producer_instances,
            callers_per_producer: source.application.callers_per_producer,
            backpressure: source.application.backpressure.clone(),
            queue_bytes: source.application.queue_bytes,
            max_outstanding_records: source.application.max_outstanding_records,
            admission_shape: source.application_api.admission_shape.clone(),
            completion_shape: source.application_api.completion_shape.clone(),
            batch_records: source.application_api.batch_records,
        },
        payload: PayloadSpec {
            bytes: source.payload.bytes,
            profile: source.payload.profile.clone(),
            identity: source.payload.identity.clone(),
        },
        producer: Some(ProducerSpec {
            acks: source.producer.acks.clone(),
            idempotence: source.producer.idempotence,
            compression: source.producer.compression.clone(),
            linger_ms: source.producer.linger_ms,
            batch_records: source.producer.batch_records,
            batch_bytes: source.producer.batch_bytes,
            request_bytes: source.producer.request_bytes,
            delivery_timeout_ms: source.producer.delivery_timeout_ms,
            partitioning: source.producer.partitioning.clone(),
            max_in_flight_requests_per_broker: source
                .producer
                .max_in_flight_requests_per_broker
                .unwrap_or(DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_BROKER),
            retry_max_replacements: source.producer.retry_max_replacements,
            retry_backoff_ms: source.producer.retry_backoff_ms,
            warmup_partitioning: source.producer.warmup_partitioning.clone(),
            warmup_serialized_partition_primer_records: source
                .producer
                .warmup_serialized_partition_primer_records,
        }),
        budget: inputs.budget,
        cluster: ClusterSpec {
            brokers: source.cluster.brokers,
            partitions: source.cluster.partitions,
            replication_factor: source.cluster.replication_factor,
            min_in_sync_replicas: source.cluster.min_in_sync_replicas,
            security: source.cluster.security.clone(),
            unclean_leader_election: source.cluster.unclean_leader_election.unwrap_or(false),
        },
        slo: slo_spec(source),
        subjects: subjects(inputs)?,
        runtime: None,
    };
    experiment.validate()?;
    Ok(experiment)
}

/// Returns the offered rate and arrival model the load mode requires.
fn offered_rate(
    source: &SourceExperiment,
    load_mode: LoadMode,
) -> CtlResult<(Option<u64>, Option<ArrivalModel>)> {
    match load_mode {
        LoadMode::ScheduledOpenLoopFixedRate => {
            let rate = source.offered_records_per_second.ok_or_else(|| {
                CtlError::invalid(
                    "a fixed-rate scenario must state offered_records_per_second".to_owned(),
                )
            })?;
            Ok((Some(rate), Some(ArrivalModel::Deterministic)))
        }
        LoadMode::ClosedLoop => {
            if source.offered_records_per_second.is_some() {
                return Err(CtlError::invalid(
                    "a closed-loop scenario states an offered rate it cannot honour; \
                     the client's own capacity sets the rate",
                ));
            }
            Ok((None, None))
        }
    }
}

/// Copies the scenario's objectives, defaulting to "none declared".
///
/// Public because the capacity search judges probes against exactly these
/// objectives and must read them before it resolves anything: a search with no
/// objective to search against is refused before a topic is created.
#[must_use]
pub fn slo_spec(source: &SourceExperiment) -> SloSpec {
    source.slo.map_or_else(SloSpec::default, |slo| SloSpec {
        corrected_p99_ms: slo.corrected_p99_ms,
        schedule_delay_p99_ms: slo.schedule_delay_p99_ms,
        drain_tail_ms: slo.drain_tail_ms,
        queue_slope_percent: slo.queue_slope_percent,
        queue_slope_floor_records_per_second: slo.queue_slope_floor_records_per_second,
        minimum_queue_samples: slo.minimum_queue_samples,
        native_retries: slo.native_retries,
        native_timeouts: slo.native_timeouts,
    })
}

/// Refuses a cluster profile that contradicts the scenario it is bound to.
fn check_cluster_profile(inputs: &ResolveInputs) -> CtlResult<()> {
    let profile = &inputs.cluster;
    let scenario = &inputs.source.cluster;
    if let Some(brokers) = profile.brokers
        && brokers != scenario.brokers
    {
        return Err(CtlError::invalid(format!(
            "cluster profile {:?} has {brokers} brokers but the scenario requires {}",
            profile.name, scenario.brokers
        )));
    }
    if let Some(security) = &profile.security
        && security != &scenario.security
    {
        return Err(CtlError::invalid(format!(
            "cluster profile {:?} offers {security:?} transport but the scenario requires {:?}",
            profile.name, scenario.security
        )));
    }
    Ok(())
}

/// Turns probed subjects into subject specifications, in subjects-file order.
fn subjects(inputs: &ResolveInputs) -> CtlResult<Vec<SubjectSpec>> {
    if inputs.subjects.is_empty() {
        return Err(CtlError::invalid(
            "the subjects file names nothing to measure",
        ));
    }
    Ok(inputs
        .subjects
        .iter()
        .map(|probe| SubjectSpec {
            name: probe.subject.name.clone(),
            adapter_name: probe.describe.name.clone(),
            adapter_version: probe.describe.version.clone(),
            command: probe.subject.command.clone(),
            // Carried, not invented: the role is the operator's statement about
            // what the subject is for, and it participates in the experiment id.
            role: probe.subject.role.clone(),
        })
        .collect())
}

/// Builds the runtime binding: bootstrap, run id, topics, execution order.
fn runtime_binding(
    inputs: &ResolveInputs,
    experiment: &ResolvedExperiment,
    run_id: &str,
) -> CtlResult<RuntimeBinding> {
    let bootstrap = inputs.runtime.bootstrap.trim().to_owned();
    if bootstrap.is_empty() {
        return Err(CtlError::invalid(
            "the attempt names no bootstrap servers to bind to",
        ));
    }
    let topic_prefix = format!("{TOPIC_PREFIX}-{run_id}");
    let mut topics = BTreeMap::new();
    for subject in &experiment.subjects {
        let measured = format!("{topic_prefix}-{}", subject.name);
        let warmup = format!("{measured}{WARMUP_TOPIC_SUFFIX}");
        topics.insert(subject.name.clone(), TopicPair { measured, warmup });
    }
    Ok(RuntimeBinding {
        bootstrap,
        run_id: run_id.to_owned(),
        topic_prefix,
        topics,
        execution_order: execution_order(inputs, experiment)?,
    })
}

/// Returns the order subjects run in: the requested one, verbatim, or the
/// subjects-file order.
fn execution_order(
    inputs: &ResolveInputs,
    experiment: &ResolvedExperiment,
) -> CtlResult<Vec<String>> {
    let declared: Vec<String> = experiment
        .subjects
        .iter()
        .map(|subject| subject.name.clone())
        .collect();
    let Some(requested) = &inputs.runtime.order else {
        return Ok(declared);
    };
    let mut remaining = declared.clone();
    for name in requested {
        let Some(position) = remaining.iter().position(|subject| subject == name) else {
            return Err(CtlError::invalid(format!(
                "the requested execution order names {name:?}, which is not a subject, \
                 or names it twice"
            )));
        };
        remaining.remove(position);
    }
    if !remaining.is_empty() {
        return Err(CtlError::invalid(format!(
            "the requested execution order leaves out {}",
            remaining.join(", ")
        )));
    }
    Ok(requested.clone())
}
