//! The field mapping: scenario and cluster profile into the resolved document
//! that gets hashed.
//!
//! # Refusals
//!
//! Resolution is where an incoherent request stops, before any topic is
//! created. A capacity search has no implementation in this milestone and is
//! refused rather than approximated. A cluster profile that contradicts the
//! scenario it is being used with — a different broker count, a different
//! transport — is refused rather than silently overriding the scenario, because
//! the scenario is the reviewed document and the cluster shape is part of the
//! experiment identity.

use bench_schema::{
    ApplicationSpec, ArrivalModel, ClusterSpec, ExperimentKind, LoadMode, PayloadSpec,
    ProducerSpec, ResolvedExperiment, SloSpec, SourceExperiment,
};

use crate::error::{CtlError, CtlResult};

use super::inputs::ResolveInputs;
use super::subjects::subjects;

/// Produce requests in flight per broker when a scenario does not say.
///
/// Five is what both adapters were configured with when the legacy scenarios
/// were written, recorded in their `native_request_concurrency` prose. Making
/// it explicit here means the resolved document never leaves a matched setting
/// implicit.
pub const DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_BROKER: u32 = 5;

/// Builds the resolved document without its runtime binding.
pub(super) fn unbound_experiment(inputs: &ResolveInputs) -> CtlResult<ResolvedExperiment> {
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
