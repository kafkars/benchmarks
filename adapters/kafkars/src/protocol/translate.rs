//! Where a resolved experiment becomes the legacy argument structs.
//!
//! The protocol verbs and the legacy positional commands run the same phase
//! code over the same argument types; this is the only place the document is
//! turned into them, so the two surfaces cannot drift into measuring different
//! workloads under one name.

use std::{error::Error, path::Path};

use bench_schema::ResolvedExperiment;

use crate::arguments::{FixedProduceArgs, ProduceArgs};

use super::documents::latency_path;

/// Maps a resolved experiment onto the legacy closed-loop argument struct.
pub(crate) fn produce_arguments(
    experiment: &ResolvedExperiment,
    subject: &str,
    output: &Path,
) -> Result<ProduceArgs, Box<dyn Error>> {
    let runtime = experiment
        .runtime
        .as_ref()
        .ok_or("the resolved experiment carries no runtime binding")?;
    let topics = runtime
        .topics
        .get(subject)
        .ok_or_else(|| format!("the resolved experiment has no topics for subject {subject:?}"))?;
    Ok(ProduceArgs {
        bootstrap: runtime.bootstrap.clone(),
        warmup_topic: topics.warmup.clone(),
        topic: topics.measured.clone(),
        run_id: runtime.run_id.clone(),
        warmup_records: usize::try_from(experiment.warmup_records)?,
        records: usize::try_from(experiment.records)?,
        payload_bytes: usize::try_from(experiment.payload.bytes)?,
        partitions: i32::try_from(experiment.cluster.partitions)?,
        max_outstanding: usize::try_from(experiment.application.max_outstanding_records)?,
        latency_path: latency_path(output),
    })
}

/// Maps a resolved experiment onto the legacy fixed-rate argument struct.
pub(crate) fn fixed_arguments(
    experiment: &ResolvedExperiment,
    subject: &str,
    output: &Path,
) -> Result<FixedProduceArgs, Box<dyn Error>> {
    let common = produce_arguments(experiment, subject, output)?;
    let offered_records_per_second = experiment
        .offered_records_per_second
        .ok_or("a fixed-rate experiment states no offered rate")?;
    Ok(FixedProduceArgs {
        common,
        offered_records_per_second,
        callers: usize::try_from(experiment.application.callers_per_producer)?,
    })
}
