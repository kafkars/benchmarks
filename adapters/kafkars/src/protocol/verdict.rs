//! Declining as a first-class answer: the `validate` verb, the report behind
//! it, and the question of which subject this process is.
//!
//! Everything in `producer.rs` above the argument surface is a constant, so an
//! experiment asking for anything else would be run as a different experiment.
//! Saying so in a validate report is the difference between a missing subject
//! somebody can explain and a comparison that quietly answers the wrong
//! question.

use std::{error::Error, path::Path};

use bench_schema::{
    ArrivalModel, ExperimentKind, LoadMode, ResolvedExperiment, SubjectSpec, ValidateReport,
};

use super::{
    describe::ADAPTER_NAME,
    documents::{print_document, read_experiment},
    settings::{QUEUE_BYTES, check_producer},
};

/// Smallest payload the identity envelope fits in.
const MIN_PAYLOAD_BYTES: u32 = 64;

/// Callers the closed-loop phase admits from.
const CLOSED_LOOP_CALLERS: u32 = 1;

/// Callers the fixed-rate phase requires, exactly.
const FIXED_RATE_CALLERS: u32 = 4;

/// Prints the verdict on one resolved experiment.
pub(crate) fn validate(experiment_path: &Path) -> Result<(), Box<dyn Error>> {
    let experiment = read_experiment(experiment_path)?;
    let subject = subject_of(&experiment, None).ok().map(|s| s.name.clone());
    print_document(&report(&experiment, subject.as_deref()))
}

/// Judges a resolved experiment against what this adapter configures.
pub(crate) fn report(experiment: &ResolvedExperiment, subject: Option<&str>) -> ValidateReport {
    let mut reasons = Vec::new();
    if let Err(reason) = experiment.validate() {
        return ValidateReport::unsupported(vec![format!(
            "the experiment is not coherent: {reason}"
        )]);
    }
    check_shape(experiment, &mut reasons);
    check_producer(experiment, &mut reasons);
    check_runtime(experiment, subject, &mut reasons);
    if reasons.is_empty() {
        ValidateReport::supported()
    } else {
        ValidateReport::unsupported(reasons)
    }
}

/// Checks the experiment's kind, load mode, and application shape.
fn check_shape(experiment: &ResolvedExperiment, reasons: &mut Vec<String>) {
    match experiment.kind {
        ExperimentKind::Producer => {}
    }
    if experiment.payload.bytes < MIN_PAYLOAD_BYTES {
        reasons.push(format!(
            "a record carries a {MIN_PAYLOAD_BYTES}-byte identity envelope, and the \
             experiment asks for {} payload bytes",
            experiment.payload.bytes
        ));
    }
    if i32::try_from(experiment.cluster.partitions).is_err() {
        reasons.push(format!(
            "partitions are addressed as a signed 32-bit integer, and the experiment asks \
             for {}",
            experiment.cluster.partitions
        ));
    }
    if experiment.application.producer_instances != 1 {
        reasons.push(format!(
            "this adapter builds exactly one producer handle, and the experiment asks for {}",
            experiment.application.producer_instances
        ));
    }
    if experiment.application.admission_shape != "public-batch" {
        reasons.push(format!(
            "this adapter admits records through the public batch surface, not {:?}",
            experiment.application.admission_shape
        ));
    }
    if experiment.application.completion_shape != "aggregate-batch-terminal" {
        reasons.push(format!(
            "this adapter waits on the aggregate terminal state of each admitted batch, \
             not {:?}",
            experiment.application.completion_shape
        ));
    }
    if experiment.application.queue_bytes != QUEUE_BYTES {
        reasons.push(format!(
            "this adapter retains {QUEUE_BYTES} client queue bytes, and the experiment \
             asks for {}",
            experiment.application.queue_bytes
        ));
    }
    match experiment.load_mode {
        LoadMode::ClosedLoop => {
            if experiment.application.callers_per_producer != CLOSED_LOOP_CALLERS {
                reasons.push(format!(
                    "the closed-loop phase admits from exactly {CLOSED_LOOP_CALLERS} caller, \
                     and the experiment asks for {}",
                    experiment.application.callers_per_producer
                ));
            }
        }
        LoadMode::ScheduledOpenLoopFixedRate => {
            if experiment.application.callers_per_producer != FIXED_RATE_CALLERS {
                reasons.push(format!(
                    "the fixed-load headline requires exactly {FIXED_RATE_CALLERS} callers, \
                     and the experiment asks for {}",
                    experiment.application.callers_per_producer
                ));
            }
            if experiment.arrival != Some(ArrivalModel::Deterministic) {
                reasons.push(
                    "this adapter schedules evenly spaced arrivals and implements no other \
                     arrival process"
                        .to_owned(),
                );
            }
        }
    }
}

/// Checks that the binding names topics this subject can produce to.
fn check_runtime(
    experiment: &ResolvedExperiment,
    subject: Option<&str>,
    reasons: &mut Vec<String>,
) {
    let Some(runtime) = experiment.runtime.as_ref() else {
        reasons.push(
            "the resolved experiment carries no runtime binding, so it names no topics and \
             no run id to produce under"
                .to_owned(),
        );
        return;
    };
    if let Some(subject) = subject
        && !runtime.topics.contains_key(subject)
    {
        reasons.push(format!(
            "the runtime binding names no topics for subject {subject:?}"
        ));
    }
}

/// Decides which subject of the experiment this process is running as.
pub(crate) fn subject_of<'a>(
    experiment: &'a ResolvedExperiment,
    output: Option<&Path>,
) -> Result<&'a SubjectSpec, Box<dyn Error>> {
    if let Some(name) = output
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        && let Some(subject) = experiment.subject(name)
    {
        return Ok(subject);
    }
    let mut ours = experiment
        .subjects
        .iter()
        .filter(|subject| subject.adapter_name == ADAPTER_NAME);
    match (ours.next(), ours.next()) {
        (Some(subject), None) => Ok(subject),
        (Some(_), Some(_)) => Err(format!(
            "the experiment has more than one {ADAPTER_NAME} subject and the output \
             directory does not say which one this is"
        )
        .into()),
        _ => Err(format!(
            "the experiment has no subject this adapter could be: no {ADAPTER_NAME} subject, \
             and no output directory naming one"
        )
        .into()),
    }
}
