//! The `run` verb: the one path that produces a measurement, and the status
//! document it leaves behind on every way out.

use std::{error::Error, path::Path, time::SystemTime};

use bench_schema::{AdapterStatus, LoadMode, ProducerBenchmarkV2};

use crate::producer;

use super::{
    documents::{read_experiment, write_result, write_status},
    timestamp::{now, utc_rfc3339_millis},
    translate::{fixed_arguments, produce_arguments},
    verdict::{report, subject_of},
};

/// Runs one resolved experiment, writing every artefact into `output`.
///
/// A status document is written on every path out of this function, including
/// the ones that fail before the client is built. A missing status therefore
/// means the process was killed, which is a distinction the control plane
/// cannot make for itself.
pub(crate) fn run(experiment_path: &Path, output: &Path) -> Result<(), Box<dyn Error>> {
    let started_at = utc_rfc3339_millis(SystemTime::now());
    std::fs::create_dir_all(output)?;
    match execute(experiment_path, output) {
        Ok(document) if document.valid => {
            write_status(output, &AdapterStatus::succeeded(started_at, now()))?;
            Ok(())
        }
        Ok(document) => {
            let reason = document
                .invalid_reason
                .unwrap_or_else(|| "the measurement was declared invalid".to_owned());
            write_status(
                output,
                &AdapterStatus::failed("drain", reason.clone(), started_at, now()),
            )?;
            Err(reason.into())
        }
        Err(failure) => {
            let reason = failure.to_string();
            write_status(
                output,
                &AdapterStatus::failed("run", reason.clone(), started_at, now()),
            )?;
            Err(failure)
        }
    }
}

/// The measured path: parse, decline or accept, translate, produce, seal the
/// result document.
fn execute(experiment_path: &Path, output: &Path) -> Result<ProducerBenchmarkV2, Box<dyn Error>> {
    let experiment = read_experiment(experiment_path)?;
    let subject = subject_of(&experiment, Some(output))?.name.clone();
    let verdict = report(&experiment, Some(&subject));
    if !verdict.supported {
        return Err(format!(
            "this adapter declined the experiment: {}",
            verdict.reasons.join("; ")
        )
        .into());
    }
    let document = match experiment.load_mode {
        LoadMode::ClosedLoop => {
            producer::run_closed_loop_v2(&produce_arguments(&experiment, &subject, output)?)
        }
        LoadMode::ScheduledOpenLoopFixedRate => {
            producer::run_fixed_rate_v2(&fixed_arguments(&experiment, &subject, output)?)
        }
    }?;
    write_result(output, &document)?;
    Ok(document)
}
