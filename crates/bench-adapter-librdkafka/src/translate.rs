//! Resolved experiment → the exact positional argument vector the unmodified C
//! program already parses.
//!
//! `adapters/librdkafka-c/config.c` accepts two shapes and nothing else:
//!
//! ```text
//! <binary> bootstrap warmup-topic topic run-id warmup-records records
//!          payload-bytes partitions max-outstanding latency.csv
//!          client-metrics.jsonl
//!
//! <binary> --fixed-rate bootstrap warmup-topic topic run-id warmup-records
//!          records payload-bytes partitions max-outstanding offered-rate
//!          callers latency.csv client-metrics.jsonl
//! ```
//!
//! Both are strictly positional and length-checked (`argc != 12` and
//! `argc != 15` are hard refusals), so this module is where a resolved
//! experiment stops being a document and becomes eleven or fourteen strings in
//! a fixed order. It is pure and golden-tested: the argv is the entire
//! interface to the reference client, and a silent reordering would compare two
//! different workloads while reporting that it compared one.
//!
//! The two trailing paths are the C program's side outputs, and they are placed
//! inside the control plane's output directory so that everything the run
//! produced is sealed together: `latency.csv` is the per-record evidence, and
//! `client-metrics.jsonl` is the librdkafka statistics stream.

use std::path::Path;

use bench_schema::{LoadMode, ResolvedExperiment, RuntimeBinding, SubjectSpec, TopicPair};

use crate::describe::ADAPTER_NAME;

/// Per-record latency evidence, written by the C program into the output
/// directory.
pub(crate) const LATENCY_FILE: &str = "latency.csv";

/// librdkafka's own statistics stream, one JSON object per interval.
pub(crate) const STATISTICS_FILE: &str = "client-metrics.jsonl";

/// The result document the C program prints on stdout.
pub(crate) const RESULT_FILE: &str = "result.json";

/// The adapter's own terminal status document.
pub(crate) const STATUS_FILE: &str = "status.json";

/// Builds the child argument vector for one subject of one experiment.
///
/// The returned vector excludes the program itself: the caller owns the binary
/// path, because the shim is told which binary to drive.
pub(crate) fn arguments(
    experiment: &ResolvedExperiment,
    subject: &str,
    output: &Path,
) -> Result<Vec<String>, String> {
    let runtime = experiment
        .runtime
        .as_ref()
        .ok_or_else(|| "the resolved experiment carries no runtime binding".to_owned())?;
    let topics = runtime
        .topics
        .get(subject)
        .ok_or_else(|| format!("the resolved experiment has no topics for subject {subject:?}"))?;
    let mut argv = Vec::with_capacity(14);
    if experiment.load_mode == LoadMode::ScheduledOpenLoopFixedRate {
        argv.push("--fixed-rate".to_owned());
    }
    argv.extend(common_arguments(experiment, runtime, topics));
    if experiment.load_mode == LoadMode::ScheduledOpenLoopFixedRate {
        let rate = experiment
            .offered_records_per_second
            .ok_or_else(|| "a fixed-rate experiment states no offered rate".to_owned())?;
        argv.push(rate.to_string());
        argv.push(experiment.application.callers_per_producer.to_string());
    }
    argv.push(path_argument(&output.join(LATENCY_FILE))?);
    argv.push(path_argument(&output.join(STATISTICS_FILE))?);
    Ok(argv)
}

/// The nine arguments both shapes share, in the order `config.c` reads them.
fn common_arguments(
    experiment: &ResolvedExperiment,
    runtime: &RuntimeBinding,
    topics: &TopicPair,
) -> Vec<String> {
    vec![
        runtime.bootstrap.clone(),
        topics.warmup.clone(),
        topics.measured.clone(),
        runtime.run_id.clone(),
        experiment.warmup_records.to_string(),
        experiment.records.to_string(),
        experiment.payload.bytes.to_string(),
        experiment.cluster.partitions.to_string(),
        experiment.application.max_outstanding_records.to_string(),
    ]
}

/// Renders a path as an argument, refusing one the C program could not read.
///
/// `config.c` rejects any argument containing a control character, a quote, or
/// a backslash, because it splices them into JSON by hand. A path that would be
/// rejected there is caught here, where the message can say which path it was.
fn path_argument(path: &Path) -> Result<String, String> {
    let text = path
        .to_str()
        .ok_or_else(|| format!("{} is not valid UTF-8", path.display()))?;
    if text.is_empty()
        || text
            .chars()
            .any(|character| character < ' ' || character == '"' || character == '\\')
    {
        return Err(format!(
            "{text:?} cannot be passed to the C adapter, which refuses quotes, \
             backslashes, and control characters in its arguments"
        ));
    }
    Ok(text.to_owned())
}

/// Decides which subject of the experiment this process is running as.
///
/// The control plane's output directory is `adapters/<subject>/`, so its last
/// component names the subject. When there is no output directory, or its name
/// is not a subject, the fallback is the unique subject whose adapter is this
/// one — and an ambiguous experiment is refused rather than guessed at, because
/// picking the wrong subject would produce to another subject's topics.
pub(crate) fn subject_of<'a>(
    experiment: &'a ResolvedExperiment,
    output: Option<&Path>,
) -> Result<&'a SubjectSpec, String> {
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
        )),
        _ => Err(format!(
            "the experiment has no subject this adapter could be: no {ADAPTER_NAME} \
             subject, and no output directory naming one"
        )),
    }
}
