//! Strict command parsing for reproducible benchmark adapter processes.
//!
//! Two command surfaces live here and neither may drift from the other. The
//! legacy positional commands are what the migrated Node control plane drives,
//! and their stdout bytes are evidence that has already been sealed, so they
//! are preserved exactly. The three adapter-protocol verbs — `describe`,
//! `validate`, `run` — take a resolved experiment document instead, and reach
//! the same phase code through `protocol`.

use std::{error::Error, ffi::OsString, path::PathBuf};

use crate::{histogram_vector, payload, producer, protocol, schedule, topics};

pub(crate) const RUN_ID_BYTES: usize = 16;
pub(crate) const MIN_PAYLOAD_BYTES: usize = 64;

pub fn run<I>(arguments: I) -> Result<(), Box<dyn Error>>
where
    I: IntoIterator<Item = OsString>,
{
    let arguments = arguments
        .into_iter()
        .map(|value| value.into_string().map_err(|_| "arguments must be UTF-8"))
        .collect::<Result<Vec<_>, _>>()?;
    let Some((command, tail)) = arguments.split_first() else {
        return Err(usage().into());
    };

    match command.as_str() {
        "payload" => emit_payload(tail),
        "schedule" => emit_schedule(tail),
        "histogram-vector" => emit_histogram_vector(tail),
        "produce" => emit_report(&producer::run(&parse_produce(tail)?)?),
        "produce-fixed" => emit_report(&producer::run_fixed(&parse_fixed_produce(tail)?)?),
        "topics-create" => topics::create(&parse_topics(tail)?),
        "topics-delete" => topics::delete(&parse_topic_deletion(tail)?),
        "describe" => describe(tail),
        "validate" => protocol::validate(&parse_experiment(tail)?),
        "run" => {
            let (experiment, output) = parse_run(tail)?;
            protocol::run(&experiment, &output)
        }
        _ => Err(usage().into()),
    }
}

/// Prints a completed phase exactly as this adapter always has: the report as
/// one JSON line on stdout, then a non-zero exit when the phase was not valid.
fn emit_report(outcome: &producer::RunOutcome) -> Result<(), Box<dyn Error>> {
    println!("{}", outcome.json);
    if outcome.valid {
        Ok(())
    } else {
        Err(outcome.invalid_reason.into())
    }
}

/// `describe [--json]`: the capability document.
fn describe(values: &[String]) -> Result<(), Box<dyn Error>> {
    match values {
        [] => {}
        [only] if only == "--json" => {}
        _ => return Err(usage().into()),
    }
    protocol::describe()
}

/// `validate --experiment <resolved.json>`.
fn parse_experiment(values: &[String]) -> Result<PathBuf, Box<dyn Error>> {
    match values {
        [flag, path] if flag == "--experiment" && !path.is_empty() => Ok(PathBuf::from(path)),
        _ => Err(usage().into()),
    }
}

/// `run --experiment <resolved.json> --output <dir>`.
fn parse_run(values: &[String]) -> Result<(PathBuf, PathBuf), Box<dyn Error>> {
    match values {
        [experiment_flag, experiment, output_flag, output]
            if experiment_flag == "--experiment"
                && output_flag == "--output"
                && !experiment.is_empty()
                && !output.is_empty() =>
        {
            Ok((PathBuf::from(experiment), PathBuf::from(output)))
        }
        _ => Err(usage().into()),
    }
}

/// `histogram-vector`: whitespace-separated `u64` values on stdin, one
/// encoded histogram on stdout.
///
/// The values are read rather than named on the command line because the
/// committed conformance input is a file of them, and a vector whose input
/// lives in the argument parser would be a vector of the argument parser.
fn emit_histogram_vector(values: &[String]) -> Result<(), Box<dyn Error>> {
    if !values.is_empty() {
        return Err(usage().into());
    }
    let mut input = std::io::stdin().lock();
    let mut output = std::io::stdout().lock();
    histogram_vector::emit(&mut input, &mut output)
}

fn emit_schedule(values: &[String]) -> Result<(), Box<dyn Error>> {
    if values.len() != 4 {
        return Err(usage().into());
    }
    let rate = parse_u64("offered rate", &values[0], false)?;
    let records = parse_u64("records", &values[1], false)?;
    let batch_records = parse_u64("batch records", &values[2], false)?;
    let callers = parse_u64("callers", &values[3], false)?;
    println!("batch_index,caller,first_sequence,count,intended_ns");
    for batch in schedule::batches(rate, records, batch_records, callers)? {
        println!(
            "{},{},{},{},{}",
            batch.index, batch.caller, batch.first_sequence, batch.count, batch.intended_ns
        );
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct ProduceArgs {
    pub(crate) bootstrap: String,
    pub(crate) warmup_topic: String,
    pub(crate) topic: String,
    pub(crate) run_id: String,
    pub(crate) warmup_records: usize,
    pub(crate) records: usize,
    pub(crate) payload_bytes: usize,
    pub(crate) partitions: i32,
    pub(crate) max_outstanding: usize,
    pub(crate) latency_path: PathBuf,
}

#[derive(Debug, Clone)]
pub(crate) struct FixedProduceArgs {
    pub(crate) common: ProduceArgs,
    pub(crate) offered_records_per_second: u64,
    pub(crate) callers: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct TopicArgs {
    pub(crate) bootstrap: String,
    pub(crate) topics: Vec<String>,
    pub(crate) partitions: i32,
    pub(crate) replication_factor: i16,
}

#[derive(Debug, Clone)]
pub(crate) struct TopicDeletionArgs {
    pub(crate) bootstrap: String,
    pub(crate) topics: Vec<String>,
}

fn parse_produce(values: &[String]) -> Result<ProduceArgs, Box<dyn Error>> {
    if values.len() != 10 {
        return Err(usage().into());
    }
    validate_endpoint(&values[0])?;
    validate_topic(&values[1])?;
    validate_topic(&values[2])?;
    validate_run_id(&values[3])?;
    let args = ProduceArgs {
        bootstrap: values[0].clone(),
        warmup_topic: values[1].clone(),
        topic: values[2].clone(),
        run_id: values[3].clone(),
        warmup_records: parse_usize("warmup records", &values[4], true)?,
        records: parse_usize("records", &values[5], false)?,
        payload_bytes: parse_usize("payload bytes", &values[6], false)?,
        partitions: parse_i32("partitions", &values[7])?,
        max_outstanding: parse_usize("max outstanding", &values[8], false)?,
        latency_path: PathBuf::from(&values[9]),
    };
    validate_workload(args.payload_bytes, args.partitions)?;
    Ok(args)
}

fn parse_fixed_produce(values: &[String]) -> Result<FixedProduceArgs, Box<dyn Error>> {
    if values.len() != 12 {
        return Err(usage().into());
    }
    let mut common_values = values[..9].to_vec();
    common_values.push(values[11].clone());
    let common = parse_produce(&common_values)?;
    let offered_records_per_second = parse_u64("offered rate", &values[9], false)?;
    let callers = parse_usize("callers", &values[10], false)?;
    if callers != 4 {
        return Err("the fixed-load headline requires exactly four callers".into());
    }
    let _ = schedule::intended_offset_ns(
        u64::try_from(common.records - 1)?,
        offered_records_per_second,
    )?;
    Ok(FixedProduceArgs {
        common,
        offered_records_per_second,
        callers,
    })
}

fn parse_topics(values: &[String]) -> Result<TopicArgs, Box<dyn Error>> {
    if values.len() < 4 {
        return Err(usage().into());
    }
    validate_endpoint(&values[0])?;
    let partitions = parse_i32("partitions", &values[1])?;
    let replication_factor = values[2].parse::<i16>()?;
    if replication_factor <= 0 {
        return Err("replication factor must be positive".into());
    }
    let topics = values[3..].to_vec();
    for topic in &topics {
        validate_topic(topic)?;
    }
    Ok(TopicArgs {
        bootstrap: values[0].clone(),
        topics,
        partitions,
        replication_factor,
    })
}

fn parse_topic_deletion(values: &[String]) -> Result<TopicDeletionArgs, Box<dyn Error>> {
    if values.len() < 2 {
        return Err(usage().into());
    }
    validate_endpoint(&values[0])?;
    let topics = values[1..].to_vec();
    for topic in &topics {
        validate_topic(topic)?;
    }
    Ok(TopicDeletionArgs {
        bootstrap: values[0].clone(),
        topics,
    })
}

fn emit_payload(values: &[String]) -> Result<(), Box<dyn Error>> {
    if values.len() != 3 {
        return Err(usage().into());
    }
    validate_run_id(&values[0])?;
    let sequence = values[1].parse::<u64>()?;
    let bytes = parse_usize("payload bytes", &values[2], false)?;
    validate_workload(bytes, 1)?;
    println!(
        "{}",
        String::from_utf8(payload::make(&values[0], sequence, bytes))?
    );
    Ok(())
}

fn validate_endpoint(value: &str) -> Result<(), Box<dyn Error>> {
    if value.is_empty() || !value.contains(':') {
        return Err("bootstrap must contain at least one host:port endpoint".into());
    }
    Ok(())
}

fn validate_topic(value: &str) -> Result<(), Box<dyn Error>> {
    if value.is_empty()
        || value.len() > 249
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(format!("invalid benchmark topic {value:?}").into());
    }
    Ok(())
}

fn validate_run_id(value: &str) -> Result<(), Box<dyn Error>> {
    if value.len() != RUN_ID_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("run ID must be exactly 16 lowercase hexadecimal bytes".into());
    }
    Ok(())
}

fn validate_workload(payload_bytes: usize, partitions: i32) -> Result<(), Box<dyn Error>> {
    if payload_bytes < MIN_PAYLOAD_BYTES {
        return Err(format!("payload must be at least {MIN_PAYLOAD_BYTES} bytes").into());
    }
    if partitions <= 0 {
        return Err("partition count must be positive".into());
    }
    Ok(())
}

fn parse_usize(name: &str, value: &str, zero_allowed: bool) -> Result<usize, Box<dyn Error>> {
    let parsed = value.parse::<usize>()?;
    if !zero_allowed && parsed == 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(parsed)
}

fn parse_u64(name: &str, value: &str, zero_allowed: bool) -> Result<u64, Box<dyn Error>> {
    let parsed = value.parse::<u64>()?;
    if !zero_allowed && parsed == 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(parsed)
}

fn parse_i32(name: &str, value: &str) -> Result<i32, Box<dyn Error>> {
    let parsed = value.parse::<i32>()?;
    if parsed <= 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(parsed)
}

fn usage() -> &'static str {
    "usage: kafkars-benchmark-adapter \
     <payload|schedule|histogram-vector|produce|produce-fixed|topics-create|topics-delete\
     |describe|validate|run> ..."
}
