//! The process entry point: one verb per line, and nothing else.
//!
//! Both command surfaces are dispatched here so that the list of verbs this
//! binary answers to is readable in one screen. Every arm delegates: parsing
//! belongs to the verb's own module, and running belongs to the phase code.

use std::{error::Error, ffi::OsString};

use crate::{producer, protocol, topics};

use super::{
    emit::{emit_histogram_vector, emit_payload, emit_schedule},
    experiment::{parse_experiment, parse_run},
    parse::usage,
    produce::{parse_fixed_produce, parse_produce},
    topic::{parse_topic_deletion, parse_topics},
};

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
