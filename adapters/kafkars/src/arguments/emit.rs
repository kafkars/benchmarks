//! The three verbs that print a deterministic artefact and touch no broker:
//! `payload`, `schedule`, and `histogram-vector`.
//!
//! Their bytes are the cross-language conformance vectors, so what these print
//! is a contract with the C adapter rather than a convenience for a person.

use std::error::Error;

use crate::{histogram_vector, payload, schedule};

use super::parse::{parse_u64, parse_usize, usage, validate_run_id, validate_workload};

pub(super) fn emit_payload(values: &[String]) -> Result<(), Box<dyn Error>> {
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

pub(super) fn emit_schedule(values: &[String]) -> Result<(), Box<dyn Error>> {
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

/// `histogram-vector`: whitespace-separated `u64` values on stdin, one
/// encoded histogram on stdout.
///
/// The values are read rather than named on the command line because the
/// committed conformance input is a file of them, and a vector whose input
/// lives in the argument parser would be a vector of the argument parser.
pub(super) fn emit_histogram_vector(values: &[String]) -> Result<(), Box<dyn Error>> {
    if !values.is_empty() {
        return Err(usage().into());
    }
    let mut input = std::io::stdin().lock();
    let mut output = std::io::stdout().lock();
    histogram_vector::emit(&mut input, &mut output)
}
