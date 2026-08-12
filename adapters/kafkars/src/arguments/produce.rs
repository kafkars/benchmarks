//! The two produce verbs' argument shapes and their positional parsers.
//!
//! The field order below is the legacy command line, which the migrated Node
//! control plane still writes; a reordering here would silently change what a
//! sealed run measured.

use std::{error::Error, path::PathBuf};

use crate::schedule;

use super::parse::{
    parse_i32, parse_u64, parse_usize, usage, validate_endpoint, validate_run_id, validate_topic,
    validate_workload,
};

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

pub(super) fn parse_produce(values: &[String]) -> Result<ProduceArgs, Box<dyn Error>> {
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

pub(super) fn parse_fixed_produce(values: &[String]) -> Result<FixedProduceArgs, Box<dyn Error>> {
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
