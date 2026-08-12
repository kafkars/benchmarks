//! The pieces every verb's parser is built from: the usage string, the field
//! validators, and the three numeric parsers.
//!
//! The usage string lives here rather than beside the dispatch table because it
//! is the error *every* parser returns; a verb that failed to parse and a verb
//! nobody recognises are the same answer to the operator, and there is one
//! place that answer is written down.

use std::error::Error;

pub(crate) const RUN_ID_BYTES: usize = 16;
pub(crate) const MIN_PAYLOAD_BYTES: usize = 64;

pub(super) fn validate_endpoint(value: &str) -> Result<(), Box<dyn Error>> {
    if value.is_empty() || !value.contains(':') {
        return Err("bootstrap must contain at least one host:port endpoint".into());
    }
    Ok(())
}

pub(super) fn validate_topic(value: &str) -> Result<(), Box<dyn Error>> {
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

pub(super) fn validate_run_id(value: &str) -> Result<(), Box<dyn Error>> {
    if value.len() != RUN_ID_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("run ID must be exactly 16 lowercase hexadecimal bytes".into());
    }
    Ok(())
}

pub(super) fn validate_workload(
    payload_bytes: usize,
    partitions: i32,
) -> Result<(), Box<dyn Error>> {
    if payload_bytes < MIN_PAYLOAD_BYTES {
        return Err(format!("payload must be at least {MIN_PAYLOAD_BYTES} bytes").into());
    }
    if partitions <= 0 {
        return Err("partition count must be positive".into());
    }
    Ok(())
}

pub(super) fn parse_usize(
    name: &str,
    value: &str,
    zero_allowed: bool,
) -> Result<usize, Box<dyn Error>> {
    let parsed = value.parse::<usize>()?;
    if !zero_allowed && parsed == 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(parsed)
}

pub(super) fn parse_u64(
    name: &str,
    value: &str,
    zero_allowed: bool,
) -> Result<u64, Box<dyn Error>> {
    let parsed = value.parse::<u64>()?;
    if !zero_allowed && parsed == 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(parsed)
}

pub(super) fn parse_i32(name: &str, value: &str) -> Result<i32, Box<dyn Error>> {
    let parsed = value.parse::<i32>()?;
    if parsed <= 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(parsed)
}

pub(super) fn usage() -> &'static str {
    "usage: kafkars-benchmark-adapter \
     <payload|schedule|histogram-vector|produce|produce-fixed|topics-create|topics-delete\
     |describe|validate|run> ..."
}
