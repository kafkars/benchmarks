//! Argument shapes for the topic verbs the control plane calls as configured
//! tools.
//!
//! Topic management is not part of the adapter protocol — an adapter never
//! decides whether its own topics were right — so these stay positional
//! commands the control plane invokes by argument vector.

use std::error::Error;

use super::parse::{parse_i32, usage, validate_endpoint, validate_topic};

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

pub(super) fn parse_topics(values: &[String]) -> Result<TopicArgs, Box<dyn Error>> {
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

pub(super) fn parse_topic_deletion(values: &[String]) -> Result<TopicDeletionArgs, Box<dyn Error>> {
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
