//! Replicated benchmark topic provisioning and exact cleanup.

use std::{
    error::Error,
    thread,
    time::{Duration, Instant},
};

use kafkars::{
    Client,
    admin::{BatchResult, NewTopic, TopicDescription},
};

use crate::arguments::{TopicArgs, TopicDeletionArgs};

const ADMIN_TIMEOUT: Duration = Duration::from_secs(60);
const DESCRIBE_TIMEOUT: Duration = Duration::from_secs(5);
const READINESS_POLL: Duration = Duration::from_millis(100);

pub(crate) fn create(arguments: &TopicArgs) -> Result<(), Box<dyn Error>> {
    let client = client(&arguments.bootstrap, "kafkars-benchmark-topic-create")?;
    let requested = arguments.topics.iter().map(|topic| {
        NewTopic::new(topic, arguments.partitions)
            .replication_factor(arguments.replication_factor)
            .config("min.insync.replicas", "2")
    });
    let result = client
        .admin()
        .create_topics(requested)
        .deadline_after(ADMIN_TIMEOUT)
        .submit()
        .wait()?;
    require_success(result, &arguments.topics, "CreateTopics")?;
    let topics = arguments
        .topics
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    await_ready(
        &client,
        &topics,
        arguments.partitions,
        usize::try_from(arguments.replication_factor)?,
    )?;
    client.shutdown().wait()?;
    Ok(())
}

pub(crate) fn await_ready(
    client: &Client,
    topics: &[&str],
    partitions: i32,
    replication_factor: usize,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + ADMIN_TIMEOUT;
    loop {
        let result = client
            .admin()
            .describe_topics(topics.iter().copied())
            .deadline_after(DESCRIBE_TIMEOUT)
            .submit()
            .wait();
        if result
            .as_ref()
            .is_ok_and(|batch| topics_are_ready(batch, topics, partitions, replication_factor))
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("benchmark topics did not reach full replicated leader readiness".into());
        }
        thread::sleep(READINESS_POLL);
    }
}

fn topics_are_ready(
    result: &BatchResult<String, TopicDescription>,
    topics: &[&str],
    partitions: i32,
    replication_factor: usize,
) -> bool {
    result.entries().len() == topics.len()
        && result
            .entries()
            .iter()
            .zip(topics)
            .all(|((topic, outcome), expected)| {
                topic == *expected
                    && outcome.as_ref().is_ok_and(|description| {
                        description.partitions().len()
                            == usize::try_from(partitions).unwrap_or_default()
                            && description.partitions().iter().enumerate().all(
                                |(index, partition)| {
                                    partition.partition_index()
                                        == i32::try_from(index).unwrap_or(i32::MIN)
                                        && partition.error().is_none()
                                        && partition.leader_id().is_some()
                                        && partition.replicas().len() == replication_factor
                                        && partition.in_sync_replicas().len() == replication_factor
                                        && partition.offline_replicas().is_empty()
                                },
                            )
                    })
            })
}

pub(crate) fn delete(arguments: &TopicDeletionArgs) -> Result<(), Box<dyn Error>> {
    let client = client(&arguments.bootstrap, "kafkars-benchmark-topic-delete")?;
    let result = client
        .admin()
        .delete_topics(arguments.topics.iter().cloned())
        .deadline_after(ADMIN_TIMEOUT)
        .submit()
        .wait()?;
    require_success(result, &arguments.topics, "DeleteTopics")?;
    client.shutdown().wait()?;
    Ok(())
}

fn client(bootstrap: &str, client_id: &str) -> Result<Client, Box<dyn Error>> {
    let client = Client::builder()
        .bootstrap_servers(bootstrap.split(',').map(str::to_owned))
        .client_id(client_id)
        .build()?;
    client.ready().wait()?;
    Ok(client)
}

fn require_success(
    result: BatchResult<String, ()>,
    expected: &[String],
    operation: &str,
) -> Result<(), Box<dyn Error>> {
    let entries = result.into_entries();
    if entries.len() != expected.len() {
        return Err(format!(
            "{operation} returned {} results for {} topics",
            entries.len(),
            expected.len()
        )
        .into());
    }
    for ((topic, outcome), expected_topic) in entries.into_iter().zip(expected) {
        if &topic != expected_topic {
            return Err(format!("{operation} returned {topic:?} for {expected_topic:?}").into());
        }
        outcome?;
    }
    Ok(())
}
