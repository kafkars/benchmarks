//! One configured client and producer handle, and its complete shutdown.
//!
//! Every measured path in this adapter — the two legacy stdout phases and the
//! two v2 protocol phases — must run against a client configured exactly
//! alike, or the four are not measuring the same subject. The settings are
//! constants in `producer.rs` and the call sequence that applies them lives
//! here, once, so a change to either reaches all four paths together.
//!
//! The two waiting-byte budgets are the one deliberate difference: the
//! closed-loop phase lets the client hold the whole queue in waiting, and the
//! fixed-rate phase holds one batch, because an open-loop offer that waits
//! behind a large local queue stops being an open-loop offer.

use std::error::Error;

use kafkars::{Client, Producer, ProducerLimits};

use crate::topics;

use super::{
    BATCH_BYTES, BATCH_RECORDS, DELIVERY_TIMEOUT, LINGER, MAX_IN_FLIGHT_REQUESTS_PER_BROKER,
    MAX_RETRIES, QUEUE_BYTES, REQUEST_BYTES, RETRY_BACKOFF,
};

/// Metadata settle attempts before a phase gives up on its topics.
const TOPIC_ATTEMPTS: usize = 3;

/// Waiting bytes the closed-loop phases allow the client to retain.
pub(super) const CLOSED_LOOP_WAITING_BYTES: usize = QUEUE_BYTES;

/// Waiting bytes the fixed-rate phases allow the client to retain.
pub(super) const FIXED_RATE_WAITING_BYTES: usize = BATCH_BYTES;

/// What a phase needs to open its client.
#[derive(Clone, Copy, Debug)]
pub(super) struct SessionSpec<'a> {
    /// Comma-separated bootstrap endpoints.
    pub(super) bootstrap: &'a str,
    /// Client id the broker attributes this phase's traffic to.
    pub(super) client_id: &'static str,
    /// Topics whose metadata must settle before the phase begins.
    pub(super) topics: [&'a str; 2],
    /// Partitions each of those topics must report.
    pub(super) partitions: i32,
    /// Records the client may hold in flight and in waiting.
    pub(super) max_outstanding: usize,
    /// Bytes the client may hold in waiting.
    pub(super) waiting_bytes: usize,
}

/// A ready client and the producer handle built from it.
#[derive(Debug)]
pub(super) struct Session {
    /// The client, kept for metrics snapshots and shutdown.
    pub(super) client: Client,
    /// The producer every phase admits through.
    pub(super) producer: Producer,
}

impl Session {
    /// Builds the client, waits for readiness and topic metadata, and returns
    /// the producer handle.
    pub(super) fn open(spec: SessionSpec<'_>) -> Result<Self, Box<dyn Error>> {
        let limits = ProducerLimits::default()
            .with_retained_bytes(QUEUE_BYTES)
            .with_in_flight_records(spec.max_outstanding)
            .with_waiting_records(spec.max_outstanding)
            .with_waiting_bytes(spec.waiting_bytes)
            .with_batch_records(BATCH_RECORDS)
            .with_batch_bytes(BATCH_BYTES)
            .with_request_bytes(REQUEST_BYTES)
            .with_max_in_flight_requests_per_broker(MAX_IN_FLIGHT_REQUESTS_PER_BROKER)
            .with_linger(LINGER);
        let client = Client::builder()
            .bootstrap_servers(spec.bootstrap.split(',').map(str::to_owned))
            .client_id(spec.client_id)
            .producer_limits(limits)
            .producer_retry(MAX_RETRIES, RETRY_BACKOFF)
            .producer_delivery_timeout(DELIVERY_TIMEOUT)
            .build()?;
        client.ready().wait()?;
        topics::await_ready(&client, &spec.topics, spec.partitions, TOPIC_ATTEMPTS)?;
        let producer = client.producer().build()?;
        Ok(Self { client, producer })
    }

    /// Closes the producer and shuts the client down.
    pub(super) fn close(self) -> Result<(), Box<dyn Error>> {
        super::close(&self.producer)?;
        self.client.shutdown().wait()?;
        Ok(())
    }
}
