//! The v2 measured path: four timestamps per offer, bounded evidence, and the
//! `kafkars.producer-benchmark.v2` document the adapter protocol writes.
//!
//! # Why this is a second path and not an edit to the first
//!
//! The legacy stdout verbs are driven by a control plane whose evidence is
//! already sealed, so their bytes are frozen: `produce` and `produce-fixed`
//! keep emitting `kafkars.producer-benchmark.v1` and the per-record
//! `latency.csv` forever. v2 changes what is measured, not how a number is
//! formatted — the admission clock now spans queue-full retries, evidence no
//! longer scales with run length, and payload bytes are prebuilt — so it
//! cannot be applied to the frozen path without redefining what those sealed
//! runs meant. The two paths therefore run side by side, over one shared
//! client configuration ([`super::session`]), until the legacy surface is
//! retired.
//!
//! # What one offer is
//!
//! One record. Its four timestamps all come from [`clock::RunClock`], so every
//! duration this path reports is a difference on one monotonic clock:
//!
//! - `intended` — when the schedule wanted it offered. Fixed-rate reads the
//!   exact legacy per-record schedule `floor(sequence * 1e9 / rate)`;
//!   closed-loop has no schedule, so `intended` is `call_start`.
//! - `call_start` — immediately before the *first* `send_batch` attempt
//!   carrying it, and never again. See [`admission::AdmissionClock`].
//! - `accepted` — when that call returned with the client owning the bytes.
//! - `terminal` — when the batch's aggregate result was observed.
//!
//! Records cross the boundary in batches, so the offers in one batch share
//! `call_start`, `accepted`, and `terminal`; only `intended` is per record.
//! That is what `completion_mode: "aggregate-batch-terminal"` declares, and it
//! is why the slab stores one entry per in-flight *group* rather than per
//! record while remaining bounded by the offer budget.
//!
//! # What this path does not report
//!
//! Native client metrics and per-record latency rows. The v2 document has no
//! field for a `kafkars` metrics snapshot, and inventing one here would mint a
//! schema this adapter does not own; `latency.csv` is exactly the run-sized
//! evidence the histograms replace. Both remain available on the legacy path.

mod admission;
#[cfg(test)]
mod admission_test;
mod clock;
mod closed_loop;
mod document;
#[cfg(test)]
mod document_test;
mod engine;
#[cfg(test)]
mod engine_test;
mod fixed_rate;
mod measurement;
#[cfg(test)]
mod measurement_test;
mod outstanding;
mod pool;
#[cfg(test)]
mod pool_test;
mod slab;

use std::{error::Error, sync::Arc, time::Instant};

use bench_schema::{LoadMode, ProducerBenchmarkV2};

use crate::arguments::{FixedProduceArgs, ProduceArgs};

use super::session::{CLOSED_LOOP_WAITING_BYTES, FIXED_RATE_WAITING_BYTES, Session, SessionSpec};

use self::{
    clock::RunClock, document::DocumentRequest, measurement::Measurement,
    outstanding::OutstandingGauge, pool::PayloadPool,
};

pub(crate) use self::document::{COMPLETION_MODE as V2_COMPLETION_MODE, OWNERSHIP as V2_OWNERSHIP};

/// Everything a phase shares with the engine that runs it.
#[derive(Debug)]
pub(super) struct PhaseContext<'a> {
    /// The producer every offer is admitted through.
    producer: &'a kafkars::Producer,
    /// Prebuilt payload bytes, copied per offer.
    pool: &'a PayloadPool,
    /// The topic this phase produces to.
    topic: Arc<str>,
    /// The one clock every timestamp is read from.
    clock: RunClock,
    /// The shared outstanding-offer observation.
    outstanding: &'a OutstandingGauge,
}

/// The shape of one phase's offer loop.
#[derive(Clone, Copy, Debug)]
pub(super) struct PhaseShape {
    /// Records this phase offers.
    records: u64,
    /// Partitions offers are assigned to, round robin by sequence.
    partitions: usize,
    /// Offers the client may own at once.
    budget: u64,
    /// Whether the first record per partition is admitted alone.
    prime_partitions: bool,
}

/// Runs one closed-loop experiment and returns its v2 document.
pub(crate) fn run_closed_loop_v2(
    arguments: &ProduceArgs,
) -> Result<ProducerBenchmarkV2, Box<dyn Error>> {
    let session = Session::open(SessionSpec {
        bootstrap: &arguments.bootstrap,
        client_id: super::CLOSED_LOOP_CLIENT_ID,
        topics: [&arguments.warmup_topic, &arguments.topic],
        partitions: arguments.partitions,
        max_outstanding: arguments.max_outstanding,
        waiting_bytes: CLOSED_LOOP_WAITING_BYTES,
    })?;
    let outcome = measure_closed_loop(&session, arguments);
    seal(outcome, session.close())
}

/// Runs one fixed-rate experiment and returns its v2 document.
pub(crate) fn run_fixed_rate_v2(
    arguments: &FixedProduceArgs,
) -> Result<ProducerBenchmarkV2, Box<dyn Error>> {
    let common = &arguments.common;
    let session = Session::open(SessionSpec {
        bootstrap: &common.bootstrap,
        client_id: super::FIXED_RATE_CLIENT_ID,
        topics: [&common.warmup_topic, &common.topic],
        partitions: common.partitions,
        max_outstanding: common.max_outstanding,
        waiting_bytes: FIXED_RATE_WAITING_BYTES,
    })?;
    let outcome = measure_fixed_rate(&session, arguments);
    seal(outcome, session.close())
}

/// Reports what the measurement said, not what shutting down afterwards said.
///
/// The session is closed on every path, including the failing ones, so a phase
/// that gave up still releases the client rather than leaving the process to
/// tear it down. That ordering had a cost: `close()?` ran first, so a shutdown
/// that failed *because* the phase had already failed replaced the diagnosis
/// with its own symptom, and the run reported the consequence instead of the
/// cause. The measurement's error is therefore the one that survives, and a
/// close failure is appended to it rather than substituted for it.
pub(super) fn seal<T>(
    outcome: Result<T, Box<dyn Error>>,
    closed: Result<(), Box<dyn Error>>,
) -> Result<T, Box<dyn Error>> {
    match (outcome, closed) {
        (Ok(document), Ok(())) => Ok(document),
        (Ok(_), Err(close)) => Err(close),
        (Err(failure), Ok(())) => Err(failure),
        (Err(failure), Err(close)) => {
            Err(format!("{failure}; the session then failed to close: {close}").into())
        }
    }
}

/// Warms up, measures, and builds the closed-loop document.
fn measure_closed_loop(
    session: &Session,
    arguments: &ProduceArgs,
) -> Result<ProducerBenchmarkV2, Box<dyn Error>> {
    let pool = PayloadPool::build(&arguments.run_id, arguments.payload_bytes)?;
    let budget = u64::try_from(arguments.max_outstanding)?;
    let partitions = usize::try_from(arguments.partitions)?;
    warm_up(session, &pool, arguments, partitions, budget)?;

    let outstanding = OutstandingGauge::default();
    let started = Instant::now();
    let context = PhaseContext {
        producer: &session.producer,
        pool: &pool,
        topic: Arc::from(arguments.topic.as_str()),
        clock: RunClock::starting_at(started),
        outstanding: &outstanding,
    };
    let shape = PhaseShape {
        records: u64::try_from(arguments.records)?,
        partitions,
        budget,
        prime_partitions: false,
    };
    let measurement = closed_loop::run(&context, shape, Measurement::closed_loop())?;
    let measured_duration_ns = context.clock.at(Instant::now());
    super::flush(&session.producer)?;
    document::build(&DocumentRequest {
        run_id: &arguments.run_id,
        load_mode: LoadMode::ClosedLoop,
        payload_bytes: u64::try_from(arguments.payload_bytes)?,
        expected_records: shape.records,
        measured_duration_ns,
        measurement: &measurement,
        outstanding: &outstanding,
    })
}

/// Warms up, measures, and builds the fixed-rate document.
fn measure_fixed_rate(
    session: &Session,
    arguments: &FixedProduceArgs,
) -> Result<ProducerBenchmarkV2, Box<dyn Error>> {
    let common = &arguments.common;
    let pool = PayloadPool::build(&common.run_id, common.payload_bytes)?;
    let budget = u64::try_from(common.max_outstanding)?;
    let partitions = usize::try_from(common.partitions)?;
    warm_up(session, &pool, common, partitions, budget)?;

    let outstanding = OutstandingGauge::default();
    let outcome = fixed_rate::run(&fixed_rate::FixedRateSpec {
        producer: &session.producer,
        pool: &pool,
        topic: &common.topic,
        records: u64::try_from(common.records)?,
        partitions,
        budget,
        callers: arguments.callers,
        offered_records_per_second: arguments.offered_records_per_second,
        outstanding: &outstanding,
    })?;
    super::flush(&session.producer)?;
    document::build(&DocumentRequest {
        run_id: &common.run_id,
        load_mode: LoadMode::ScheduledOpenLoopFixedRate,
        payload_bytes: u64::try_from(common.payload_bytes)?,
        expected_records: u64::try_from(common.records)?,
        measured_duration_ns: outcome.measured_duration_ns,
        measurement: &outcome.measurement,
        outstanding: &outstanding,
    })
}

/// Runs the warmup phase, whose evidence is discarded and whose failure is not.
///
/// The warmup runs on the same bounded engine as the measured interval, so a
/// long warmup cannot reintroduce the run-sized allocation the v2 path exists
/// to remove. Its measurement is dropped: warming a client is not evidence
/// about it.
fn warm_up(
    session: &Session,
    pool: &PayloadPool,
    arguments: &ProduceArgs,
    partitions: usize,
    budget: u64,
) -> Result<(), Box<dyn Error>> {
    if arguments.warmup_records == 0 {
        return Ok(());
    }
    let outstanding = OutstandingGauge::default();
    let context = PhaseContext {
        producer: &session.producer,
        pool,
        topic: Arc::from(arguments.warmup_topic.as_str()),
        clock: RunClock::starting_at(Instant::now()),
        outstanding: &outstanding,
    };
    let records = u64::try_from(arguments.warmup_records)?;
    let shape = PhaseShape {
        records,
        partitions,
        budget,
        prime_partitions: true,
    };
    let warmup = closed_loop::run(&context, shape, Measurement::closed_loop())?;
    super::flush(&session.producer)?;
    let outcomes = warmup.outcomes();
    if outcomes.acknowledged != records || outcomes.failed != 0 || outcomes.timed_out != 0 {
        return Err(format!(
            "warmup offered {records}, acknowledged {}, failed {}, and timed out {}: {}",
            outcomes.acknowledged,
            outcomes.failed,
            outcomes.timed_out,
            warmup.first_failure().unwrap_or("no failure was reported"),
        )
        .into());
    }
    Ok(())
}
