//! Public-client setup, warmup, fixed-rate execution, and complete shutdown.

use std::error::Error;

use crate::{arguments::FixedProduceArgs, report::NativeMetrics};

use super::{
    FIXED_RATE_CLIENT_ID, FIXED_RATE_INVALID, RunOutcome,
    batch_phase::run_batch_phase,
    fixed_phase, fixed_report,
    phase::PhaseSpec,
    session::{FIXED_RATE_WAITING_BYTES, Session, SessionSpec},
};

pub(super) fn run(arguments: &FixedProduceArgs) -> Result<RunOutcome, Box<dyn Error>> {
    let common = &arguments.common;
    let session = Session::open(SessionSpec {
        bootstrap: &common.bootstrap,
        client_id: FIXED_RATE_CLIENT_ID,
        topics: [&common.warmup_topic, &common.topic],
        partitions: common.partitions,
        max_outstanding: common.max_outstanding,
        waiting_bytes: FIXED_RATE_WAITING_BYTES,
    })?;
    let producer = session.producer.clone();

    if common.warmup_records > 0 {
        let warmup = run_batch_phase(
            &producer,
            PhaseSpec {
                topic: &common.warmup_topic,
                run_id: &common.run_id,
                records: common.warmup_records,
                payload_bytes: common.payload_bytes,
                partitions: common.partitions,
                max_outstanding: common.max_outstanding,
                prime_partitions: true,
            },
        )?;
        super::flush(&producer)?;
        if !super::phase_is_valid(&warmup, common.warmup_records) {
            session.close()?;
            return Err("fixed-load warmup failed its exact terminal contract".into());
        }
    }
    let spec = fixed_phase::FixedPhaseSpec {
        phase: PhaseSpec {
            topic: &common.topic,
            run_id: &common.run_id,
            records: common.records,
            payload_bytes: common.payload_bytes,
            partitions: common.partitions,
            max_outstanding: common.max_outstanding,
            prime_partitions: false,
        },
        offered_records_per_second: arguments.offered_records_per_second,
        callers: arguments.callers,
    };
    let before = super::metrics(&session.client)?;
    let mut phase = fixed_phase::run_fixed_phase(&producer, spec)?;
    super::flush(&producer)?;
    let after = super::metrics(&session.client)?;
    fixed_phase::write_latencies(&common.latency_path, &phase.samples)?;
    let native_metrics = NativeMetrics::between(&before, &after, phase.batch_admission);
    let report = fixed_report::build(arguments, &mut phase, native_metrics)?;
    let outcome = RunOutcome::render(&report, report.valid, FIXED_RATE_INVALID)?;

    session.close()?;
    Ok(outcome)
}
