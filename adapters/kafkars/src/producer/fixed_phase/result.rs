//! Fixed-rate terminal accounting with corrected and uncorrected latency.

use std::{
    error::Error,
    fs::File,
    io::{BufWriter, Write},
    time::{Duration, Instant},
};

use kafkars::{ErrorKind, Record, SendBatchResult};

use crate::{
    report::{ApplicationBatchAdmissionMetrics, LatencyReport, latencies},
    schedule,
};

use super::{FixedPhaseSpec, caller::Slot};

#[derive(Debug)]
pub(in crate::producer) struct FixedPhaseResult {
    pub(in crate::producer) offered: usize,
    pub(in crate::producer) accepted: usize,
    pub(in crate::producer) acknowledged: usize,
    pub(in crate::producer) failed: usize,
    pub(in crate::producer) duration: Duration,
    pub(in crate::producer) samples: Vec<Vec<FixedLatencySample>>,
    pub(in crate::producer) failure_details: Vec<String>,
    pub(in crate::producer) batch_admission: ApplicationBatchAdmissionMetrics,
}

#[derive(Debug)]
pub(in crate::producer) struct FixedLatencySample {
    sequence: usize,
    intended_ns: u64,
    admitted_ns: u64,
    completed_ns: u64,
    uncorrected_latency_ns: u64,
    corrected_latency_ns: u64,
}

pub(super) enum BatchCompletion {
    Complete,
    Retry(Vec<Record>),
}

impl FixedPhaseResult {
    pub(super) fn for_caller(_caller_id: usize, batches: usize) -> Self {
        let records = batches.saturating_mul(crate::producer::BATCH_RECORDS);
        Self {
            offered: 0,
            accepted: 0,
            acknowledged: 0,
            failed: 0,
            duration: Duration::ZERO,
            samples: vec![Vec::with_capacity(records)],
            failure_details: Vec::new(),
            batch_admission: ApplicationBatchAdmissionMetrics::default(),
        }
    }

    pub(in crate::producer) fn merge(
        results: Vec<Self>,
        expected_records: usize,
    ) -> Result<Self, Box<dyn Error>> {
        let mut merged = Self {
            offered: 0,
            accepted: 0,
            acknowledged: 0,
            failed: 0,
            duration: Duration::ZERO,
            samples: Vec::with_capacity(results.len()),
            failure_details: Vec::new(),
            batch_admission: ApplicationBatchAdmissionMetrics::default(),
        };
        for mut result in results {
            merged.offered += result.offered;
            merged.accepted += result.accepted;
            merged.acknowledged += result.acknowledged;
            merged.failed += result.failed;
            merged.duration = merged.duration.max(result.duration);
            merged.samples.append(&mut result.samples);
            merged.failure_details.append(&mut result.failure_details);
            merged.batch_admission.merge(result.batch_admission);
        }
        if merged.offered != expected_records {
            return Err("fixed-load callers did not cover every scheduled record".into());
        }
        if merged.samples.iter().map(Vec::len).sum::<usize>() != merged.acknowledged {
            return Err("fixed-load terminal samples do not match acknowledgements".into());
        }
        Ok(merged)
    }

    pub(in crate::producer) fn latency_reports(
        &self,
    ) -> (LatencyReport, LatencyReport, LatencyReport) {
        (
            summarize(&self.samples, |sample| sample.uncorrected_latency_ns),
            summarize(&self.samples, |sample| sample.corrected_latency_ns),
            summarize(&self.samples, |sample| {
                sample.admitted_ns.saturating_sub(sample.intended_ns)
            }),
        )
    }
}

pub(super) fn record_batch(
    phase: &mut FixedPhaseResult,
    slot: &Slot,
    batch: SendBatchResult,
    epoch: Instant,
    spec: FixedPhaseSpec<'_>,
) -> Result<BatchCompletion, Box<dyn Error>> {
    let completed = Instant::now();
    let (deliveries, rejection) = batch.into_parts();
    let accepted = deliveries.len();
    phase.offered += usize::try_from(slot.schedule.count)?;
    phase.accepted += accepted;
    for (index, delivery) in deliveries.into_iter().enumerate() {
        let sequence = usize::try_from(slot.schedule.first_sequence)? + index;
        match delivery {
            Ok(_) => record_success(phase, sequence, slot.admitted, completed, epoch, spec)?,
            Err(error) => {
                phase.failed += 1;
                phase.failure_details.push(format!(
                    "sequence {sequence} kind={:?} delivery={:?} broker_code={:?} message={error}",
                    error.kind(),
                    error.delivery_status(),
                    error.broker_code(),
                ));
            }
        }
    }
    match rejection {
        None if accepted == usize::try_from(slot.schedule.count)? => Ok(BatchCompletion::Complete),
        Some(rejection) if accepted == 0 && rejection.error().kind() == ErrorKind::Backpressure => {
            let (records, _) = rejection.into_parts();
            phase.offered = phase.offered.saturating_sub(records.len());
            Ok(BatchCompletion::Retry(records))
        }
        _ => Err("fixed-load batch returned a partial or malformed admission result".into()),
    }
}

fn record_success(
    phase: &mut FixedPhaseResult,
    sequence: usize,
    admitted: Instant,
    completed: Instant,
    epoch: Instant,
    spec: FixedPhaseSpec<'_>,
) -> Result<(), Box<dyn Error>> {
    let intended_ns =
        schedule::intended_offset_ns(u64::try_from(sequence)?, spec.offered_records_per_second)?;
    let admitted_ns = u64::try_from(admitted.duration_since(epoch).as_nanos())?;
    let completed_ns = u64::try_from(completed.duration_since(epoch).as_nanos())?;
    let uncorrected = u64::try_from(completed.duration_since(admitted).as_nanos())?;
    let corrected = completed_ns
        .checked_sub(intended_ns)
        .ok_or("fixed-load completion preceded its intended schedule")?;
    admitted_ns
        .checked_sub(intended_ns)
        .ok_or("fixed-load admission preceded its intended record schedule")?;
    phase.acknowledged += 1;
    phase
        .samples
        .first_mut()
        .ok_or("fixed-load caller lost its sample segment")?
        .push(FixedLatencySample {
            sequence,
            intended_ns,
            admitted_ns,
            completed_ns,
            uncorrected_latency_ns: uncorrected,
            corrected_latency_ns: corrected,
        });
    Ok(())
}

pub(in crate::producer) fn write_latencies(
    path: &std::path::Path,
    segments: &[Vec<FixedLatencySample>],
) -> Result<(), Box<dyn Error>> {
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(
        writer,
        "sequence,intended_ns,admitted_ns,completed_ns,uncorrected_latency_ns,corrected_latency_ns"
    )?;
    let mut positions = vec![0; segments.len()];
    let total = segments.iter().map(Vec::len).sum::<usize>();
    let mut previous = None;
    for _ in 0..total {
        let (segment_index, sample) = next_sample(segments, &positions)
            .ok_or("fixed-load sample merge ended before its declared total")?;
        if previous.is_some_and(|sequence| sequence >= sample.sequence) {
            return Err("fixed-load samples are duplicated or out of order".into());
        }
        writeln!(
            writer,
            "{},{},{},{},{},{}",
            sample.sequence,
            sample.intended_ns,
            sample.admitted_ns,
            sample.completed_ns,
            sample.uncorrected_latency_ns,
            sample.corrected_latency_ns,
        )?;
        previous = Some(sample.sequence);
        positions[segment_index] += 1;
    }
    writer.flush()?;
    Ok(())
}

fn summarize(
    segments: &[Vec<FixedLatencySample>],
    field: impl Fn(&FixedLatencySample) -> u64,
) -> LatencyReport {
    let mut values = segments
        .iter()
        .flatten()
        .map(|sample| u128::from(field(sample)))
        .collect::<Vec<_>>();
    latencies(&mut values)
}

fn next_sample<'a>(
    segments: &'a [Vec<FixedLatencySample>],
    positions: &[usize],
) -> Option<(usize, &'a FixedLatencySample)> {
    segments
        .iter()
        .enumerate()
        .filter_map(|(index, segment)| segment.get(positions[index]).map(|sample| (index, sample)))
        .min_by_key(|(_index, sample)| sample.sequence)
}

#[cfg(test)]
mod result_test;
