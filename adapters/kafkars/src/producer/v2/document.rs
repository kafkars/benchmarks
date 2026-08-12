//! Assembling `kafkars.producer-benchmark.v2` from a finished measurement.

use std::error::Error;

use bench_schema::{
    DeclaredExecution, LoadMode, MeasuredThroughput, OfferOutcomes, ProducerBenchmarkV2,
    QueueObservation,
};

use crate::protocol::ADAPTER_NAME;

use super::{measurement::Measurement, outstanding::OutstandingGauge};

/// Nanoseconds in one second.
const NANOS_PER_SECOND: f64 = 1_000_000_000.0;

/// How payload bytes came to exist: templates built before the run.
pub(crate) const PAYLOAD_CONSTRUCTION: &str = "prebuilt-pool";

/// Who owned the bytes across the client boundary.
///
/// Not `zero-copy` and not `copy-in`: `send_batch` takes ownership, so each
/// offer carries its own buffer, and that buffer is a copy of a pooled
/// template rather than freshly computed bytes. Naming it precisely is the
/// point — a reader comparing this against an adapter that hands the client a
/// borrowed slice is comparing different work, and should be able to see that
/// from the document alone.
pub(crate) const OWNERSHIP: &str = "owned-per-offer-from-pool";

/// How completion is observed: one aggregate terminal per admitted batch.
///
/// This is the same words the capability document and the experiment's
/// `completion_shape` already use, because it is the same fact: the adapter
/// polls one future per batch and reads every record's terminal out of that
/// batch's aggregate result, so records in a batch share a terminal instant.
pub(crate) const COMPLETION_MODE: &str = "aggregate-batch-terminal";

/// Whether serialization is inside the measured interval.
pub(crate) const SERIALIZATION: &str = "excluded";

/// Everything the document needs that the measurement does not carry.
#[derive(Debug)]
pub(super) struct DocumentRequest<'a> {
    /// The attempt's run id.
    pub(super) run_id: &'a str,
    /// The load mode this phase ran under.
    pub(super) load_mode: LoadMode,
    /// Bytes in one record's payload.
    pub(super) payload_bytes: u64,
    /// Records the experiment asked for.
    pub(super) expected_records: u64,
    /// The measured interval.
    pub(super) measured_duration_ns: u64,
    /// The finished evidence.
    pub(super) measurement: &'a Measurement,
    /// The shared outstanding-offer observation.
    pub(super) outstanding: &'a OutstandingGauge,
}

/// Builds the v2 document and refuses to return one that fails its own
/// accounting.
///
/// Validating here rather than at the write site means a document that cannot
/// hold its invariants never reaches a file, so a sealed bundle cannot contain
/// one. The failure that produces is an adapter error with a status document,
/// which is a thing a reader can act on; a silently inconsistent measurement
/// is not.
pub(super) fn build(request: &DocumentRequest<'_>) -> Result<ProducerBenchmarkV2, Box<dyn Error>> {
    let outcomes = request.measurement.outcomes();
    let invalid_reason = verdict(request, outcomes);
    let document = ProducerBenchmarkV2 {
        schema: ProducerBenchmarkV2::SCHEMA.to_owned(),
        adapter: ADAPTER_NAME.to_owned(),
        adapter_version: env!("CARGO_PKG_VERSION").to_owned(),
        run_id: request.run_id.to_owned(),
        load_mode: request.load_mode,
        declared: DeclaredExecution {
            payload_construction: PAYLOAD_CONSTRUCTION.to_owned(),
            ownership: OWNERSHIP.to_owned(),
            completion_mode: COMPLETION_MODE.to_owned(),
            serialization: SERIALIZATION.to_owned(),
        },
        outcomes,
        timing: request.measurement.timing(),
        throughput: throughput(request, outcomes),
        queue: QueueObservation {
            max_outstanding_observed: request.outstanding.maximum(),
            final_outstanding: request.outstanding.current(),
        },
        // Self-reported process resources are a named deferred check in the
        // control plane's classification, not something this adapter silently
        // omits.
        resources: None,
        // `kafkars` emits no statistics JSONL; its native metric snapshot has
        // no field in this document and no schema of its own to point at.
        native_metrics_path: None,
        valid: invalid_reason.is_none(),
        invalid_reason,
    };
    document.validate()?;
    Ok(document)
}

/// Goodput over the measured interval.
fn throughput(request: &DocumentRequest<'_>, outcomes: OfferOutcomes) -> MeasuredThroughput {
    #[expect(
        clippy::cast_precision_loss,
        reason = "throughput is reported evidence, never hashed into an identity"
    )]
    let seconds = request.measured_duration_ns as f64 / NANOS_PER_SECOND;
    #[expect(
        clippy::cast_precision_loss,
        reason = "throughput is reported evidence, never hashed into an identity"
    )]
    let acknowledged = outcomes.acknowledged as f64;
    #[expect(
        clippy::cast_precision_loss,
        reason = "throughput is reported evidence, never hashed into an identity"
    )]
    let bytes = outcomes.acknowledged.saturating_mul(request.payload_bytes) as f64;
    // A zero interval would divide to infinity, which serde writes as `null`
    // and the reader then cannot parse as a number. Reporting zero goodput for
    // a zero-length interval is both true and readable.
    let (records_per_second, bytes_per_second) = if seconds > 0.0 {
        (acknowledged / seconds, bytes / seconds)
    } else {
        (0.0, 0.0)
    };
    MeasuredThroughput {
        measured_duration_ns: request.measured_duration_ns,
        acknowledged_records_per_second: records_per_second,
        acknowledged_payload_bytes_per_second: bytes_per_second,
    }
}

/// Why this measurement is not believable, when it is not.
///
/// The adapter states its own verdict; it does not get to decide the run's.
/// The control plane reads this alongside the read-back verification and the
/// status document, and any of the three can withhold validity.
fn verdict(request: &DocumentRequest<'_>, outcomes: OfferOutcomes) -> Option<String> {
    let expected = request.expected_records;
    let context = format!(
        " after {} public admission attempts{}",
        request.measurement.admission_attempts(),
        request
            .measurement
            .first_failure()
            .map_or_else(String::new, |failure| format!("; first failure: {failure}"))
    );
    if outcomes.offered != expected {
        return Some(format!(
            "the phase offered {} of {expected} records{context}",
            outcomes.offered
        ));
    }
    if outcomes.accepted != outcomes.offered {
        return Some(format!(
            "the client accepted {} of {} offered records{context}",
            outcomes.accepted, outcomes.offered
        ));
    }
    if outcomes.acknowledged != expected {
        return Some(format!(
            "{} of {expected} records were acknowledged: {} failed, {} timed out, \
             {} unknown{context}",
            outcomes.acknowledged, outcomes.failed, outcomes.timed_out, outcomes.unknown
        ));
    }
    None
}
