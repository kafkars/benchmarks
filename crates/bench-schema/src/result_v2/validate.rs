//! The accounting invariants `kafkars.producer-benchmark.v2` enforces, and the
//! order they are named in.
//!
//! Separated from the document's shape because they are a different kind of
//! statement: the struct says what fields exist, and this file says which
//! combinations of their values could actually have happened. A reader auditing
//! "can this document lie about how many records it sent" reads only this file.
//!
//! Every check names the first field that failed rather than accumulating a
//! list, because these are structural impossibilities: a document whose offered
//! count is below its accepted count is not a measurement with one problem, it
//! is bytes that no run could have produced.

use crate::error::{SchemaError, SchemaResult};
use crate::experiment::LoadMode;
use crate::schema_id::require_schema;

use super::{OfferOutcomes, ProducerBenchmarkV2};

/// Checks the invariants documented on [`ProducerBenchmarkV2`].
///
/// # Errors
///
/// Returns an error naming the first violated invariant.
pub(super) fn validate(document: &ProducerBenchmarkV2) -> SchemaResult<()> {
    require_schema(&document.schema, ProducerBenchmarkV2::SCHEMA)?;
    if document.timing.clock != "monotonic-ns" {
        return Err(SchemaError::invalid_field(
            "timing.clock",
            &format!("{:?} is not \"monotonic-ns\"", document.timing.clock),
        ));
    }
    let o = document.outcomes;
    if o.offered < o.accepted {
        return Err(SchemaError::invalid_field(
            "outcomes.offered",
            &format!("offered {} is below accepted {}", o.offered, o.accepted),
        ));
    }
    let terminals = o
        .acknowledged
        .saturating_add(o.failed)
        .saturating_add(o.timed_out);
    if terminals.saturating_add(o.unknown) != o.accepted {
        return Err(SchemaError::invalid_field(
            "outcomes.accepted",
            &format!(
                "accepted {} does not equal terminals {terminals} plus unknown {}",
                o.accepted, o.unknown
            ),
        ));
    }
    validate_timing(document, o, terminals)?;
    if !document.valid && document.invalid_reason.is_none() {
        return Err(SchemaError::invalid_field(
            "invalid_reason",
            "an invalid measurement must say why",
        ));
    }
    Ok(())
}

/// Checks the histogram totals against the outcome accounting.
fn validate_timing(
    document: &ProducerBenchmarkV2,
    o: OfferOutcomes,
    terminals: u64,
) -> SchemaResult<()> {
    let timing = &document.timing;
    for (name, histogram) in [
        ("timing.intended_to_terminal", &timing.intended_to_terminal),
        ("timing.accepted_to_terminal", &timing.accepted_to_terminal),
        (
            "timing.call_start_to_accepted",
            &timing.call_start_to_accepted,
        ),
    ] {
        histogram
            .validate()
            .map_err(|error| SchemaError::invalid_field(name, &error.to_string()))?;
    }
    if timing.call_start_to_accepted.total != o.accepted {
        return Err(SchemaError::invalid_field(
            "timing.call_start_to_accepted",
            &format!(
                "total {} does not equal accepted {}",
                timing.call_start_to_accepted.total, o.accepted
            ),
        ));
    }
    for (name, histogram) in [
        ("timing.intended_to_terminal", &timing.intended_to_terminal),
        ("timing.accepted_to_terminal", &timing.accepted_to_terminal),
    ] {
        if histogram.total != terminals {
            return Err(SchemaError::invalid_field(
                name,
                &format!(
                    "total {} does not equal terminal count {terminals}",
                    histogram.total
                ),
            ));
        }
    }
    validate_lateness(document, o)
}

/// Checks that scheduler lateness is present exactly when a schedule existed.
fn validate_lateness(document: &ProducerBenchmarkV2, o: OfferOutcomes) -> SchemaResult<()> {
    match (&document.load_mode, &document.timing.intended_to_call_start) {
        (LoadMode::ScheduledOpenLoopFixedRate, Some(lateness)) => {
            lateness.validate().map_err(|error| {
                SchemaError::invalid_field("timing.intended_to_call_start", &error.to_string())
            })?;
            if lateness.total != o.offered {
                return Err(SchemaError::invalid_field(
                    "timing.intended_to_call_start",
                    &format!(
                        "total {} does not equal offered {}",
                        lateness.total, o.offered
                    ),
                ));
            }
            Ok(())
        }
        (LoadMode::ScheduledOpenLoopFixedRate, None) => Err(SchemaError::invalid_field(
            "timing.intended_to_call_start",
            "scheduled open-loop measurement is missing scheduler lateness",
        )),
        (LoadMode::ClosedLoop, Some(_)) => Err(SchemaError::invalid_field(
            "timing.intended_to_call_start",
            "closed-loop measurement must not carry scheduler lateness",
        )),
        (LoadMode::ClosedLoop, None) => Ok(()),
    }
}
