//! What this adapter says it is: the name, the schema ids it writes, and the
//! capability document `describe` prints.
//!
//! Every claim here is checked against `producer.rs` and against the verdict
//! rules next door, because a capability document is a promise a control plane
//! plans a comparison around.

use std::error::Error;

use bench_schema::{AdapterCapabilities, AdapterDescription, LoadMode, ProducerBenchmarkV2};

use crate::producer;

use super::documents::print_document;

/// Adapter name, matching the subject name the legacy harness used.
pub(crate) const ADAPTER_NAME: &str = "kafkars";

/// Result document this adapter writes on the protocol path, in both load
/// modes.
///
/// One schema for both, because v2 describes offers rather than phases: a
/// closed-loop and a fixed-rate measurement differ in whether their offers had
/// a schedule, which the document says in `load_mode` and in the presence of
/// `timing.intended_to_call_start`, not in its shape.
pub(crate) const RESULT_SCHEMA: &str = ProducerBenchmarkV2::SCHEMA;

/// Result document the legacy `produce` verb still prints on stdout.
pub(crate) const LEGACY_CLOSED_LOOP_RESULT_SCHEMA: &str = "kafkars.producer-benchmark.v1";

/// Result document the legacy `produce-fixed` verb still prints on stdout.
pub(crate) const LEGACY_FIXED_RATE_RESULT_SCHEMA: &str = "kafkars.producer-fixed-load.v1";

/// Prints the capability document.
pub(crate) fn describe() -> Result<(), Box<dyn Error>> {
    print_document(&description())
}

/// Returns the capability document for this adapter.
///
/// Every claim here is checked against `producer.rs`: the client is built with
/// idempotence and `acks=all`, it never negotiates TLS, it has no transactional
/// call, and it compresses nothing. The version is the adapter's own, because
/// the adapter and the client it wraps are versioned together in this
/// repository's sibling layout.
pub(crate) fn description() -> AdapterDescription {
    let mut result_schemas = std::collections::BTreeMap::new();
    result_schemas.insert(LoadMode::ClosedLoop, RESULT_SCHEMA.to_owned());
    result_schemas.insert(
        LoadMode::ScheduledOpenLoopFixedRate,
        RESULT_SCHEMA.to_owned(),
    );
    AdapterDescription {
        schema: AdapterDescription::SCHEMA.to_owned(),
        name: ADAPTER_NAME.to_owned(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        capabilities: AdapterCapabilities {
            producer: true,
            consumer: false,
            idempotence: true,
            transactions: false,
            tls: false,
            compression: vec!["none".to_owned()],
            completion_modes: vec![producer::V2_COMPLETION_MODE.to_owned()],
            // Records are handed to the client as owned `Bytes`, so admission
            // moves the buffer rather than copying it. The second entry is the
            // narrower thing the v2 path declares having done: the same owned
            // handoff, with the bytes copied from a pool built before the
            // measured interval. A reader who checks a measurement's
            // `declared.ownership` against this list must find it here.
            ownership_modes: vec![
                "owned-handoff".to_owned(),
                producer::V2_OWNERSHIP.to_owned(),
            ],
            metric_families: vec![
                "latency".to_owned(),
                "throughput".to_owned(),
                "native-metrics".to_owned(),
            ],
        },
        result_schemas: Some(result_schemas),
    }
}
