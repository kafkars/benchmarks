//! The capability document: what the unmodified C benchmark binary can
//! actually do, stated conservatively.
//!
//! Every flag here is a claim somebody will rely on when a subject is missing
//! from a comparison, so the rule is to describe the C program as it is
//! compiled today rather than as librdkafka is capable of being configured.
//! librdkafka supports TLS, transactions, and four compression codecs;
//! `adapters/librdkafka-c/producer.c` configures none of them, and
//! `scripts/bootstrap-librdkafka` builds the library with SSL, GSSAPI, curl,
//! and the external compression libraries disabled. Claiming those capabilities
//! would produce a subject that accepts an experiment and then measures a
//! different one.
//!
//! The version is the pinned anchor, not a runtime query. The C program prints
//! `rd_kafka_version_str()` into its own result document, which is where the
//! actually-linked version belongs; a describe that shelled out to the binary
//! to ask would make the capability document depend on the binary being present
//! and runnable, which is exactly what `describe` is used to find out.

use std::collections::BTreeMap;

use bench_schema::{AdapterCapabilities, AdapterDescription, LoadMode};

/// Adapter name, matching the subject name the legacy harness used.
pub(crate) const ADAPTER_NAME: &str = "librdkafka-c";

/// The pinned librdkafka release this repository builds and measures.
pub(crate) const LIBRDKAFKA_VERSION: &str = "2.15.0";

/// Result document the C program writes for either load mode.
///
/// One schema now covers both, because the v2 document names its own load mode
/// instead of encoding it in the schema id. The legacy stdout verbs still print
/// `kafkars.producer-benchmark.v1` and `kafkars.producer-fixed-load.v1`, but
/// nothing on the protocol path asks for those any more: `run` drives the
/// `--v2-output` shape, which writes `result.json` itself.
pub(crate) const RESULT_SCHEMA: &str = "kafkars.producer-benchmark.v2";

/// Returns the capability document for the pinned C adapter.
pub(crate) fn description() -> AdapterDescription {
    let mut result_schemas = BTreeMap::new();
    result_schemas.insert(LoadMode::ClosedLoop, RESULT_SCHEMA.to_owned());
    result_schemas.insert(
        LoadMode::ScheduledOpenLoopFixedRate,
        RESULT_SCHEMA.to_owned(),
    );
    AdapterDescription {
        schema: AdapterDescription::SCHEMA.to_owned(),
        name: ADAPTER_NAME.to_owned(),
        version: LIBRDKAFKA_VERSION.to_owned(),
        capabilities: AdapterCapabilities {
            producer: true,
            // No consumer path exists in the C sources; the verifier is a
            // separate program outside the adapter protocol.
            consumer: false,
            // `enable.idempotence=true` is set unconditionally in producer.c.
            idempotence: true,
            // Nothing in the C sources calls a transactional API.
            transactions: false,
            // The pinned build disables SSL, and the argv surface has no way to
            // name a certificate.
            tls: false,
            // `compression.type=none` is hard-coded.
            compression: vec!["none".to_owned()],
            // Delivery reports are the only completion signal the C program
            // waits on, aggregated per produced batch.
            completion_modes: vec!["aggregate-batch-terminal".to_owned()],
            // Both phases produce with `RD_KAFKA_MSG_F_COPY`, so the client
            // copies every payload in and the caller keeps its buffer.
            ownership_modes: vec!["copy-in".to_owned()],
            metric_families: vec![
                "latency".to_owned(),
                "throughput".to_owned(),
                "librdkafka-statistics".to_owned(),
            ],
        },
        result_schemas: Some(result_schemas),
    }
}
