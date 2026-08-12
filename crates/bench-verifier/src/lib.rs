//! Typed views over the verification evidence that decides whether a benchmark
//! attempt is allowed to mean anything.
//!
//! Verification is deliberately not an adapter concern. An adapter reports what
//! it believes it produced; the verifier independently reads the topic back and
//! reports what the broker actually retained. Cross-client validity is decided
//! from the verifier's report, never from a subject's self-assessment, so no
//! adapter can vouch for itself.
//!
//! The verifier itself is currently the C program at
//! `adapters/librdkafka-c/verifier.c`, invoked by the control plane as a
//! configured tool. This crate owns only the *reading* side: the schema-checked
//! deserialization of the `kafkars.producer-verification.v1` document that
//! program prints on stdout. Porting the verifier to Rust is a later milestone;
//! when that happens the wire document is expected to stay byte-compatible,
//! because sealed bundles are immutable and old bundles must keep parsing.
#![forbid(unsafe_code)]

mod report;

pub use report::{PRODUCER_VERIFICATION_SCHEMA_V1, VerificationReport};

#[cfg(test)]
mod report_test;
