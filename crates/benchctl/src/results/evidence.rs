//! What one subject's own documents say, read out of the bundle.
//!
//! # Why v2 and only v2
//!
//! The protocol path reads `adapters/<subject>/result.json` as
//! [`ProducerBenchmarkV2`]. That document carries the four-timestamp offer model
//! and the accounting invariants that make "the run drained" a checkable
//! statement rather than a hope, so parsing it *is* half the verdict: a document
//! whose offers do not add up cannot be read at all. The lenient v1 view
//! ([`KnownProducerResult`](bench_schema::KnownProducerResult)) is no longer part
//! of classification; it survives in the schema crate for the legacy stdout
//! flows, which keep emitting v1 forever.

use bench_schema::{AdapterOutcome, AdapterStatus, Histogram, ProducerBenchmarkV2};

use crate::attempt::AttemptPaths;

/// The quantile every headline latency in this crate is taken at.
pub const HEADLINE_QUANTILE: f64 = 0.99;

/// What one subject's own evidence says once it has run.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SubjectEvidence {
    /// The adapter's terminal status, when it wrote a readable one.
    pub adapter_status: Option<AdapterStatus>,
    /// The adapter's v2 measurement document, when it wrote a readable one.
    pub result: Option<ProducerBenchmarkV2>,
    /// Whether a result document was found where one was expected.
    pub result_present: bool,
    /// Why the result document present on disk could not be used.
    pub result_error: Option<String>,
    /// Problems noticed while reading, in the order they were noticed.
    pub notes: Vec<String>,
}

impl SubjectEvidence {
    /// The outcome the adapter claimed for itself.
    #[must_use]
    pub fn adapter_outcome(&self) -> Option<AdapterOutcome> {
        self.adapter_status.as_ref().map(|status| status.outcome)
    }

    /// Acknowledged records per second over the measured interval.
    #[must_use]
    pub fn goodput(&self) -> Option<f64> {
        self.result
            .as_ref()
            .map(|result| result.throughput.acknowledged_records_per_second)
    }

    /// The 99th percentile of `terminal - intended`, derived from the histogram.
    ///
    /// This is the latency a reader should quote: it includes admission wait and
    /// scheduler lateness, so a client that absorbed backpressure by making the
    /// application wait cannot hide that wait outside the measurement.
    #[must_use]
    pub fn intended_to_terminal_p99_ns(&self) -> Option<u64> {
        let result = self.result.as_ref()?;
        let histogram = Histogram::decode(&result.timing.intended_to_terminal).ok()?;
        histogram.value_at_quantile(HEADLINE_QUANTILE)
    }
}

/// Reads one subject's adapter output out of the bundle.
///
/// Never fails: an unreadable or malformed document is a fact about the attempt,
/// recorded as a note, not a reason to abandon the seal.
#[must_use]
pub fn read_subject(paths: &AttemptPaths, subject: &str) -> SubjectEvidence {
    let mut evidence = SubjectEvidence::default();
    let status_path = paths.adapter_status_json(subject);
    if status_path.exists() {
        match std::fs::read(&status_path) {
            Ok(bytes) => match bench_schema::parse_json_slice::<AdapterStatus>(&bytes) {
                Ok(status) => evidence.adapter_status = Some(status),
                Err(error) => evidence.notes.push(format!(
                    "the adapter status document is unreadable: {error}"
                )),
            },
            Err(error) => evidence.notes.push(format!(
                "the adapter status document could not be read: {error}"
            )),
        }
    }
    let result_path = paths.adapter_result_json(subject);
    if result_path.exists() {
        evidence.result_present = true;
        match std::fs::read(&result_path) {
            Ok(bytes) => match ProducerBenchmarkV2::from_slice(&bytes) {
                Ok(result) => evidence.result = Some(result),
                Err(error) => {
                    evidence.result_error = Some(error.to_string());
                    evidence
                        .notes
                        .push(format!("the result document is unreadable: {error}"));
                }
            },
            Err(error) => {
                evidence.result_error = Some(error.to_string());
                evidence
                    .notes
                    .push(format!("the result document could not be read: {error}"));
            }
        }
    }
    evidence
}
