//! Reading subject evidence after execution: adapter status, lenient views of
//! result documents, and presence checks, feeding classification without ever
//! re-serializing adapter-owned bytes.
//!
//! This module answers the second of the two questions a sealed bundle must
//! answer. `status.json` says whether the machinery did what it was told;
//! `classification.json` says whether the numbers mean anything, and that is
//! decided here. The two are kept apart deliberately: a run can complete every
//! phase and be invalid, and a partial run can still hold one subject's perfectly
//! good evidence.
//!
//! Nothing here rewrites what an adapter wrote. A result document is captured as
//! bytes and sealed as those exact bytes; the lenient views extract the handful
//! of facts the gate needs and leave every other key untouched. Re-serializing
//! would drop the fields the view does not name and round-trip the adapter's
//! floating-point numbers through a second formatter, which is a way of altering
//! evidence while believing you are reading it.
//!
//! [`DEFERRED_CHECKS`] is the honest half of the verdict. Every check the legacy
//! Node harness performs that this milestone does not is named in the
//! classification document, so that a reader knows exactly what "valid" does not
//! cover here.

use bench_schema::{
    AdapterOutcome, AdapterStatus, Classification, Comparison, ComparisonPair, KnownProducerResult,
    PRODUCER_BENCHMARK_V1, ProcessExit, SubjectExecution, SubjectValidity, SubjectVerification,
};

use crate::attempt::AttemptPaths;

/// Checks this milestone does not perform, named in every classification.
///
/// These are the gates the legacy Node control plane still owns. Listing them by
/// name is the alternative to a footnote nobody reads: a bundle from this
/// milestone states what it did not check, in the same document that states what
/// it did.
pub const DEFERRED_CHECKS: [&str; 7] = [
    "bootstrap-confidence-intervals",
    "paired-repetition-suites",
    "latency-csv-row-validation",
    "native-metric-gates",
    "librdkafka-statistics-summaries",
    "compressed-payload-coverage",
    "process-resource-capture",
];

/// What one subject's own evidence says once it has run.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SubjectEvidence {
    /// The adapter's terminal status, when it wrote a readable one.
    pub adapter_status: Option<AdapterStatus>,
    /// The known fields of the adapter's result document, when it wrote one.
    pub result: Option<KnownProducerResult>,
    /// Whether a result document was found where one was expected.
    pub result_present: bool,
    /// Problems noticed while reading, in the order they were noticed.
    pub notes: Vec<String>,
}

impl SubjectEvidence {
    /// The outcome the adapter claimed for itself.
    #[must_use]
    pub fn adapter_outcome(&self) -> Option<AdapterOutcome> {
        self.adapter_status.as_ref().map(|status| status.outcome)
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
            Ok(bytes) => match KnownProducerResult::from_slice(&bytes) {
                Ok(result) => evidence.result = Some(result),
                Err(error) => evidence
                    .notes
                    .push(format!("the result document is unreadable: {error}")),
            },
            Err(error) => evidence
                .notes
                .push(format!("the result document could not be read: {error}")),
        }
    }
    evidence
}

/// What a read-back verification concluded about one topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationVerdict {
    /// Nothing was produced to this topic, so there was nothing to read back.
    NotRequired,
    /// A verification was expected and did not happen.
    NotRun,
    /// The verifier ran and the topic did not satisfy the contract.
    Failed,
    /// The verifier ran and the topic satisfied the contract.
    Satisfied,
}

impl VerificationVerdict {
    /// Reports whether this verdict permits the subject to be believed.
    #[must_use]
    pub const fn permits_validity(self) -> bool {
        matches!(self, Self::NotRequired | Self::Satisfied)
    }
}

/// Everything the verdict needs to know about one subject.
#[derive(Debug, Clone, PartialEq)]
pub struct SubjectOutcome {
    /// Subject name, matching the resolved experiment.
    pub name: String,
    /// How the adapter process ended; absent when it never ran.
    pub execution: Option<ProcessExit>,
    /// Whether the supervisor killed it in response to an interrupt.
    pub interrupted: bool,
    /// What the subject's own documents say.
    pub evidence: SubjectEvidence,
    /// Verification outcomes as the run status records them.
    pub verification: SubjectVerification,
    /// Whether the measured topic satisfied its contract.
    pub measured: VerificationVerdict,
    /// Whether the warmup topic satisfied its contract.
    pub warmup: VerificationVerdict,
}

impl SubjectOutcome {
    /// Creates an outcome for a subject that never ran.
    #[must_use]
    pub fn skipped(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            execution: None,
            interrupted: false,
            evidence: SubjectEvidence::default(),
            verification: SubjectVerification::default(),
            measured: VerificationVerdict::NotRun,
            warmup: VerificationVerdict::NotRun,
        }
    }

    /// The run-status view of this subject.
    #[must_use]
    pub fn execution_record(&self) -> SubjectExecution {
        SubjectExecution {
            name: self.name.clone(),
            execution: self.execution,
            adapter_outcome: self.evidence.adapter_outcome(),
            result_present: self.evidence.result_present,
            verification: self.verification,
        }
    }

    /// Whether this subject's evidence may be believed, with every reason it
    /// may not.
    #[must_use]
    pub fn validity(&self) -> SubjectValidity {
        let mut reasons = self.process_reasons();
        reasons.extend(self.document_reasons());
        reasons.extend(self.verification_reasons());
        SubjectValidity {
            name: self.name.clone(),
            valid: reasons.is_empty(),
            reasons,
        }
    }

    /// Reasons drawn from how the adapter process ended.
    fn process_reasons(&self) -> Vec<String> {
        let Some(exit) = self.execution else {
            return vec!["the adapter never ran".to_owned()];
        };
        let mut reasons = Vec::new();
        if exit.timed_out {
            reasons.push("the adapter exceeded its run deadline".to_owned());
        }
        if self.interrupted {
            reasons.push("the run was interrupted before the adapter finished".to_owned());
        }
        if let Some(signal) = exit.signal {
            if !exit.timed_out && !self.interrupted {
                reasons.push(format!("the adapter died on signal {signal}"));
            }
        }
        match exit.exit_code {
            Some(0) => {}
            Some(code) => reasons.push(format!("the adapter exited with code {code}")),
            None => {
                if !exit.timed_out && !self.interrupted && exit.signal.is_none() {
                    reasons.push("the adapter ended without an exit code".to_owned());
                }
            }
        }
        reasons
    }

    /// Reasons drawn from the documents the adapter wrote.
    fn document_reasons(&self) -> Vec<String> {
        let mut reasons = Vec::new();
        match self.evidence.adapter_outcome() {
            Some(AdapterOutcome::Succeeded) => {}
            Some(AdapterOutcome::Failed) => {
                let detail = self
                    .evidence
                    .adapter_status
                    .as_ref()
                    .and_then(|status| status.failure.as_ref())
                    .map_or_else(
                        || "the adapter reported failure".to_owned(),
                        |failure| {
                            format!(
                                "the adapter reported failure at {}: {}",
                                failure.stage, failure.reason
                            )
                        },
                    );
                reasons.push(detail);
            }
            None => reasons.push("the adapter wrote no readable status document".to_owned()),
        }
        match &self.evidence.result {
            None if self.evidence.result_present => {
                reasons.push("the result document could not be read".to_owned());
            }
            None => reasons.push("the adapter wrote no result document".to_owned()),
            Some(result) => {
                if !result.has_known_schema() {
                    reasons.push(format!(
                        "the result document declares {}, which is not a producer result",
                        result.schema.as_deref().unwrap_or("no schema")
                    ));
                }
                if !result.adapter_declared_valid() {
                    reasons.push("the adapter did not declare its result valid".to_owned());
                }
            }
        }
        reasons
    }

    /// Reasons drawn from read-back verification.
    fn verification_reasons(&self) -> Vec<String> {
        let mut reasons = Vec::new();
        for (phase, verdict) in [("measured", self.measured), ("warmup", self.warmup)] {
            match verdict {
                VerificationVerdict::NotRequired | VerificationVerdict::Satisfied => {}
                VerificationVerdict::NotRun => {
                    reasons.push(format!("the {phase} topic was never verified"));
                }
                VerificationVerdict::Failed => {
                    reasons.push(format!(
                        "the {phase} topic did not satisfy the verification contract"
                    ));
                }
            }
        }
        reasons
    }
}

/// Builds the attempt's classification from its subjects.
///
/// `extra_reasons` carries facts the subjects cannot see — a failed topic
/// creation, an interrupt, a phase that never ran — which invalidate the run as
/// a whole even when a subject's own documents look fine.
#[must_use]
pub fn classify(subjects: &[SubjectOutcome], extra_reasons: &[String]) -> Classification {
    let verdicts: Vec<SubjectValidity> = subjects.iter().map(SubjectOutcome::validity).collect();
    let mut reasons: Vec<String> = extra_reasons.to_vec();
    if subjects.is_empty() {
        reasons.push("the attempt ran no subjects".to_owned());
    }
    for verdict in &verdicts {
        for reason in &verdict.reasons {
            reasons.push(format!("{}: {reason}", verdict.name));
        }
    }
    Classification {
        schema: Classification::SCHEMA.to_owned(),
        run_valid: reasons.is_empty(),
        // Never true in this milestone: the deferred checks below are exactly
        // the evidence a published claim would need.
        claim_eligible: false,
        subjects: verdicts,
        deferred_checks: DEFERRED_CHECKS
            .iter()
            .map(|check| (*check).to_owned())
            .collect(),
        reasons,
    }
}

/// Builds the attempt's comparison document.
///
/// Subjects are comparable only when every one of them produced a
/// `kafkars.producer-benchmark.v1` result: ratios between documents of different
/// shapes would be comparing different measurements. The baseline is the first
/// subject in execution order, and every later subject is a candidate against
/// it, so a ratio above one on goodput and below one on latency both favour the
/// candidate.
#[must_use]
#[expect(
    clippy::cast_precision_loss,
    reason = "latency percentiles are nanosecond counts far below the f64 integer limit, and a \
              ratio is evidence rather than identity"
)]
pub fn compare(order: &[String], subjects: &[SubjectOutcome]) -> Comparison {
    let mut reasons = Vec::new();
    let ordered: Vec<&SubjectOutcome> = order
        .iter()
        .filter_map(|name| subjects.iter().find(|subject| &subject.name == name))
        .collect();
    if ordered.len() < 2 {
        reasons.push("a comparison needs at least two subjects that ran".to_owned());
    }
    for subject in &ordered {
        let schema = subject
            .evidence
            .result
            .as_ref()
            .and_then(|result| result.schema.clone());
        if schema.as_deref() != Some(PRODUCER_BENCHMARK_V1) {
            reasons.push(format!(
                "{} produced {} rather than {PRODUCER_BENCHMARK_V1}",
                subject.name,
                schema.unwrap_or_else(|| "no result".to_owned())
            ));
        }
    }
    let comparable = reasons.is_empty();
    let mut pairs = Vec::new();
    if comparable {
        if let Some((baseline, candidates)) = ordered.split_first() {
            for candidate in candidates {
                pairs.push(ComparisonPair {
                    baseline: baseline.name.clone(),
                    candidate: candidate.name.clone(),
                    acknowledged_goodput_ratio: ratio(goodput(candidate), goodput(baseline)),
                    p99_latency_ratio: ratio(
                        p99(candidate).map(|value| value as f64),
                        p99(baseline).map(|value| value as f64),
                    ),
                });
            }
        }
    }
    Comparison {
        schema: Comparison::SCHEMA.to_owned(),
        comparable,
        pairs,
        reasons,
    }
}

/// The acknowledged goodput a subject reported, when it reported one.
fn goodput(subject: &SubjectOutcome) -> Option<f64> {
    subject
        .evidence
        .result
        .as_ref()
        .and_then(|result| result.acknowledged_records_per_second)
}

/// The headline 99th percentile a subject reported, when it reported one.
fn p99(subject: &SubjectOutcome) -> Option<u64> {
    subject
        .evidence
        .result
        .as_ref()
        .and_then(KnownProducerResult::headline_p99_ns)
}

/// Divides two measurements, refusing anything that would not be a number.
fn ratio(candidate: Option<f64>, baseline: Option<f64>) -> Option<f64> {
    let candidate = candidate?;
    let baseline = baseline?;
    if baseline <= 0.0 || !candidate.is_finite() || !baseline.is_finite() {
        return None;
    }
    Some(candidate / baseline)
}
