//! Why a subject's evidence may not be believed.
//!
//! # The three gates a subject passes
//!
//! 1. **The process.** It ran, it was not killed, it exited zero.
//! 2. **The documents.** A readable status saying `succeeded`, and a readable v2
//!    result the adapter itself declared valid.
//! 3. **The accounting.** Complete drain — no unknown offers, nothing left
//!    outstanding — and a failure count within [`max_failed_records`], whatever
//!    the experiment declared. A scenario that states no objectives is not
//!    thereby allowed to lose records: it has stated no *latency* ceiling, and
//!    the legacy harness's rule that every record must be acknowledged was never
//!    conditional on one.

use bench_schema::{AdapterOutcome, PRODUCER_BENCHMARK_V2, SloSpec, SubjectValidity};

use super::outcome::{SubjectOutcome, VerificationVerdict};

/// The most failed-or-timed-out records any measurement may lose and still be
/// believed.
///
/// The schema's [`SloSpec`] states ceilings on latency, schedule delay, drain,
/// and native client counters, and says nothing about delivery failures. That
/// silence is not permission: a run that lost records measured a different thing
/// from the one it was asked to measure, and the legacy harness required every
/// record to be acknowledged regardless of what else a scenario declared. The
/// argument is kept so that a future `SloSpec` field can relax this per
/// experiment — where it would be hashed into the experiment id and a reader
/// could see that the run was allowed to lose records.
///
/// The number is [`bench_report::MAX_FAILED_RECORDS`] rather than a second
/// constant with the same value. Classification and the capacity search's
/// objective gate must agree about what "lost a record" costs; two constants
/// would be two chances to disagree, and the disagreement would show up as a
/// probe that satisfied its objectives inside a bundle classified invalid.
#[must_use]
pub const fn max_failed_records(_slo: &SloSpec) -> u64 {
    bench_report::MAX_FAILED_RECORDS
}

impl SubjectOutcome {
    /// Whether this subject's evidence may be believed, with every reason it
    /// may not.
    #[must_use]
    pub fn validity(&self) -> SubjectValidity {
        let mut reasons = self.process_reasons();
        reasons.extend(self.document_reasons());
        reasons.extend(self.provenance_reasons());
        reasons.extend(self.accounting_reasons());
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
                let detail = self
                    .evidence
                    .result_error
                    .as_deref()
                    .unwrap_or("no reason recorded");
                reasons.push(format!(
                    "the result document is not a readable {PRODUCER_BENCHMARK_V2}: {detail}"
                ));
            }
            None => reasons.push("the adapter wrote no result document".to_owned()),
            Some(result) => {
                if !result.valid {
                    let detail = result
                        .invalid_reason
                        .as_deref()
                        .unwrap_or("no reason given");
                    reasons.push(format!(
                        "the adapter did not declare its result valid: {detail}"
                    ));
                }
            }
        }
        reasons
    }

    /// Reasons drawn from comparing what the adapter *said* it was with what it
    /// turned out to be.
    ///
    /// `describe` is answered before the run, and its version is hashed into the
    /// experiment id: it is the string that decides which attempts are
    /// repetitions of one another. The result document's `adapter_version` is
    /// written during the run, by the client, and can therefore report a version
    /// the describe answer only guessed at — a wrapper around a shared library
    /// that names its own release rather than the library's, for instance, where
    /// upgrading the library changes the measurement and nothing else.
    ///
    /// When those two disagree, the experiment id is naming a client that did
    /// not run. Two upgrades' worth of attempts would pool under one id and the
    /// suite would compute a median across them. Saying so is a validity reason
    /// rather than a warning, because a median over two different clients is not
    /// a slightly noisier number — it is a number about nothing.
    fn provenance_reasons(&self) -> Vec<String> {
        let Some(result) = &self.evidence.result else {
            return Vec::new();
        };
        if self.declared_adapter_version.is_empty()
            || result.adapter_version == self.declared_adapter_version
        {
            return Vec::new();
        }
        vec![format!(
            "the adapter described itself as version {:?} but its result document reports {:?}; \
             the described version is what the experiment id is built from, so this attempt is \
             filed under a client that did not run it",
            self.declared_adapter_version, result.adapter_version
        )]
    }

    /// Reasons drawn from the v2 offer accounting: drain, and the failure
    /// ceiling every measurement is held to.
    fn accounting_reasons(&self) -> Vec<String> {
        let Some(result) = &self.evidence.result else {
            return Vec::new();
        };
        let mut reasons = Vec::new();
        if result.outcomes.unknown != 0 {
            reasons.push(format!(
                "the run did not drain: {} accepted offers reached no terminal state",
                result.outcomes.unknown
            ));
        }
        if result.queue.final_outstanding != 0 {
            reasons.push(format!(
                "the run did not drain: {} offers were still outstanding when it ended",
                result.queue.final_outstanding
            ));
        }
        // Unconditional, whatever the experiment declared. An empty `SloSpec`
        // means "no objective stated", not "any number of records may be lost":
        // the ceiling is zero either way, and asking only when objectives exist
        // made a scenario without them the most permissive one in the
        // repository. It also disagreed with `evaluate_slo`, which never had
        // that guard — so the same measurement could be classified valid here
        // and rejected by the capacity search's gate.
        let lost = result
            .outcomes
            .failed
            .saturating_add(result.outcomes.timed_out);
        let ceiling = max_failed_records(&self.slo);
        if lost > ceiling {
            reasons.push(format!(
                "{lost} records failed or timed out, above the {ceiling} a measurement may \
                 lose and still be believed"
            ));
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
