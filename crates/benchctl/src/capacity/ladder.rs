//! The ladder itself.
//!
//! 1. **Bracket.** Probe the initial rate. If it satisfies, multiply by the
//!    growth factor until one does not, or until the configured maximum is
//!    reached. If it does not, divide by the growth factor until one does, or
//!    until the configured minimum is reached.
//! 2. **Bisect.** With a satisfied `low` and an unsatisfied `high`, halve the
//!    bracket until it is narrower than the resolution.
//! 3. **Confirm.** Repeat the surviving rate `repetitions_per_rate` times. A
//!    confirmation that misses turns a converged search into an unconverged one,
//!    because a rate that holds once and fails twice is not a capacity.
//!
//! [`MAX_PROBES`] bounds the whole thing. A pathological objective — one no rate
//! can satisfy, or one every rate can — must end the process, not occupy a
//! cluster indefinitely. Which subject a probe's verdict is taken from, and what
//! makes that verdict a satisfied one, are [`super::evaluate`]'s business.

use bench_schema::{CapacityProbe, CapacityStatus, SloSpec};

use crate::error::{CtlError, CtlResult};
use crate::pipeline::{self, AttemptEnd, LoadedInputs, SealedAttempt};

use super::bounds::SearchBounds;
use super::command::CapacityCommand;
use super::evaluate::{ProbeOutcome, evaluate};
use super::rewrite::probe_inputs;

/// Hard ceiling on how many attempts one search may make, confirmations
/// included.
pub const MAX_PROBES: usize = 24;

/// Everything one search accumulates.
pub(super) struct SearchState<'a> {
    pub(super) command: &'a CapacityCommand,
    pub(super) inputs: &'a LoadedInputs,
    pub(super) slo: SloSpec,
    pub(super) bounds: SearchBounds,
    pub(super) probes: Vec<CapacityProbe>,
    pub(super) confirmation: Vec<CapacityProbe>,
    /// Highest rate known to satisfy the objectives.
    pub(super) low: Option<u64>,
    /// Lowest rate known to violate them.
    pub(super) high: Option<u64>,
    /// The first sealed attempt, which names the report directory.
    pub(super) first: Option<SealedAttempt>,
    /// Why the search stopped where it did.
    pub(super) notes: Vec<String>,
}

impl<'a> SearchState<'a> {
    pub(super) fn new(
        command: &'a CapacityCommand,
        inputs: &'a LoadedInputs,
        slo: SloSpec,
        bounds: SearchBounds,
    ) -> Self {
        Self {
            command,
            inputs,
            slo,
            bounds,
            probes: Vec::new(),
            confirmation: Vec::new(),
            low: None,
            high: None,
            first: None,
            notes: Vec::new(),
        }
    }

    /// Walks the ladder and returns how it ended.
    pub(super) fn run(&mut self) -> CtlResult<CapacityStatus> {
        if !self.bracket()? {
            return Ok(CapacityStatus::Invalid);
        }
        if !self.bisect()? {
            return Ok(CapacityStatus::Invalid);
        }
        let Some(candidate) = self.low else {
            self.notes.push(format!(
                "no rate down to the configured floor of {} met the objectives",
                self.bounds.minimum
            ));
            return Ok(CapacityStatus::Unconverged);
        };
        let Some(upper) = self.high else {
            self.notes.push(format!(
                "every rate up to the configured ceiling of {} met the objectives, \
                 so the capacity is at or above it",
                self.bounds.maximum
            ));
            return Ok(CapacityStatus::Unconverged);
        };
        if upper - candidate > self.bounds.resolution(Some(upper)) {
            self.notes.push(format!(
                "the search ran out of probes with the bracket still [{candidate}, {upper}]"
            ));
            return Ok(CapacityStatus::Unconverged);
        }
        self.confirm(candidate)
    }

    /// Phase one: find a satisfied rate below an unsatisfied one.
    ///
    /// Returns false when a probe could not be read, which makes the whole
    /// search invalid rather than merely unconverged.
    fn bracket(&mut self) -> CtlResult<bool> {
        let mut rate = self.bounds.initial;
        let first = self.probe(rate)?;
        if !first.readable {
            return Ok(false);
        }
        let growing = first.record.satisfied;
        if growing {
            self.low = Some(rate);
        } else {
            self.high = Some(rate);
        }
        while self.probes.len() < MAX_PROBES {
            let next = if growing {
                let grown = rate.saturating_mul(self.bounds.growth_factor);
                grown.min(self.bounds.maximum)
            } else {
                (rate / self.bounds.growth_factor).max(self.bounds.minimum)
            };
            if next == rate {
                // The ladder has reached a configured bound and cannot move.
                break;
            }
            rate = next;
            let outcome = self.probe(rate)?;
            if !outcome.readable {
                return Ok(false);
            }
            // The direction is fixed by the first probe, so the first change of
            // answer is the bracket and the loop is done.
            if outcome.record.satisfied {
                self.low = Some(rate);
                if !growing {
                    break;
                }
            } else {
                self.high = Some(rate);
                if growing {
                    break;
                }
            }
        }
        Ok(true)
    }

    /// Phase two: halve the bracket until it is inside the resolution.
    fn bisect(&mut self) -> CtlResult<bool> {
        while let (Some(low), Some(high)) = (self.low, self.high) {
            if high - low <= self.bounds.resolution(Some(high)) || self.probes.len() >= MAX_PROBES {
                break;
            }
            let middle = low + (high - low) / 2;
            if middle == low || middle == high {
                break;
            }
            let outcome = self.probe(middle)?;
            if !outcome.readable {
                return Ok(false);
            }
            if outcome.record.satisfied {
                self.low = Some(middle);
            } else {
                self.high = Some(middle);
            }
        }
        Ok(true)
    }

    /// Phase three: repeat the surviving rate and require every repetition.
    fn confirm(&mut self, candidate: u64) -> CtlResult<CapacityStatus> {
        for repetition in 0..self.bounds.confirmations {
            if self.probes.len() + self.confirmation.len() >= MAX_PROBES {
                self.notes.push(format!(
                    "the probe budget of {MAX_PROBES} ran out after {repetition} confirmations"
                ));
                return Ok(CapacityStatus::Unconverged);
            }
            let outcome = self.run_probe(candidate)?;
            let satisfied = outcome.record.satisfied && outcome.readable;
            self.confirmation.push(outcome.record);
            if !satisfied {
                self.notes.push(format!(
                    "confirmation {} of {} did not hold at {candidate}",
                    repetition + 1,
                    self.bounds.confirmations
                ));
                return Ok(CapacityStatus::Unconverged);
            }
        }
        Ok(CapacityStatus::Converged)
    }

    /// Runs one probe and records it in the ladder.
    fn probe(&mut self, rate: u64) -> CtlResult<ProbeOutcome> {
        let outcome = self.run_probe(rate)?;
        self.probes.push(outcome.record.clone());
        Ok(outcome)
    }

    /// Runs one probe at `rate` without recording it.
    ///
    /// # Errors
    ///
    /// Returns an error only when nothing could be sealed at all; a probe that
    /// sealed a failure is a fact about that rate, and travels back as an
    /// unsatisfied, unreadable outcome.
    fn run_probe(&mut self, rate: u64) -> CtlResult<ProbeOutcome> {
        println!("==> probing {rate} records/s");
        let inputs = probe_inputs(self.inputs, rate)?;
        let end = pipeline::attempt(
            &inputs,
            &self.command.common,
            &self.command.results_root,
            self.command.budget,
        );
        let AttemptEnd::Sealed(sealed) = end else {
            return Err(match end {
                AttemptEnd::Unsealable(error) => error,
                AttemptEnd::Sealed(_) => CtlError::internal("unreachable sealed attempt"),
            });
        };
        if self.first.is_none() {
            self.first = Some(sealed.clone());
        }
        Ok(evaluate(
            &sealed,
            &self.subject_names(&sealed),
            &self.target(&sealed),
            rate,
            &self.slo,
        ))
    }
}
