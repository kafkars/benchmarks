//! Deterministic public-call linearization shared by fixed-load callers.
//!
//! Four callers admitting into one client would otherwise interleave their
//! public calls differently on every run, so the batch the broker sees first
//! would be a property of the scheduler rather than of the schedule. Taking
//! turns by batch index makes the public call sequence the schedule's own, and
//! a caller that is refused keeps its turn rather than losing its place.
//!
//! Both fixed-rate phases share this one primitive: the legacy `fixed_phase`
//! and the v2 measured path linearize admission identically, because a
//! difference here would be a difference in what the two paths measure.

use std::{
    error::Error,
    sync::{Condvar, Mutex, MutexGuard},
};

#[derive(Debug)]
pub(in crate::producer) struct AdmissionTurn {
    state: Mutex<TurnState>,
    changed: Condvar,
}

#[derive(Debug, Default)]
struct TurnState {
    next: u64,
    failed: bool,
}

impl AdmissionTurn {
    pub(in crate::producer) fn new() -> Self {
        Self {
            state: Mutex::new(TurnState::default()),
            changed: Condvar::new(),
        }
    }

    pub(in crate::producer) fn wait(
        &self,
        index: u64,
    ) -> Result<AdmissionPermit<'_>, Box<dyn Error>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "fixed-load admission turn poisoned")?;
        loop {
            if state.failed {
                return Err("another fixed-load caller failed admission".into());
            }
            if state.next == index {
                return Ok(AdmissionPermit {
                    turn: self,
                    state: Some(state),
                    resolved: false,
                });
            }
            if state.next > index {
                return Err("fixed-load caller attempted an obsolete admission turn".into());
            }
            state = self
                .changed
                .wait(state)
                .map_err(|_| "fixed-load admission turn poisoned while waiting")?;
        }
    }
}

pub(in crate::producer) struct AdmissionPermit<'a> {
    turn: &'a AdmissionTurn,
    state: Option<MutexGuard<'a, TurnState>>,
    resolved: bool,
}

impl AdmissionPermit<'_> {
    pub(in crate::producer) fn complete(mut self) -> Result<(), Box<dyn Error>> {
        let state = self
            .state
            .as_mut()
            .ok_or("fixed-load admission permit lost its state")?;
        state.next = state
            .next
            .checked_add(1)
            .ok_or("fixed-load admission turn overflowed")?;
        self.resolved = true;
        self.turn.changed.notify_all();
        Ok(())
    }

    pub(in crate::producer) fn retry(mut self) {
        self.resolved = true;
    }
}

impl Drop for AdmissionPermit<'_> {
    fn drop(&mut self) {
        if !self.resolved {
            if let Some(state) = self.state.as_mut() {
                state.failed = true;
            }
            self.turn.changed.notify_all();
        }
    }
}
