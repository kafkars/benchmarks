//! Deterministic public-call linearization shared by fixed-load callers.

use std::{
    error::Error,
    sync::{Condvar, Mutex, MutexGuard},
};

#[derive(Debug)]
pub(super) struct AdmissionTurn {
    state: Mutex<TurnState>,
    changed: Condvar,
}

#[derive(Debug, Default)]
struct TurnState {
    next: u64,
    failed: bool,
}

impl AdmissionTurn {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(TurnState::default()),
            changed: Condvar::new(),
        }
    }

    pub(super) fn wait(&self, index: u64) -> Result<AdmissionPermit<'_>, Box<dyn Error>> {
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

pub(super) struct AdmissionPermit<'a> {
    turn: &'a AdmissionTurn,
    state: Option<MutexGuard<'a, TurnState>>,
    resolved: bool,
}

impl AdmissionPermit<'_> {
    pub(super) fn complete(mut self) -> Result<(), Box<dyn Error>> {
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

    pub(super) fn retry(mut self) {
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
