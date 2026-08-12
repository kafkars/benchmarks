//! Interrupt handling: SIGINT/SIGTERM latched into an atomic flag via
//! signal-hook so a run can stop between polls and still seal partial
//! evidence.
//!
//! A signal handler may do almost nothing safely, so it does almost nothing: it
//! sets one atomic boolean. Everything else — killing children, reaping them,
//! writing the bundle — happens on the ordinary control flow, which polls the
//! latch between `try_wait` calls. That is the whole reason `signal-hook` is in
//! the dependency set: `unsafe_code` is forbidden here, and installing a handler
//! by hand is unsafe by definition.
//!
//! The latch is process-wide and installed at most once. Two attempts in one
//! process share it, which is correct — a Ctrl-C is aimed at the process, not at
//! a phase — and registering the same signal twice would leave a handler behind
//! that outlives the attempt that installed it.
//!
//! Registration failure is reported and then ignored. A control plane that
//! refuses to run because it could not arrange to be interrupted politely is
//! worse than one that runs and dies impolitely: the always-seal guard still
//! writes a bundle either way.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use signal_hook::consts::{SIGINT, SIGTERM};

/// The signals that ask a run to stop early.
const LATCHED_SIGNALS: [i32; 2] = [SIGINT, SIGTERM];

/// A latch that is raised when the process is asked to stop.
///
/// Cloning shares the latch rather than copying its value, so a clone handed to
/// a signal handler and a clone polled by the supervisor see the same flag.
#[derive(Debug, Clone)]
pub struct InterruptFlag {
    latch: Arc<AtomicBool>,
}

impl InterruptFlag {
    /// Creates a flag that nothing but the caller can raise.
    ///
    /// Used by tests and by any code path that wants the polling behavior
    /// without installing a signal handler.
    #[must_use]
    pub fn unarmed() -> Self {
        Self {
            latch: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Reports whether an interrupt has been latched.
    #[must_use]
    pub fn is_set(&self) -> bool {
        self.latch.load(Ordering::SeqCst)
    }

    /// Raises the latch, as a signal handler would.
    pub fn raise(&self) {
        self.latch.store(true, Ordering::SeqCst);
    }

    /// The shared latch, for handing to `signal-hook`.
    fn shared(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.latch)
    }
}

static PROCESS_LATCH: OnceLock<InterruptFlag> = OnceLock::new();

/// Returns the process-wide interrupt flag, installing the SIGINT and SIGTERM
/// handlers the first time it is called.
///
/// Subsequent calls return the same flag without touching the signal
/// disposition, so an attempt started after an interrupt already arrived sees
/// the latch already raised — which is the honest answer, not a fresh start.
#[must_use]
pub fn process_latch() -> InterruptFlag {
    PROCESS_LATCH
        .get_or_init(|| {
            let flag = InterruptFlag::unarmed();
            for signal in LATCHED_SIGNALS {
                if let Err(error) = signal_hook::flag::register(signal, flag.shared()) {
                    eprintln!(
                        "benchctl: could not latch signal {signal} ({error}); \
                         an interrupt will not seal partial evidence"
                    );
                }
            }
            flag
        })
        .clone()
}
