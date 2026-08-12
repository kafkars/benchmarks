//! The identifier of one attempt.
//!
//! An attempt id is `<utc-compact-seconds>-<8 hex>`: sortable by wall-clock
//! start, made unique by hashing process id, nanosecond clock, a monotonic
//! counter, and an ASLR-derived address — entropy without an RNG dependency
//! and without `unsafe`. Uniqueness is ultimately enforced by directory
//! creation, not by the id: claiming an attempt directory that already exists
//! is a hard error, never a reuse.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bench_schema::sha256_hex;

use crate::error::{CtlError, CtlResult};
use crate::time::utc_compact_seconds;

/// Length of the compact UTC prefix of an attempt id.
const COMPACT_TIME_LENGTH: usize = 16;
/// Length of the entropy suffix of an attempt id.
const ENTROPY_HEX_LENGTH: usize = 8;

static ATTEMPT_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Identifier of one attempt: `20260812T140305Z-1a2b3c4d`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptId(String);

impl AttemptId {
    /// Generates a fresh id for an attempt starting at `now`.
    #[must_use]
    pub fn generate(now: SystemTime) -> Self {
        let counter = ATTEMPT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = now
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos();
        let marker = 0u8;
        let address = std::ptr::from_ref(&marker).addr();
        let seed = format!("{}|{nanos}|{counter}|{address}", std::process::id());
        let digest = sha256_hex(seed.as_bytes());
        let entropy = &digest[..ENTROPY_HEX_LENGTH];
        Self(format!("{}-{entropy}", utc_compact_seconds(now)))
    }

    /// Validates and wraps an attempt id in its canonical text form.
    ///
    /// # Errors
    ///
    /// Returns an invalid-experiment error when the text does not have the
    /// `<utc-compact-seconds>-<8 lowercase hex>` shape.
    pub fn parse(text: &str) -> CtlResult<Self> {
        let expected_length = COMPACT_TIME_LENGTH + 1 + ENTROPY_HEX_LENGTH;
        let bytes = text.as_bytes();
        let shape_ok = bytes.len() == expected_length
            && bytes.get(COMPACT_TIME_LENGTH) == Some(&b'-')
            && bytes[..COMPACT_TIME_LENGTH]
                .iter()
                .all(|b| b.is_ascii_digit() || *b == b'T' || *b == b'Z')
            && bytes[COMPACT_TIME_LENGTH + 1..]
                .iter()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b));
        if shape_ok {
            Ok(Self(text.to_owned()))
        } else {
            Err(CtlError::invalid(format!("malformed attempt id: {text:?}")))
        }
    }

    /// The id's canonical text form.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for AttemptId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
