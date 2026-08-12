//! Attempt identity and the on-disk layout of one evidence bundle.
//!
//! Two facts, kept apart because they fail differently. An [`AttemptId`] is
//! minted from a clock and some entropy and can be minted anywhere; an
//! [`AttemptPaths`] is a directory that had to be claimed, and claiming it is
//! what actually enforces uniqueness.
//!
//! - `id` — [`AttemptId`], its shape, and how one is generated.
//! - `paths` — [`AttemptPaths`], every file name a bundle holds, and the
//!   pending-to-finalized move.

mod id;
mod paths;

pub use self::id::AttemptId;
pub use self::paths::{AttemptPaths, PENDING_DIR};
