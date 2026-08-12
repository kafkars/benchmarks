//! One attempt, from three TOML files to a sealed evidence bundle.
//!
//! `run`, `suite`, and `capacity` differ only in how many attempts they make and
//! what they do with the bundles afterwards. The attempt itself — read the
//! inputs, probe every subject, resolve twice, claim a workspace, capture the
//! environment, hand over to the sealing supervisor — is the same act every
//! time, and it lives here so that a repetition and a capacity probe cannot
//! drift into being different kinds of run than the one `benchctl run` makes.
//!
//! # Nothing here returns early past the workspace
//!
//! [`attempt`] answers with an [`AttemptEnd`], never with a bare error. Once the
//! workspace exists, every ending — a control-plane error, a caught panic, a
//! subject that timed out — is a sealed bundle plus the exit code that ending
//! maps to. [`AttemptEnd::Unsealable`] is reserved for the failures that happen
//! before there is anywhere to seal into: unreadable inputs, or a results
//! directory that could not be claimed.
//!
//! # Layout
//!
//! - `inputs` — [`CommonArguments`], [`LoadedInputs`], and reading the three
//!   documents off disk.
//! - `outcome` — [`AttemptEnd`], [`SealedAttempt`], and reading a bundle back.
//! - `driver` — [`attempt`]: the workspace, the panic boundary, the seal.
//! - `resolution` — probing and the two resolution passes.

mod driver;
mod inputs;
mod outcome;
mod resolution;

pub use self::driver::attempt;
pub use self::inputs::{CommonArguments, LoadedInputs, load, read};
pub use self::outcome::{AttemptEnd, SealedAttempt, read_classification, read_status};
pub use self::resolution::{probe_and_resolve, scratch_directory, write_document};
