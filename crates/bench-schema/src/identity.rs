//! The experiment identity: what makes two runs runs *of the same thing*.
//!
//! The experiment id is the sha-256 of the canonical bytes of the resolved
//! experiment with exactly two kinds of key removed:
//!
//! 1. the top-level `runtime` key, and
//! 2. every subject's `command` key.
//!
//! Both exclusions answer the same question — "would a reader consider this a
//! different experiment?" — and both answers are no. A run against a different
//! bootstrap, with different topic names and a different run id, is the same
//! experiment executed again; that is the entire point of having an id to
//! aggregate by. A subject invoked through `./target/release/adapter` rather
//! than `/usr/local/bin/adapter` is the same subject; what the binary actually
//! was is recorded in the subjects lock, where it belongs, together with its
//! digest.
//!
//! The exclusion is golden-tested rather than merely documented, because it is
//! exactly the kind of rule that drifts: someone adds a field to
//! [`RuntimeBinding`](crate::RuntimeBinding), the id changes for every historic
//! experiment, and nothing fails until a comparison silently stops matching.
//!
//! This module is the only place in the crate that uses `sha2`.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::canon;
use crate::error::{SchemaError, SchemaResult};
use crate::experiment::ResolvedExperiment;

/// Characters in a full sha-256 hex digest.
pub const DIGEST_HEX_LENGTH: usize = 64;

/// Characters in the shortened form used for directory names.
pub const SHORT_ID_LENGTH: usize = 16;

/// Returns the lowercase hex sha-256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut hex = String::with_capacity(DIGEST_HEX_LENGTH);
    for byte in digest {
        hex.push_str(HEX_DIGITS[usize::from(byte >> 4)]);
        hex.push_str(HEX_DIGITS[usize::from(byte & 0x0f)]);
    }
    hex
}

const HEX_DIGITS: [&str; 16] = [
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f",
];

/// Reports whether `text` is a full lowercase hex sha-256 digest.
pub fn is_digest_hex(text: &str) -> bool {
    text.len() == DIGEST_HEX_LENGTH && text.chars().all(is_lowercase_hex_digit)
}

fn is_lowercase_hex_digit(character: char) -> bool {
    character.is_ascii_digit() || matches!(character, 'a'..='f')
}

/// A validated experiment id: sixty-four lowercase hexadecimal characters.
///
/// Serializes as the bare string, so it can stand in for a `String` field in
/// any document without changing the bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct ExperimentId(String);

impl ExperimentId {
    /// Parses a full hex digest into an experiment id.
    pub fn parse(text: &str) -> SchemaResult<Self> {
        if !is_digest_hex(text) {
            return Err(SchemaError::identity(format!(
                "an experiment id must be {DIGEST_HEX_LENGTH} lowercase hexadecimal characters, \
                 found {text:?}"
            )));
        }
        Ok(Self(text.to_owned()))
    }

    /// Returns the full hex digest.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the first [`SHORT_ID_LENGTH`] characters, which is what names
    /// the results directory for this experiment.
    pub fn short(&self) -> &str {
        &self.0[..SHORT_ID_LENGTH]
    }
}

impl TryFrom<String> for ExperimentId {
    type Error = SchemaError;

    fn try_from(text: String) -> SchemaResult<Self> {
        Self::parse(&text)
    }
}

impl core::fmt::Display for ExperimentId {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Returns the document the experiment id is computed over: the resolved
/// experiment minus `runtime` and minus every subject's `command`.
///
/// Exposed because a reader who wants to know *why* two runs share an id should
/// be able to diff the documents that were actually hashed.
pub fn identity_document(experiment: &ResolvedExperiment) -> SchemaResult<Value> {
    let mut document = canon::to_canonical_value(experiment)?;
    let object = document.as_object_mut().ok_or_else(|| {
        SchemaError::identity("a resolved experiment must serialize as a JSON object")
    })?;
    object.remove("runtime");
    if let Some(subjects) = object.get_mut("subjects").and_then(Value::as_array_mut) {
        for subject in subjects {
            if let Some(subject) = subject.as_object_mut() {
                subject.remove("command");
            }
        }
    }
    Ok(document)
}

/// Returns the canonical bytes the experiment id is the digest of.
pub fn identity_bytes(experiment: &ResolvedExperiment) -> SchemaResult<Vec<u8>> {
    canon::canonical_bytes_of_value(&identity_document(experiment)?)
}

/// Computes the experiment id of a resolved experiment.
///
/// The experiment is validated first: an incoherent document must not be given
/// an identity, because the identity is what a future reader will trust.
pub fn experiment_id(experiment: &ResolvedExperiment) -> SchemaResult<ExperimentId> {
    experiment.validate()?;
    let bytes = identity_bytes(experiment)?;
    ExperimentId::parse(&sha256_hex(&bytes))
}
