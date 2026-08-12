//! The reviewed document: which scenarios belong to one cadence, and how many
//! attempts of each the pack asks for.
//!
//! Scenario paths are used exactly as the manifest writes them, which the pack
//! files document as repository-root-relative. `benchctl pack` therefore runs
//! from the repository root, and a manifest that moved would not silently
//! resolve against its own directory.

use serde::{Deserialize, Serialize};

use crate::error::{CtlError, CtlResult};

/// One entry: which scenario, and how many attempts of it the pack asks for.
///
/// Unknown fields are refused for the same reason every other human-authored
/// document in this repository refuses them: a mistyped key in a reviewed
/// manifest is a scenario somebody believes is in the cadence and that nothing
/// is running.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackEntry {
    /// Path to a scenario TOML, as the manifest writes it.
    pub scenario: String,
    /// How many attempts of that scenario the pack asks for.
    pub repetitions: u32,
}

/// A reviewed statement of which scenarios belong to one cadence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackManifest {
    /// Identifier used in logs and report titles.
    pub name: String,
    /// Which schedule this pack belongs to; prose, not a trigger.
    pub cadence: String,
    /// What a reader should conclude from a complete run of the pack.
    pub description: String,
    /// The entries, in the order they run.
    ///
    /// Defaulted rather than required so that an absent list and an empty one
    /// reach the same explanation in [`PackManifest::validate`]; serde's
    /// "missing field" would be true and unhelpful.
    #[serde(default)]
    pub entries: Vec<PackEntry>,
}

impl PackManifest {
    /// Parses a manifest from its TOML text.
    ///
    /// # Errors
    ///
    /// Returns an invalid-experiment error when the text is not the manifest
    /// shape, or when an entry could not describe work to do.
    pub fn from_toml_str(text: &str) -> CtlResult<Self> {
        let manifest: Self = toml::from_str(text)
            .map_err(|error| CtlError::invalid(format!("pack manifest: {error}")))?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Checks the invariants a manifest can check on its own.
    ///
    /// # Errors
    ///
    /// Returns an error naming the first violated invariant.
    pub fn validate(&self) -> CtlResult<()> {
        if self.entries.is_empty() {
            return Err(CtlError::invalid(
                "a pack with no entries declares a cadence that runs nothing",
            ));
        }
        for (index, entry) in self.entries.iter().enumerate() {
            if entry.scenario.trim().is_empty() {
                return Err(CtlError::invalid(format!(
                    "entries[{index}].scenario is empty, so the entry names nothing to run"
                )));
            }
            if entry.repetitions == 0 {
                return Err(CtlError::invalid(format!(
                    "entries[{index}] asks for zero repetitions of {:?}, which is a way of \
                     writing down a scenario without running it",
                    entry.scenario
                )));
            }
        }
        Ok(())
    }
}
