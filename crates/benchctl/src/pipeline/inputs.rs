//! The three input documents every attempt-making verb reads, and the scenario
//! text that gets sealed verbatim.

use std::path::{Path, PathBuf};

use bench_schema::{ClusterProfile, SourceExperiment, SubjectsFile};

use crate::error::{CtlError, CtlResult};

/// The inputs every attempt-making verb shares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommonArguments {
    /// Path to the scenario TOML.
    pub experiment: PathBuf,
    /// Path to the subject list TOML.
    pub subjects: PathBuf,
    /// Path to the cluster profile TOML.
    pub cluster: PathBuf,
    /// Bootstrap servers this attempt binds to.
    pub bootstrap: String,
    /// Seed override; `None` keeps the scenario's payload seed.
    pub seed: Option<u64>,
    /// Execution order override, in the order given.
    pub order: Option<Vec<String>>,
}

/// The three input documents, plus the scenario text that gets sealed verbatim.
#[derive(Debug, Clone)]
pub struct LoadedInputs {
    /// The scenario exactly as authored, sealed byte for byte.
    pub source_toml: String,
    /// The parsed scenario.
    pub source: SourceExperiment,
    /// The parsed subject list.
    pub subjects: SubjectsFile,
    /// The parsed cluster profile.
    pub cluster: ClusterProfile,
}

/// Reads and parses the three input documents.
///
/// # Errors
///
/// Returns an invalid-experiment error when a file cannot be read, does not
/// parse, or declares a subject role the vocabulary does not know.
pub fn load(common: &CommonArguments) -> CtlResult<LoadedInputs> {
    let source_toml = read(&common.experiment)?;
    let subjects_toml = read(&common.subjects)?;
    let cluster_toml = read(&common.cluster)?;
    let subjects = SubjectsFile::from_toml_str(&subjects_toml)?;
    // A misspelled role would quietly demote a subject to unlabeled, which is
    // the failure `deny_unknown_fields` exists to prevent one level down.
    subjects.validate()?;
    Ok(LoadedInputs {
        source: SourceExperiment::from_toml_str(&source_toml)?,
        subjects,
        cluster: ClusterProfile::from_toml_str(&cluster_toml)?,
        source_toml,
    })
}

/// Reads a file as text, naming it in the error.
pub fn read(path: &Path) -> CtlResult<String> {
    std::fs::read_to_string(path)
        .map_err(|error| CtlError::invalid(format!("read {}: {error}", path.display())))
}
