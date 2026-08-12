//! Everything the resolution is given, and nothing it is not.

use std::collections::BTreeMap;

use bench_schema::{
    AdapterDescription, BudgetSpec, ClusterProfile, SourceExperiment, SubjectEntry, ValidateReport,
};

/// One subject as the control plane found it: what the operator asked for, and
/// what the adapter said when asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectProbe {
    /// The subjects-file entry verbatim: name and argument vector.
    pub subject: SubjectEntry,
    /// The capability document the adapter printed for `describe`.
    pub describe: AdapterDescription,
    /// The adapter's verdict on this experiment.
    ///
    /// `None` means the subject has not been asked yet. That state is real:
    /// `validate` takes the resolved experiment as a file, so the document has
    /// to exist before the answer does. [`resolve_experiment`](super::resolve_experiment)
    /// accepts it; [`resolve_pure`](super::resolve_pure) does not, because a
    /// lock without a verdict would record that nobody checked.
    pub validate: Option<ValidateReport>,
    /// Digest of the program file, when it could be read.
    pub binary_sha256: Option<String>,
    /// Digest of every argument that named a readable regular file, keyed by
    /// the argument. Empty for the ordinary single-binary subject.
    pub argument_binary_sha256s: BTreeMap<String, String>,
}

/// The attempt-specific facts the identity ignores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeInputs {
    /// Bootstrap servers the subjects will connect to.
    pub bootstrap: String,
    /// Attempt id, which together with the experiment id fixes the run id.
    pub attempt_id: String,
    /// Execution order override, subject names in the order the operator asked
    /// for. `None` keeps the subjects-file order.
    pub order: Option<Vec<String>>,
}

/// Everything [`resolve_pure`](super::resolve_pure) needs, and nothing it does
/// not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveInputs {
    /// The scenario as authored.
    pub source: SourceExperiment,
    /// The cluster profile the scenario is being run against.
    pub cluster: ClusterProfile,
    /// Subjects in subjects-file order, each with what it said about itself.
    pub subjects: Vec<SubjectProbe>,
    /// Seed override; `None` keeps the scenario's payload seed.
    pub seed: Option<u64>,
    /// Ceilings the attempt declares for itself.
    pub budget: BudgetSpec,
    /// Attempt binding.
    pub runtime: RuntimeInputs,
}
