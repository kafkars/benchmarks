//! Experiment resolution: source TOML plus cluster profile plus subject
//! describe/validate results, deterministically merged into one canonical
//! `kafkars.experiment.v1` document and its subjects lock. The pure core is
//! separated from process-spawning so goldens can pin its bytes.
//!
//! Stub: implemented by the resolver workstream (agent B).
