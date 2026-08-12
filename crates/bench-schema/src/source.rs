//! The human-authored TOML inputs: a scenario, a cluster profile, and a subject
//! list.
//!
//! These three files are what a person writes and reviews; everything else in
//! this crate is machine-written. They resolve into a
//! [`ResolvedExperiment`](crate::ResolvedExperiment), which is where defaults
//! stop being implicit.
//!
//! Parsing is fail-closed: every type denies unknown fields. A scenario is a
//! declaration of intent, and a mistyped key in a declaration of intent is not
//! a harmless extra — it is a setting the author believes is in effect and that
//! nothing is reading. The cost is that adding a scenario key means adding it
//! here too, which is the intended amount of friction.
//!
//! The scenario types model the migrated legacy scenarios under
//! `scenarios/producer/` exactly, and tests parse those files from disk rather
//! than from a copy, so a drift between the two is a test failure. Note that
//! `scenarios/producer/producer-baseline.toml` is deliberately *not* one of
//! them: it is the design document's future parameter matrix, not an executable
//! scenario, and it has never been read by any harness.
//!
//! The scenario vocabulary for `load_mode` is not the resolved vocabulary. The
//! legacy scenarios say `closed-loop-capacity-point`, and the resolved document
//! says `closed-loop`; [`SourceLoadMode::resolved`] is the only place that
//! mapping is written down, and it is also where the capacity search mode is
//! refused, because this milestone has no implementation of it.
//!
//! # Layout
//!
//! - `parse` — TOML text in, a named schema error out.
//! - `mode` — the scenario's load-mode vocabulary and its mapping.
//! - `sections` — one type per `[section]` table a scenario carries.
//! - `scenario` — the scenario file those sections assemble into.
//! - `subjects` — the subject list and its role check.
//! - `cluster` — the cluster profile and the configured tool vectors.

mod cluster;
mod mode;
mod parse;
mod scenario;
mod sections;
mod subjects;

pub use cluster::{ClusterProfile, ClusterTools};
pub use mode::SourceLoadMode;
pub use scenario::SourceExperiment;
pub use sections::{
    SourceApplication, SourceApplicationApi, SourceCluster, SourceNativeRequestConcurrency,
    SourcePayload, SourceProducer, SourceSearch, SourceSlo, SourceValidity,
};
pub use subjects::{SubjectEntry, SubjectsFile};
