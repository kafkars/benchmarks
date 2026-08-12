//! `benchctl pack`: one cadence's scenarios, run in the order a reviewed
//! manifest states.
//!
//! # What a pack adds, and what it deliberately does not
//!
//! A pack is a list. It contributes no measurement, no statistics, and no
//! aggregation of its own: every entry is dispatched to the verb that already
//! knows how to run it, and the evidence that lands on disk is byte-for-byte
//! the evidence those verbs would have written if a person had typed them one
//! at a time. That is the whole design constraint. A runner that summarized
//! *across* entries would be inventing a cross-scenario statistic nobody
//! specified, and a runner that reordered or retried them would make "what the
//! nightly ran" a question only the runner can answer.
//!
//! What it does contribute is that the list is reviewed. `scenarios/packs/`
//! states which scenarios belong to which cadence; this verb is the thing that
//! cannot silently disagree with that file.
//!
//! # Exit codes
//!
//! `0` only when every entry exited `0`; `20` when any entry did not. The
//! per-entry codes are the ones those verbs already define — a suite's `20` is
//! still "the bundles exist and say why" — and the pack neither widens nor
//! narrows them, it only reports that at least one entry did not end cleanly.
//!
//! A malformed manifest is a different axis: it is refused with `65` before any
//! attempt runs, because a pack whose entries cannot be read has not measured
//! anything to report. A single unreadable *scenario* does not stop the pack:
//! that entry is recorded as failed and the remaining entries still run, so one
//! broken file in a nightly does not cost the evidence from the other five.
//!
//! # Layout
//!
//! - `manifest` — [`PackManifest`] and [`PackEntry`], the reviewed document.
//! - `plan` — [`EntryPlan`], the rule deciding which verb an entry runs.
//! - `command` — [`PackCommand`] and the loop over entries.
//! - `report` — the closing table.

mod command;
mod manifest;
mod plan;
mod report;

pub use self::command::{EntryOutcome, PackCommand, execute};
pub use self::manifest::{PackEntry, PackManifest};
pub use self::plan::EntryPlan;
pub use self::report::render_table;
