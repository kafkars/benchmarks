//! A parsed command line: which verb, and everything that verb was given.
//!
//! `resolve` probes the subjects and prints the resolved experiment. It creates
//! no attempt workspace and seals nothing, so it is the safe way to ask "what
//! would this run, and under what experiment id" before spending a cluster on
//! it. `run` does everything `resolve` does and then hands the attempt to the
//! sealing supervisor, which is the only thing that writes into the bundle.
//!
//! The other five verbs' inputs belong to the modules that implement them, and
//! are named here rather than restated.

use std::path::PathBuf;

use bench_schema::BudgetSpec;

use crate::capacity::CapacityCommand;
use crate::pack::PackCommand;
use crate::pipeline::CommonArguments;
use crate::report::{PacketCommand, ReportCommand};
use crate::suite::SuiteCommand;

/// `benchctl resolve`: probe and resolve, write the document, seal nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveCommand {
    /// Shared inputs.
    pub common: CommonArguments,
    /// Where the resolved document goes; `None` means stdout.
    pub out: Option<PathBuf>,
}

/// `benchctl run`: the whole pipeline, ending in a sealed bundle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunCommand {
    /// Shared inputs.
    pub common: CommonArguments,
    /// Root of the evidence tree.
    pub results_root: PathBuf,
    /// Ceilings this attempt declares for itself.
    pub budget: BudgetSpec,
}

/// A parsed command line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Print [`USAGE`](super::USAGE) and exit successfully.
    Help,
    /// Resolve and print.
    Resolve(ResolveCommand),
    /// Resolve, run, and seal.
    Run(RunCommand),
    /// Repeat one experiment and summarize the repetitions.
    Suite(SuiteCommand),
    /// Search for the highest satisfying offered rate.
    Capacity(CapacityCommand),
    /// Run every entry of one cadence's manifest.
    Pack(PackCommand),
    /// Render one sealed bundle.
    Report(ReportCommand),
    /// Check an LLM summary against its packet.
    Packet(PacketCommand),
}
