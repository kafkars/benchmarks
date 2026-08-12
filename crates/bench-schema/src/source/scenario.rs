//! The scenario file: the human statement of what to measure.

use serde::{Deserialize, Serialize};

use crate::error::SchemaResult;

use super::{
    mode::SourceLoadMode,
    parse::parse_toml,
    sections::{
        SourceApplication, SourceApplicationApi, SourceCluster, SourceNativeRequestConcurrency,
        SourcePayload, SourceProducer, SourceSearch, SourceSlo, SourceValidity,
    },
};

/// A scenario file: the human statement of what to measure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceExperiment {
    /// Scenario name; carried into the resolved experiment.
    pub name: String,
    /// Rigor profile — `diagnostic`, `reference-capacity`, and so on.
    pub status: String,
    /// Whether the author believes the scenario could support a claim. Always
    /// false in this milestone.
    pub claim_eligible: bool,
    /// How load is offered.
    pub load_mode: SourceLoadMode,
    /// Records the measured phase produces; absent for a capacity search, which
    /// measures for a window instead.
    #[serde(default)]
    pub records: Option<u64>,
    /// Records the warmup phase produces.
    #[serde(default)]
    pub warmup_records: Option<u64>,
    /// Offered rate for a fixed-rate scenario.
    #[serde(default)]
    pub offered_records_per_second: Option<u64>,
    /// Measurement window for a capacity search, in seconds.
    #[serde(default)]
    pub window_seconds: Option<u64>,
    /// Warmup window for a capacity search, in seconds.
    #[serde(default)]
    pub warmup_seconds: Option<u64>,
    /// Repetitions per candidate rate in a capacity search.
    #[serde(default)]
    pub repetitions_per_rate: Option<u32>,
    /// How the application offers records.
    pub application: SourceApplication,
    /// Which client surfaces the application must use.
    pub application_api: SourceApplicationApi,
    /// What each record carries.
    pub payload: SourcePayload,
    /// Client configuration under test.
    pub producer: SourceProducer,
    /// Cluster shape the scenario requires.
    pub cluster: SourceCluster,
    /// Capacity search bounds, when the scenario searches.
    #[serde(default)]
    pub search: Option<SourceSearch>,
    /// Objectives the run is judged against.
    #[serde(default)]
    pub slo: Option<SourceSlo>,
    /// Validity checks the scenario declares mandatory.
    #[serde(default)]
    pub validity: Option<SourceValidity>,
    /// Prose record of how request concurrency was matched across clients.
    #[serde(default)]
    pub native_request_concurrency: Option<SourceNativeRequestConcurrency>,
}

impl SourceExperiment {
    /// Parses a scenario from TOML text.
    pub fn from_toml_str(text: &str) -> SchemaResult<Self> {
        parse_toml(text, "scenario")
    }
}
