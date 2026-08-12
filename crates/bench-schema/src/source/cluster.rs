//! The cluster profile: the facts about a cluster that a scenario must not
//! contain, and the tools the control plane calls out to.

use serde::{Deserialize, Serialize};

use crate::error::SchemaResult;

use super::parse::parse_toml;

/// Argument vectors for the tools the control plane calls out to.
///
/// Topic creation and read-back verification are deliberately *not* part of the
/// adapter protocol: an adapter must never be the thing that decides whether
/// its own output was correct. The tools are named by configuration so that no
/// adapter name is hard-coded into the control plane, and each vector is a
/// prefix — the control plane appends the positional arguments the legacy tools
/// already expect.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterTools {
    /// Prefix for topic creation, called as
    /// `<prefix...> <bootstrap> <partitions> <replication-factor> <topic...>`.
    #[serde(default)]
    pub topic_create: Vec<String>,
    /// Prefix for topic deletion, called as `<prefix...> <bootstrap> <topic...>`.
    #[serde(default)]
    pub topic_delete: Vec<String>,
    /// Prefix for read-back verification, called as
    /// `<prefix...> <bootstrap> <topic> <run-id> <records> <payload-bytes> <partitions>`.
    #[serde(default)]
    pub verify: Vec<String>,
}

impl ClusterTools {
    /// Reports whether no tool at all is configured.
    ///
    /// A profile with no tools can still run an experiment; the control plane
    /// records the skipped phases as deferred checks rather than pretending
    /// they passed.
    pub fn is_empty(&self) -> bool {
        self.topic_create.is_empty() && self.topic_delete.is_empty() && self.verify.is_empty()
    }
}

/// A cluster profile: the facts about a cluster that a scenario must not
/// contain.
///
/// Keeping bootstrap servers out of the scenario is what lets the same
/// experiment id describe a run on a laptop and a run in CI.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterProfile {
    /// Profile name, used in logs and evidence.
    pub name: String,
    /// Bootstrap servers, `host:port[,host:port...]`.
    pub bootstrap: String,
    /// Broker version, when the operator knows it.
    #[serde(default)]
    pub broker_version: Option<String>,
    /// Transport security vocabulary, when it differs from the scenario's.
    #[serde(default)]
    pub security: Option<String>,
    /// Brokers actually in this cluster.
    #[serde(default)]
    pub brokers: Option<u32>,
    /// Who owns the cluster's lifecycle, recorded in the environment document.
    #[serde(default)]
    pub lifecycle: Option<String>,
    /// Tools the control plane calls out to.
    #[serde(default)]
    pub tools: ClusterTools,
}

impl ClusterProfile {
    /// Parses a cluster profile from TOML text.
    pub fn from_toml_str(text: &str) -> SchemaResult<Self> {
        parse_toml(text, "cluster profile")
    }
}
