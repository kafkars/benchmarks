//! The three documents an adapter speaks: what it can do, whether it accepts an
//! experiment, and how its run ended.
//!
//! An adapter is a separate process with three verbs — `describe --json`,
//! `validate --experiment <file>`, and `run --experiment <file> --output <dir>`
//! — and each verb has exactly one document as its answer. That is the whole
//! protocol. Keeping it to documents rather than exit codes and stdout
//! conventions is what makes an adapter written in another language, or by
//! somebody else, a first-class subject.
//!
//! Two boundaries matter here.
//!
//! - An adapter declares capabilities and declines experiments; it never
//!   decides whether its own results were valid. Read-back verification is a
//!   configured tool outside the protocol, because a client that grades its own
//!   homework is not evidence.
//! - [`AdapterStatus`] exists so that a failed run still says something. An
//!   adapter that cannot produce a result writes a status and exits non-zero,
//!   and the control plane seals both.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::experiment::LoadMode;
use crate::schema_id::{ADAPTER_STATUS_V1, ADAPTER_V1, ADAPTER_VALIDATE_V1};

/// What an adapter says it is able to measure.
///
/// Capabilities are coarse on purpose: they exist to keep the control plane
/// from asking obviously impossible questions, not to describe the client in
/// detail. The fine-grained answer is [`ValidateReport`], which gets to see the
/// actual experiment.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each flag is an independent client feature, and a state enum would invent \
              combinations no client has"
)]
pub struct AdapterCapabilities {
    /// Whether the adapter can run producer experiments.
    pub producer: bool,
    /// Whether the adapter can run consumer experiments.
    pub consumer: bool,
    /// Whether the client supports the idempotent producer.
    pub idempotence: bool,
    /// Whether the client supports transactions.
    pub transactions: bool,
    /// Whether the client supports TLS transport.
    pub tls: bool,
    /// Compression codecs the adapter accepts, including `none`.
    pub compression: Vec<String>,
    /// Completion surfaces the adapter can wait on.
    pub completion_modes: Vec<String>,
    /// Ownership models the adapter can drive.
    pub ownership_modes: Vec<String>,
    /// Metric families the adapter can report.
    pub metric_families: Vec<String>,
}

/// `kafkars.adapter.v1`: the answer to `describe --json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterDescription {
    /// Schema id, always [`AdapterDescription::SCHEMA`].
    pub schema: String,
    /// Adapter name, stable across versions.
    pub name: String,
    /// Adapter version, which is usually the version of the client it wraps.
    pub version: String,
    /// What the adapter can do.
    pub capabilities: AdapterCapabilities,
    /// Which result schema the adapter writes for each load mode it supports.
    ///
    /// Absent when the adapter declines to say, in which case the control plane
    /// treats the result document as opaque bytes rather than guessing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_schemas: Option<BTreeMap<LoadMode, String>>,
}

impl AdapterDescription {
    /// Schema id this document declares.
    pub const SCHEMA: &'static str = ADAPTER_V1;

    /// Reports whether the document declares the expected schema id.
    pub fn has_expected_schema(&self) -> bool {
        self.schema == Self::SCHEMA
    }

    /// Returns the result schema id the adapter writes for a load mode.
    pub fn result_schema(&self, load_mode: LoadMode) -> Option<&str> {
        self.result_schemas
            .as_ref()?
            .get(&load_mode)
            .map(String::as_str)
    }
}

/// `kafkars.adapter-validate.v1`: the answer to `validate --experiment`.
///
/// A declined experiment is a normal outcome, not a failure. The reasons are
/// what a person reads when a subject is missing from a comparison, so they are
/// written for that reader.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidateReport {
    /// Schema id, always [`ValidateReport::SCHEMA`].
    pub schema: String,
    /// Whether the adapter will run this experiment as written.
    pub supported: bool,
    /// Why not, one reason per unmet requirement. Empty when supported.
    pub reasons: Vec<String>,
}

impl ValidateReport {
    /// Schema id this document declares.
    pub const SCHEMA: &'static str = ADAPTER_VALIDATE_V1;

    /// Creates a report accepting the experiment.
    pub fn supported() -> Self {
        Self {
            schema: Self::SCHEMA.to_owned(),
            supported: true,
            reasons: Vec::new(),
        }
    }

    /// Creates a report declining the experiment for the given reasons.
    pub fn unsupported(reasons: Vec<String>) -> Self {
        Self {
            schema: Self::SCHEMA.to_owned(),
            supported: false,
            reasons,
        }
    }

    /// Reports whether the document declares the expected schema id.
    pub fn has_expected_schema(&self) -> bool {
        self.schema == Self::SCHEMA
    }
}

/// How an adapter's own run ended, in the adapter's words.
///
/// The wire strings are pinned by tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdapterOutcome {
    /// The adapter produced its result document.
    Succeeded,
    /// The adapter stopped without producing a usable result.
    Failed,
}

impl AdapterOutcome {
    /// Returns the wire string for this outcome.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}

/// Where an adapter was when it failed, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterFailure {
    /// Adapter-defined stage name — for example `connect`, `warmup`, `drain`.
    pub stage: String,
    /// What went wrong, in one line, for a person reading a sealed bundle.
    pub reason: String,
}

/// `kafkars.adapter-status.v1`: the adapter's own account of its run.
///
/// This is written even when the run failed, and especially then. The control
/// plane records the process exit separately; the two together distinguish "the
/// adapter knew it failed" from "the adapter was killed".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterStatus {
    /// Schema id, always [`AdapterStatus::SCHEMA`].
    pub schema: String,
    /// Whether the adapter believes it succeeded.
    pub outcome: AdapterOutcome,
    /// Failure detail; present exactly when the outcome is
    /// [`AdapterOutcome::Failed`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<AdapterFailure>,
    /// UTC RFC 3339 timestamp taken when the adapter started work.
    pub started_at: String,
    /// UTC RFC 3339 timestamp taken when the adapter stopped work.
    pub finished_at: String,
}

impl AdapterStatus {
    /// Schema id this document declares.
    pub const SCHEMA: &'static str = ADAPTER_STATUS_V1;

    /// Creates a successful status.
    pub fn succeeded(started_at: impl Into<String>, finished_at: impl Into<String>) -> Self {
        Self {
            schema: Self::SCHEMA.to_owned(),
            outcome: AdapterOutcome::Succeeded,
            failure: None,
            started_at: started_at.into(),
            finished_at: finished_at.into(),
        }
    }

    /// Creates a failed status naming the stage and the reason.
    pub fn failed(
        stage: impl Into<String>,
        reason: impl Into<String>,
        started_at: impl Into<String>,
        finished_at: impl Into<String>,
    ) -> Self {
        Self {
            schema: Self::SCHEMA.to_owned(),
            outcome: AdapterOutcome::Failed,
            failure: Some(AdapterFailure {
                stage: stage.into(),
                reason: reason.into(),
            }),
            started_at: started_at.into(),
            finished_at: finished_at.into(),
        }
    }

    /// Reports whether the document declares the expected schema id.
    pub fn has_expected_schema(&self) -> bool {
        self.schema == Self::SCHEMA
    }
}
