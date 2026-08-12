//! The document itself: `kafkars.experiment.v1`, and the load and shape rules
//! that decide whether it is coherent.
//!
//! Field order here is the byte contract, because these bytes are what the
//! experiment id hashes. The subject and binding rules live next to the types
//! they judge, in [`super::subject`]; what stays here is the document, its
//! schema check, and the rules about the run itself.

use serde::{Deserialize, Serialize};

use crate::error::{SchemaError, SchemaResult};
use crate::schema_id::{self, EXPERIMENT_V1};

use super::{
    mode::{ArrivalModel, ExperimentKind, LoadMode},
    spec::{ApplicationSpec, BudgetSpec, ClusterSpec, PayloadSpec, ProducerSpec, SloSpec},
    subject::{RuntimeBinding, SubjectSpec},
};

/// `kafkars.experiment.v1`: resolved intent, ready to run and ready to hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedExperiment {
    /// Schema id, always [`ResolvedExperiment::SCHEMA`].
    pub schema: String,
    /// Scenario name this experiment was resolved from.
    pub name: String,
    /// What kind of client behavior is measured.
    pub kind: ExperimentKind,
    /// Rigor profile the scenario declares — for example `diagnostic`.
    pub profile: String,
    /// Whether the run may support a published claim. False everywhere in this
    /// milestone; [`ResolvedExperiment::validate`] rejects true.
    pub claim_eligible: bool,
    /// How load is offered.
    pub load_mode: LoadMode,
    /// Records the measured phase produces.
    pub records: u64,
    /// Records the warmup phase produces before measurement starts.
    pub warmup_records: u64,
    /// Offered rate; required by, and only by, a fixed-rate load mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offered_records_per_second: Option<u64>,
    /// Arrival process; required by, and only by, a fixed-rate load mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arrival: Option<ArrivalModel>,
    /// Seed for every deterministic choice the run makes.
    pub seed: u64,
    /// How the application offers records.
    pub application: ApplicationSpec,
    /// What each record carries.
    pub payload: PayloadSpec,
    /// Producer configuration; required by, and only by, a producer experiment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer: Option<ProducerSpec>,
    /// Ceilings the attempt declares for itself.
    pub budget: BudgetSpec,
    /// Cluster shape the experiment requires.
    pub cluster: ClusterSpec,
    /// Objectives the run is judged against; may be empty.
    pub slo: SloSpec,
    /// Subjects to measure, in declaration order.
    pub subjects: Vec<SubjectSpec>,
    /// Attempt-specific binding, excluded from the experiment id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<RuntimeBinding>,
}

impl ResolvedExperiment {
    /// Schema id this document declares.
    pub const SCHEMA: &'static str = EXPERIMENT_V1;

    /// Reports whether the document declares the expected schema id.
    pub fn has_expected_schema(&self) -> bool {
        self.schema == Self::SCHEMA
    }

    /// Returns the subject with the given name.
    pub fn subject(&self, name: &str) -> Option<&SubjectSpec> {
        self.subjects.iter().find(|subject| subject.name == name)
    }

    /// Checks every cross-field rule the type system cannot express.
    ///
    /// The rules are deliberately strict and stated in one place: an experiment
    /// that reaches an adapter has already been judged coherent, so an adapter
    /// declining it means the adapter cannot do it, not that the request was
    /// malformed.
    pub fn validate(&self) -> SchemaResult<()> {
        schema_id::require_schema(&self.schema, Self::SCHEMA)?;
        if self.name.trim().is_empty() {
            return Err(SchemaError::invalid_field("name", "must not be empty"));
        }
        if self.profile.trim().is_empty() {
            return Err(SchemaError::invalid_field("profile", "must not be empty"));
        }
        if self.claim_eligible {
            return Err(SchemaError::invalid_field(
                "claim_eligible",
                "no experiment in this milestone may support a published claim",
            ));
        }
        self.validate_load()?;
        self.validate_shape()?;
        self.validate_subjects()?;
        self.validate_runtime()
    }

    fn validate_load(&self) -> SchemaResult<()> {
        if self.records == 0 {
            return Err(SchemaError::invalid_field(
                "records",
                "a measured phase of zero records measures nothing",
            ));
        }
        match self.load_mode {
            LoadMode::ScheduledOpenLoopFixedRate => {
                match self.offered_records_per_second {
                    None => {
                        return Err(SchemaError::invalid_field(
                            "offered_records_per_second",
                            "a fixed-rate experiment must state the rate it offers",
                        ));
                    }
                    Some(0) => {
                        return Err(SchemaError::invalid_field(
                            "offered_records_per_second",
                            "must be greater than zero",
                        ));
                    }
                    Some(_) => {}
                }
                if self.arrival.is_none() {
                    return Err(SchemaError::invalid_field(
                        "arrival",
                        "a fixed-rate experiment must state its arrival process",
                    ));
                }
            }
            LoadMode::ClosedLoop => {
                if self.offered_records_per_second.is_some() {
                    return Err(SchemaError::invalid_field(
                        "offered_records_per_second",
                        "a closed-loop experiment does not offer a chosen rate",
                    ));
                }
                if self.arrival.is_some() {
                    return Err(SchemaError::invalid_field(
                        "arrival",
                        "a closed-loop experiment has no arrival schedule",
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_shape(&self) -> SchemaResult<()> {
        match self.kind {
            ExperimentKind::Producer => {
                if self.producer.is_none() {
                    return Err(SchemaError::invalid_field(
                        "producer",
                        "a producer experiment must carry a producer section",
                    ));
                }
            }
        }
        if self.payload.bytes == 0 {
            return Err(SchemaError::invalid_field(
                "payload.bytes",
                "must be greater than zero",
            ));
        }
        if self.application.producer_instances == 0 {
            return Err(SchemaError::invalid_field(
                "application.producer_instances",
                "must be greater than zero",
            ));
        }
        if self.application.callers_per_producer == 0 {
            return Err(SchemaError::invalid_field(
                "application.callers_per_producer",
                "must be greater than zero",
            ));
        }
        if self.cluster.brokers == 0 {
            return Err(SchemaError::invalid_field(
                "cluster.brokers",
                "must be greater than zero",
            ));
        }
        if self.cluster.partitions == 0 {
            return Err(SchemaError::invalid_field(
                "cluster.partitions",
                "must be greater than zero",
            ));
        }
        if self.cluster.replication_factor == 0 {
            return Err(SchemaError::invalid_field(
                "cluster.replication_factor",
                "must be greater than zero",
            ));
        }
        if self.cluster.min_in_sync_replicas == 0
            || self.cluster.min_in_sync_replicas > self.cluster.replication_factor
        {
            return Err(SchemaError::invalid_field(
                "cluster.min_in_sync_replicas",
                "must be between one and the replication factor",
            ));
        }
        Ok(())
    }
}
