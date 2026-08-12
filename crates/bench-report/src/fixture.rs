//! Test-only builders for the documents this crate reads.
//!
//! Fixtures are constructed in code rather than checked in as JSON so that a
//! schema change breaks the build here instead of producing a summary of a
//! document shape that no longer exists. Everything a fixture builds satisfies
//! the accounting invariants `bench-schema` enforces, so a test that wants an
//! *invalid* document has to say so explicitly.

use std::path::{Path, PathBuf};

use bench_schema::{
    Classification, DeclaredExecution, EncodedHistogram, ExperimentKind, Histogram, LoadMode,
    MeasuredThroughput, OfferOutcomes, OfferTiming, ProcessResources, ProducerBenchmarkV2,
    QueueObservation, ResolvedExperiment, RunStatus, SubjectSpec,
};

/// Encodes a histogram of `count` recordings of the same value, so that every
/// percentile of it is exactly that value.
pub(crate) fn uniform_histogram(count: u64, value_ns: u64) -> EncodedHistogram {
    let mut histogram = Histogram::new();
    for _ in 0..count {
        histogram.record(value_ns);
    }
    histogram.encode()
}

/// The knobs a test needs over one `kafkars.producer-benchmark.v2` document.
#[derive(Debug, Clone)]
pub(crate) struct ResultFixture {
    /// Adapter name and, by default, the subject name.
    pub(crate) adapter: String,
    /// The attempt's sixteen-hex run id.
    pub(crate) run_id: String,
    /// Records acknowledged by the broker.
    pub(crate) acknowledged: u64,
    /// Records that reached a failure terminal.
    pub(crate) failed: u64,
    /// Records that reached a timeout terminal.
    pub(crate) timed_out: u64,
    /// Accepted offers with no terminal.
    pub(crate) unknown: u64,
    /// Offers outstanding when the run ended.
    pub(crate) final_outstanding: u64,
    /// Every offer-to-terminal latency takes this value.
    pub(crate) terminal_ns: u64,
    /// Every admission wait takes this value.
    pub(crate) admission_ns: u64,
    /// Scheduler lateness, when the load mode has a schedule.
    pub(crate) lateness_ns: Option<u64>,
    /// Acknowledged records per second.
    pub(crate) goodput: f64,
    /// Self-reported process resources.
    pub(crate) resources: Option<ProcessResources>,
    /// Bundle-relative path of the native statistics stream.
    pub(crate) native_metrics_path: Option<String>,
    /// The adapter's own validity verdict.
    pub(crate) valid: bool,
}

impl Default for ResultFixture {
    fn default() -> Self {
        Self {
            adapter: "subject".to_owned(),
            run_id: "0123456789abcdef".to_owned(),
            acknowledged: 1_000,
            failed: 0,
            timed_out: 0,
            unknown: 0,
            final_outstanding: 0,
            terminal_ns: 5_000_000,
            admission_ns: 1_000,
            lateness_ns: None,
            goodput: 100_000.0,
            resources: Some(ProcessResources {
                max_rss_bytes: 64 * 1024 * 1024,
                user_cpu_ns: 4_000_000_000,
                system_cpu_ns: 1_000_000_000,
            }),
            native_metrics_path: None,
            valid: true,
        }
    }
}

impl ResultFixture {
    /// Builds the document, satisfying every accounting invariant.
    pub(crate) fn build(&self) -> ProducerBenchmarkV2 {
        let terminals = self
            .acknowledged
            .saturating_add(self.failed)
            .saturating_add(self.timed_out);
        let accepted = terminals.saturating_add(self.unknown);
        let load_mode = if self.lateness_ns.is_some() {
            LoadMode::ScheduledOpenLoopFixedRate
        } else {
            LoadMode::ClosedLoop
        };
        ProducerBenchmarkV2 {
            schema: ProducerBenchmarkV2::SCHEMA.to_owned(),
            adapter: self.adapter.clone(),
            adapter_version: "0.1.0".to_owned(),
            run_id: self.run_id.clone(),
            load_mode,
            declared: DeclaredExecution {
                payload_construction: "prebuilt-pool".to_owned(),
                ownership: "copy-in".to_owned(),
                completion_mode: "public-future".to_owned(),
                serialization: "excluded".to_owned(),
            },
            outcomes: OfferOutcomes {
                offered: accepted,
                accepted,
                acknowledged: self.acknowledged,
                failed: self.failed,
                timed_out: self.timed_out,
                unknown: self.unknown,
            },
            timing: OfferTiming {
                clock: "monotonic-ns".to_owned(),
                intended_to_terminal: uniform_histogram(terminals, self.terminal_ns),
                accepted_to_terminal: uniform_histogram(terminals, self.terminal_ns / 2),
                call_start_to_accepted: uniform_histogram(accepted, self.admission_ns),
                intended_to_call_start: self
                    .lateness_ns
                    .map(|value| uniform_histogram(accepted, value)),
            },
            throughput: MeasuredThroughput {
                measured_duration_ns: 10_000_000_000,
                acknowledged_records_per_second: self.goodput,
                acknowledged_payload_bytes_per_second: self.goodput * 1_024.0,
            },
            queue: QueueObservation {
                max_outstanding_observed: 512,
                final_outstanding: self.final_outstanding,
            },
            resources: self.resources,
            native_metrics_path: self.native_metrics_path.clone(),
            valid: self.valid,
            invalid_reason: (!self.valid).then(|| "fixture declared it invalid".to_owned()),
        }
    }
}

/// The knobs a test needs over one sealed bundle directory.
#[derive(Debug, Clone)]
pub(crate) struct BundleFixture {
    /// Attempt id, which also names the directory.
    pub(crate) attempt_id: String,
    /// Scenario name carried by the resolved experiment.
    pub(crate) scenario: String,
    /// Subject name, adapter name, optional role, and its result.
    pub(crate) subjects: Vec<(String, Option<String>, ResultFixture)>,
    /// Whether `classification.json` says the attempt may be believed.
    pub(crate) run_valid: bool,
    /// Per-subject validity, when it differs from the run verdict.
    pub(crate) invalid_subjects: Vec<String>,
    /// Native statistics streams to write, by subject name.
    pub(crate) native_metrics: Vec<(String, String)>,
}

impl BundleFixture {
    /// Creates a two-subject fixture with the given attempt id.
    pub(crate) fn new(attempt_id: &str) -> Self {
        Self {
            attempt_id: attempt_id.to_owned(),
            scenario: "producer-comparison".to_owned(),
            subjects: Vec::new(),
            run_valid: true,
            invalid_subjects: Vec::new(),
            native_metrics: Vec::new(),
        }
    }

    /// Adds one subject with a role and a result.
    pub(crate) fn subject(mut self, name: &str, role: Option<&str>, result: ResultFixture) -> Self {
        self.subjects.push((
            name.to_owned(),
            role.map(str::to_owned),
            ResultFixture {
                adapter: name.to_owned(),
                ..result
            },
        ));
        self
    }

    /// Writes the bundle under `root` and returns its directory.
    ///
    /// # Panics
    ///
    /// Panics when the temporary directory cannot be written, which a test
    /// cannot proceed past anyway.
    pub(crate) fn write(&self, root: &Path) -> PathBuf {
        let bundle = root.join(&self.attempt_id);
        write_json(&bundle.join("status.json"), &self.status());
        write_json(&bundle.join("experiment.resolved.json"), &self.experiment());
        write_json(&bundle.join("classification.json"), &self.classification());
        for (name, _, result) in &self.subjects {
            write_json(
                &bundle.join("adapters").join(name).join("result.json"),
                &result.build(),
            );
        }
        for (name, stream) in &self.native_metrics {
            let path = bundle
                .join("adapters")
                .join(name)
                .join(crate::economics::STATISTICS_FILE_NAME);
            create_parent(&path);
            std::fs::write(&path, stream).unwrap_or_else(|error| {
                panic!("write {}: {error}", path.display());
            });
        }
        bundle
    }

    /// The run status document this fixture seals.
    fn status(&self) -> RunStatus {
        RunStatus {
            schema: RunStatus::SCHEMA.to_owned(),
            experiment_id: None,
            attempt_id: self.attempt_id.clone(),
            execution_status: bench_schema::ExecutionStatus::Complete,
            failure_reason: None,
            interrupted: false,
            subjects: self
                .subjects
                .iter()
                .map(|(name, _, _)| bench_schema::SubjectExecution {
                    name: name.clone(),
                    execution: None,
                    adapter_outcome: None,
                    result_present: true,
                    verification: bench_schema::SubjectVerification::default(),
                })
                .collect(),
            phases: Vec::new(),
        }
    }

    /// The classification document this fixture seals.
    fn classification(&self) -> Classification {
        Classification {
            schema: Classification::SCHEMA.to_owned(),
            run_valid: self.run_valid,
            claim_eligible: false,
            subjects: self
                .subjects
                .iter()
                .map(|(name, _, _)| bench_schema::SubjectValidity {
                    name: name.clone(),
                    valid: !self.invalid_subjects.contains(name),
                    reasons: if self.invalid_subjects.contains(name) {
                        vec!["fixture marked this subject invalid".to_owned()]
                    } else {
                        Vec::new()
                    },
                })
                .collect(),
            deferred_checks: vec!["fixture".to_owned()],
            reasons: Vec::new(),
        }
    }

    /// The resolved experiment this fixture seals.
    fn experiment(&self) -> ResolvedExperiment {
        ResolvedExperiment {
            schema: ResolvedExperiment::SCHEMA.to_owned(),
            name: self.scenario.clone(),
            kind: ExperimentKind::Producer,
            profile: "diagnostic".to_owned(),
            claim_eligible: false,
            load_mode: LoadMode::ClosedLoop,
            records: 1_000,
            warmup_records: 100,
            offered_records_per_second: None,
            arrival: None,
            seed: 7,
            application: bench_schema::ApplicationSpec {
                producer_instances: 1,
                callers_per_producer: 1,
                backpressure: "block-within-original-offer".to_owned(),
                queue_bytes: 67_108_864,
                max_outstanding_records: 100_000,
                admission_shape: "public-batch".to_owned(),
                completion_shape: "aggregate-batch-terminal".to_owned(),
                batch_records: 256,
            },
            payload: bench_schema::PayloadSpec {
                bytes: 1_024,
                profile: "deterministic-ascii-envelope".to_owned(),
                identity: None,
            },
            producer: Some(bench_schema::ProducerSpec {
                acks: "all".to_owned(),
                idempotence: true,
                compression: "none".to_owned(),
                linger_ms: 5,
                batch_records: 256,
                batch_bytes: 1_048_576,
                request_bytes: 1_048_576,
                delivery_timeout_ms: 60_000,
                partitioning: "explicit-round-robin".to_owned(),
                max_in_flight_requests_per_broker: 5,
                retry_max_replacements: 600,
                retry_backoff_ms: 100,
                warmup_partitioning: None,
                warmup_serialized_partition_primer_records: None,
            }),
            budget: bench_schema::BudgetSpec::default(),
            cluster: bench_schema::ClusterSpec {
                brokers: 3,
                partitions: 6,
                replication_factor: 3,
                min_in_sync_replicas: 2,
                security: "plaintext".to_owned(),
                unclean_leader_election: false,
            },
            slo: bench_schema::SloSpec::default(),
            subjects: self
                .subjects
                .iter()
                .map(|(name, role, _)| SubjectSpec {
                    name: name.clone(),
                    adapter_name: name.clone(),
                    adapter_version: "0.1.0".to_owned(),
                    command: vec![format!("./{name}")],
                    role: role.clone(),
                })
                .collect(),
            runtime: None,
        }
    }
}

/// Writes a document as pretty JSON, creating parent directories.
///
/// # Panics
///
/// Panics when the write fails, which a test cannot proceed past.
fn write_json<T: serde::Serialize>(path: &Path, document: &T) {
    create_parent(path);
    let bytes = serde_json::to_vec_pretty(document)
        .unwrap_or_else(|error| panic!("serialize {}: {error}", path.display()));
    std::fs::write(path, bytes).unwrap_or_else(|error| panic!("write {}: {error}", path.display()));
}

/// Creates a path's parent directory.
///
/// # Panics
///
/// Panics when the directory cannot be created.
fn create_parent(path: &Path) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .unwrap_or_else(|error| panic!("create {}: {error}", parent.display()));
    }
}

/// Returns a fresh, empty scratch directory named after the caller's test.
///
/// # Panics
///
/// Panics when the directory cannot be created.
pub(crate) fn scratch_directory(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!("bench-report-{name}"));
    if root.exists() {
        std::fs::remove_dir_all(&root)
            .unwrap_or_else(|error| panic!("clear {}: {error}", root.display()));
    }
    std::fs::create_dir_all(&root)
        .unwrap_or_else(|error| panic!("create {}: {error}", root.display()));
    root
}
