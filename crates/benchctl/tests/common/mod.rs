//! Shared fixtures for the offline integration tests: a resolved experiment
//! bound to the fake adapter, a finalized attempt workspace, and the assertions
//! that a sealed bundle has to satisfy.
//!
//! Every test here drives [`benchctl::seal::run_attempt`] directly rather than
//! the binary, because the binary's argument surface belongs to the resolver
//! workstream and the always-seal guarantee does not. Nothing in this module
//! touches a network or a broker: the subjects and all three cluster tools are
//! the `fake-adapter` fixture binary, chosen per invocation by a `--mode`
//! argument so that tests running in parallel threads cannot race over an
//! environment variable.
#![allow(
    dead_code,
    reason = "each integration test crate uses a different subset of these helpers"
)]
#![expect(
    clippy::unwrap_used,
    reason = "a fixture that cannot be built must fail the test immediately"
)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::SystemTime;

use bench_schema::{
    ApplicationSpec, BrokerFacts, BudgetSpec, BundleManifest, Classification, ClusterSpec,
    ClusterTools, Comparison, EnvironmentDocument, ExecutionOrder, ExperimentKind, HostFacts,
    LoadMode, PayloadSpec, ProducerSpec, ResolvedExperiment, RunStatus, RuntimeBinding, SloSpec,
    SubjectSpec, SubjectsLock, TopicPair, UNAVAILABLE, parse_checksums, sha256_hex,
};
use benchctl::seal::AttemptRequest;
use benchctl::{AttemptId, AttemptPaths};

/// The fixture binary that plays every adapter, topic tool, and verifier.
pub(crate) const FAKE_ADAPTER: &str = env!("CARGO_BIN_EXE_fake-adapter");

/// The run id every fixture experiment binds to.
pub(crate) const RUN_ID: &str = "0123456789abcdef";

/// The subject command for an adapter behaving in `mode`.
pub(crate) fn adapter(mode: &str) -> Vec<String> {
    vec![
        FAKE_ADAPTER.to_owned(),
        "--mode".to_owned(),
        mode.to_owned(),
    ]
}

/// The three cluster tools, all played by the fixture in `mode`.
///
/// Each mode affects exactly one verb, so a verifier mode leaves topic creation
/// alone and vice versa.
pub(crate) fn tools(mode: &str) -> ClusterTools {
    let tool = |verb: &str| {
        let mut argv = adapter(mode);
        argv.push(verb.to_owned());
        argv
    };
    ClusterTools {
        topic_create: tool("topics-create"),
        topic_delete: tool("topics-delete"),
        verify: tool("verify"),
    }
}

/// A resolved experiment over `(subject name, adapter mode)` pairs.
pub(crate) fn experiment(subjects: &[(&str, &str)]) -> ResolvedExperiment {
    let mut topics = BTreeMap::new();
    let mut order = Vec::new();
    let mut specs = Vec::new();
    for (name, mode) in subjects {
        topics.insert(
            (*name).to_owned(),
            TopicPair {
                measured: format!("kfb-{RUN_ID}-{name}"),
                warmup: format!("kfb-{RUN_ID}-{name}-warmup"),
            },
        );
        order.push((*name).to_owned());
        specs.push(SubjectSpec {
            name: (*name).to_owned(),
            adapter_name: "fake-adapter".to_owned(),
            adapter_version: "0.1.0".to_owned(),
            command: adapter(mode),
            role: None,
        });
    }
    ResolvedExperiment {
        schema: ResolvedExperiment::SCHEMA.to_owned(),
        name: "offline-integration".to_owned(),
        kind: ExperimentKind::Producer,
        profile: "diagnostic".to_owned(),
        claim_eligible: false,
        load_mode: LoadMode::ClosedLoop,
        records: 1_000,
        warmup_records: 100,
        offered_records_per_second: None,
        arrival: None,
        seed: 44,
        application: ApplicationSpec {
            producer_instances: 1,
            callers_per_producer: 1,
            backpressure: "block-within-original-offer".to_owned(),
            queue_bytes: 67_108_864,
            max_outstanding_records: 8_192,
            admission_shape: "public-batch".to_owned(),
            completion_shape: "aggregate-batch-terminal".to_owned(),
            batch_records: 256,
        },
        payload: PayloadSpec {
            bytes: 1_024,
            profile: "deterministic-ascii-envelope".to_owned(),
            identity: None,
        },
        producer: Some(ProducerSpec {
            acks: "all".to_owned(),
            idempotence: true,
            compression: "none".to_owned(),
            linger_ms: 5,
            batch_records: 256,
            batch_bytes: 65_536,
            request_bytes: 1_048_576,
            delivery_timeout_ms: 60_000,
            partitioning: "explicit-round-robin".to_owned(),
            max_in_flight_requests_per_broker: 5,
            retry_max_replacements: 600,
            retry_backoff_ms: 100,
            warmup_partitioning: None,
            warmup_serialized_partition_primer_records: None,
        }),
        budget: BudgetSpec::default(),
        cluster: ClusterSpec {
            brokers: 3,
            partitions: 12,
            replication_factor: 3,
            min_in_sync_replicas: 2,
            security: "plaintext".to_owned(),
            unclean_leader_election: false,
        },
        slo: SloSpec::default(),
        subjects: specs,
        runtime: Some(RuntimeBinding {
            bootstrap: "127.0.0.1:39092".to_owned(),
            run_id: RUN_ID.to_owned(),
            topic_prefix: format!("kfb-{RUN_ID}"),
            topics,
            execution_order: order,
        }),
    }
}

/// An environment document that captured nothing, which is legal and honest.
pub(crate) fn environment() -> EnvironmentDocument {
    EnvironmentDocument {
        schema: EnvironmentDocument::SCHEMA.to_owned(),
        captured_at: "2026-08-12T14:03:05.000Z".to_owned(),
        source: BTreeMap::new(),
        toolchain: BTreeMap::new(),
        host: HostFacts {
            platform: "test".to_owned(),
            release: UNAVAILABLE.to_owned(),
            architecture: UNAVAILABLE.to_owned(),
            cpu: UNAVAILABLE.to_owned(),
            logical_cpus: 0,
            memory_bytes: 0,
            uname: UNAVAILABLE.to_owned(),
        },
        broker: BrokerFacts {
            version: UNAVAILABLE.to_owned(),
            bootstrap: "127.0.0.1:39092".to_owned(),
            lifecycle: "externally managed by the caller".to_owned(),
        },
    }
}

/// The scenario text every fixture attempt seals verbatim.
pub(crate) const SOURCE_TOML: &str = "name = \"offline-integration\"\nstatus = \"diagnostic\"\n";

/// Creates a finalized attempt workspace under a fresh results root.
pub(crate) fn workspace(label: &str) -> (PathBuf, AttemptPaths) {
    let results_root = std::env::temp_dir().join(format!(
        "benchctl-integration-{}-{label}-{}",
        std::process::id(),
        AttemptId::generate(SystemTime::now()).as_str(),
    ));
    let id = AttemptId::generate(SystemTime::now());
    let paths = AttemptPaths::create_pending(&results_root, &id)
        .unwrap()
        .finalize(&results_root, "d2932f88ad348028")
        .unwrap();
    (results_root, paths)
}

/// Builds an attempt request over a fixture experiment.
pub(crate) fn request(
    paths: &AttemptPaths,
    resolved: ResolvedExperiment,
    tools: ClusterTools,
) -> AttemptRequest {
    AttemptRequest {
        resolved,
        lock: SubjectsLock::new(Vec::new()),
        source_toml: SOURCE_TOML.to_owned(),
        environment: environment(),
        tools,
        paths: paths.clone(),
        started_at: SystemTime::now(),
    }
}

/// Reads a sealed document's bytes, failing loudly when it is missing.
///
/// The parses below are spelled out one document at a time rather than through
/// one generic reader because this crate does not depend on `serde` directly;
/// `bench_schema` owns every document's byte form.
fn read_bytes(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|error| panic!("{} is missing: {error}", path.display()))
}

/// Reads the sealed run status.
pub(crate) fn read_status(paths: &AttemptPaths) -> RunStatus {
    let bytes = read_bytes(&paths.status_json());
    bench_schema::parse_json_slice(&bytes).unwrap()
}

/// Reads the sealed classification.
pub(crate) fn read_classification(paths: &AttemptPaths) -> Classification {
    let bytes = read_bytes(&paths.classification_json());
    bench_schema::parse_json_slice(&bytes).unwrap()
}

/// Reads the sealed comparison.
pub(crate) fn read_comparison(paths: &AttemptPaths) -> Comparison {
    let bytes = read_bytes(&paths.comparison_json());
    bench_schema::parse_json_slice(&bytes).unwrap()
}

/// Reads the sealed execution order.
pub(crate) fn read_execution_order(paths: &AttemptPaths) -> ExecutionOrder {
    let bytes = read_bytes(&paths.execution_order_json());
    bench_schema::parse_json_slice(&bytes).unwrap()
}

/// Reads the sealed bundle manifest.
pub(crate) fn read_bundle(paths: &AttemptPaths) -> BundleManifest {
    let bytes = read_bytes(&paths.bundle_json());
    bench_schema::parse_json_slice(&bytes).unwrap()
}

/// The names of the phases the attempt recorded, in order.
pub(crate) fn phase_names(status: &RunStatus) -> Vec<String> {
    status
        .phases
        .iter()
        .map(|phase| phase.name.clone())
        .collect()
}

/// Asserts that every file the bundle layout promises is on disk.
pub(crate) fn assert_layout_is_complete(paths: &AttemptPaths, subjects: &[&str]) {
    for path in [
        paths.status_json(),
        paths.experiment_source_toml(),
        paths.experiment_resolved_json(),
        paths.subjects_lock_json(),
        paths.environment_json(),
        paths.execution_order_json(),
        paths.classification_json(),
        paths.comparison_json(),
        paths.checksums_txt(),
        paths.bundle_json(),
        paths.root().join("topic-create.stdout.log"),
        paths.root().join("topic-cleanup.stdout.log"),
    ] {
        assert!(path.is_file(), "{} is missing", path.display());
    }
    for subject in subjects {
        for path in [
            paths.adapter_result_json(subject),
            paths.adapter_status_json(subject),
            paths.adapter_stdout_log(subject),
            paths.adapter_stderr_log(subject),
            paths.verification_json(subject, "measured"),
            paths.verification_json(subject, "warmup"),
        ] {
            assert!(path.is_file(), "{} is missing", path.display());
        }
    }
}

/// Re-hashes every line of `checksums.txt`, then the manifest that seals it.
pub(crate) fn assert_bundle_is_self_consistent(paths: &AttemptPaths) {
    let checksums = std::fs::read(paths.checksums_txt()).unwrap();
    let text = String::from_utf8(checksums.clone()).unwrap();
    let entries = parse_checksums(&text).unwrap();
    assert!(!entries.is_empty(), "a sealed bundle lists its files");
    let mut total = 0u64;
    for entry in &entries {
        let bytes = std::fs::read(paths.root().join(entry.path())).unwrap();
        assert_eq!(entry.digest(), sha256_hex(&bytes), "{}", entry.path());
        total += u64::try_from(bytes.len()).unwrap();
    }
    let bundle = read_bundle(paths);
    assert!(bundle.has_expected_schema());
    assert_eq!(bundle.bundle_digest, sha256_hex(&checksums));
    assert_eq!(bundle.file_count, u64::try_from(entries.len()).unwrap());
    assert_eq!(bundle.total_bytes, total);
    assert!(
        !text.contains("checksums.txt") && !text.contains("bundle.json"),
        "the manifest must not list itself or the file that seals it"
    );
}

/// Verifies the bundle the way a person would: with the system checksum tool.
///
/// This is the property the whole checksum format exists for — a bundle can be
/// verified without this repository — so it is asserted with the tool rather
/// than with our own hasher.
pub(crate) fn assert_system_checksum_check_passes(paths: &AttemptPaths) {
    let attempts: [(&str, &[&str]); 2] = [
        ("shasum", &["-a", "256", "-c", "checksums.txt"]),
        ("sha256sum", &["-c", "checksums.txt"]),
    ];
    for (program, arguments) in attempts {
        match Command::new(program)
            .args(arguments)
            .current_dir(paths.root())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
        {
            Ok(status) => {
                assert!(status.success(), "{program} -c rejected the sealed bundle");
                return;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("could not run {program}: {error}"),
        }
    }
    panic!("neither shasum nor sha256sum is available to verify the bundle");
}

/// Asserts that a process id no longer names a live process.
pub(crate) fn assert_process_is_gone(pid: &str) {
    let alive = Command::new("kill")
        .args(["-0", pid])
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(!alive.success(), "process {pid} was left behind");
}

/// Removes a results root once its assertions have passed.
pub(crate) fn cleanup(results_root: &Path) {
    std::fs::remove_dir_all(results_root).unwrap();
}
