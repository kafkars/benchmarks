//! The always-seal funnel, driven with `/bin/sh` subjects and tools.
//!
//! This module also hosts the resolved-experiment fixture the other supervisor
//! test modules build on, because every one of them needs the same shape and a
//! second copy of it would drift.
//!
//! The protocol-level matrix — an adapter that aborts, one that ignores its
//! deadline, a verifier that disagrees with its adapter — lives in the
//! integration tests, where the fake adapter binary is available. What is
//! exercised here is the funnel itself: that a bundle exists, that it is
//! internally consistent, and that the status is the worst thing that happened.
#![expect(
    clippy::unwrap_used,
    reason = "a seal fixture that cannot be built must fail the test immediately"
)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::SystemTime;

use bench_schema::{
    ApplicationSpec, BudgetSpec, Classification, ClusterSpec, ClusterTools, EnvironmentDocument,
    ExecutionStatus, ExperimentKind, HostFacts, LoadMode, PayloadSpec, ProducerSpec,
    ResolvedExperiment, RunStatus, RuntimeBinding, SloSpec, SubjectSpec, SubjectsLock, TopicPair,
    UNAVAILABLE, parse_checksums, sha256_hex,
};

use crate::attempt::{AttemptId, AttemptPaths};
use crate::seal::{AttemptRequest, run_attempt, seal_failure};

/// The run id every fixture experiment binds to.
pub(crate) const RUN_ID: &str = "0123456789abcdef";

/// An argv that runs `script` through `/bin/sh`, ignoring anything appended.
pub(crate) fn shell(script: &str) -> Vec<String> {
    vec![
        "/bin/sh".to_owned(),
        "-c".to_owned(),
        script.to_owned(),
        "benchctl-fixture".to_owned(),
    ]
}

/// A resolved experiment over the given subjects, each with its own command.
pub(crate) fn experiment(subjects: &[(&str, Vec<String>)]) -> ResolvedExperiment {
    let mut topics = BTreeMap::new();
    let mut order = Vec::new();
    let mut specs = Vec::new();
    for (name, command) in subjects {
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
            command: command.clone(),
        });
    }
    ResolvedExperiment {
        schema: ResolvedExperiment::SCHEMA.to_owned(),
        name: "supervisor-fixture".to_owned(),
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

/// An environment document with nothing captured, which is legal and honest.
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
        broker: bench_schema::BrokerFacts {
            version: UNAVAILABLE.to_owned(),
            bootstrap: "127.0.0.1:39092".to_owned(),
            lifecycle: "externally managed by the caller".to_owned(),
        },
    }
}

/// Creates a finalized attempt workspace under a fresh results root.
pub(crate) fn workspace(label: &str) -> (PathBuf, AttemptPaths) {
    let results_root = std::env::temp_dir().join(format!(
        "benchctl-seal-test-{}-{label}-{}",
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

/// Builds a request over a fixture experiment.
fn request(
    paths: &AttemptPaths,
    resolved: ResolvedExperiment,
    tools: ClusterTools,
) -> AttemptRequest {
    AttemptRequest {
        resolved,
        lock: SubjectsLock::new(Vec::new()),
        source_toml: "name = \"supervisor-fixture\"\n".to_owned(),
        environment: environment(),
        tools,
        paths: paths.clone(),
        started_at: SystemTime::now(),
    }
}

/// Reads a sealed document back out of the bundle.
fn read_status(paths: &AttemptPaths) -> RunStatus {
    let bytes = std::fs::read(paths.status_json()).unwrap();
    bench_schema::parse_json_slice(&bytes).unwrap()
}

/// Reads the classification back out of the bundle.
fn read_classification(paths: &AttemptPaths) -> Classification {
    let bytes = std::fs::read(paths.classification_json()).unwrap();
    bench_schema::parse_json_slice(&bytes).unwrap()
}

/// Re-hashes every line of `checksums.txt` and the manifest that seals it.
fn assert_bundle_is_self_consistent(paths: &AttemptPaths) {
    let checksums = std::fs::read(paths.checksums_txt()).unwrap();
    let text = String::from_utf8(checksums.clone()).unwrap();
    let entries = parse_checksums(&text).unwrap();
    assert!(!entries.is_empty(), "a sealed bundle lists its files");
    for entry in &entries {
        let bytes = std::fs::read(paths.root().join(entry.path())).unwrap();
        assert_eq!(entry.digest(), sha256_hex(&bytes), "{}", entry.path());
    }
    let bundle: bench_schema::BundleManifest =
        bench_schema::parse_json_slice(&std::fs::read(paths.bundle_json()).unwrap()).unwrap();
    assert_eq!(bundle.bundle_digest, sha256_hex(&checksums));
    assert_eq!(bundle.file_count, u64::try_from(entries.len()).unwrap());
    assert!(
        !text.contains("checksums.txt") && !text.contains("bundle.json"),
        "the manifest must not list itself or the file that seals it"
    );
}

#[test]
fn a_pre_attempt_failure_still_seals_a_bundle() {
    let (results_root, paths) = workspace("failure");
    let status = seal_failure(
        &paths,
        Some("name = \"unusable\"\n"),
        ExecutionStatus::Partial,
        "probe failed",
    )
    .unwrap();
    assert_eq!(status, ExecutionStatus::Partial);
    assert_eq!(
        std::fs::read_to_string(paths.experiment_source_toml()).unwrap(),
        "name = \"unusable\"\n"
    );
    let sealed = read_status(&paths);
    assert_eq!(sealed.execution_status, ExecutionStatus::Partial);
    assert_eq!(sealed.failure_reason.as_deref(), Some("probe failed"));
    assert_eq!(sealed.experiment_id, None);
    assert!(sealed.subjects.is_empty());
    let classification = read_classification(&paths);
    assert!(!classification.run_valid);
    assert!(!classification.claim_eligible);
    assert!(classification.reasons.iter().any(|r| r == "probe failed"));
    assert_bundle_is_self_consistent(&paths);
    std::fs::remove_dir_all(&results_root).unwrap();
}

#[test]
fn a_seal_that_cannot_write_still_leaves_a_status_and_a_manifest() {
    let (results_root, paths) = workspace("seal-io-failure");
    // A directory where a document belongs makes exactly one write fail, which
    // is the only way to reach the third always-seal layer from outside.
    std::fs::create_dir(paths.classification_json()).unwrap();
    let error = seal_failure(
        &paths,
        Some("name = \"unusable\"\n"),
        ExecutionStatus::Partial,
        "probe failed",
    )
    .unwrap_err();
    assert_eq!(error.kind(), crate::error::CtlErrorKind::Seal);
    assert_eq!(error.exit_code(), 74);
    assert!(
        paths.status_json().is_file(),
        "status.json is written first so that a half-sealed bundle still says why"
    );
    assert!(
        paths.checksums_txt().is_file(),
        "the last-resort guard sealed what it could"
    );
    assert!(paths.bundle_json().is_file());
    std::fs::remove_dir_all(&results_root).unwrap();
}

#[test]
fn an_attempt_with_no_tools_is_partial_and_names_every_skipped_check() {
    let (results_root, paths) = workspace("no-tools");
    let resolved = experiment(&[("only", shell("exit 0"))]);
    let status = run_attempt(request(&paths, resolved, ClusterTools::default())).unwrap();
    assert_eq!(status, ExecutionStatus::Partial);

    let sealed = read_status(&paths);
    assert!(
        sealed.experiment_id.is_some(),
        "a valid experiment has an id"
    );
    assert_eq!(sealed.subjects.len(), 1);
    assert_eq!(
        sealed.subjects[0].execution.map(|exit| exit.exit_code),
        Some(Some(0))
    );
    let phases: Vec<&str> = sealed.phases.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(
        phases,
        vec![
            "seal-inputs",
            "topic-create",
            "subject:only:run",
            "subject:only:verify-measured",
            "subject:only:verify-warmup",
            "topic-cleanup",
        ]
    );
    // The sealed inputs are all present and byte-identical to what was handed in.
    assert_eq!(
        std::fs::read_to_string(paths.experiment_source_toml()).unwrap(),
        "name = \"supervisor-fixture\"\n"
    );
    assert!(paths.experiment_resolved_json().is_file());
    assert!(paths.subjects_lock_json().is_file());
    assert!(paths.environment_json().is_file());
    assert!(paths.execution_order_json().is_file());
    assert!(paths.comparison_json().is_file());

    let classification = read_classification(&paths);
    assert!(!classification.run_valid);
    assert_eq!(classification.deferred_checks.len(), 7);
    assert_bundle_is_self_consistent(&paths);
    std::fs::remove_dir_all(&results_root).unwrap();
}

#[test]
fn a_failed_topic_tool_stops_the_attempt_before_any_subject_runs() {
    let (results_root, paths) = workspace("topic-fail");
    let tools = ClusterTools {
        topic_create: shell("exit 1"),
        topic_delete: shell("exit 0"),
        verify: Vec::new(),
    };
    let resolved = experiment(&[("first", shell("exit 0")), ("second", shell("exit 0"))]);
    let status = run_attempt(request(&paths, resolved, tools)).unwrap();
    assert_eq!(status, ExecutionStatus::Partial);

    let sealed = read_status(&paths);
    assert!(
        sealed
            .failure_reason
            .as_deref()
            .is_some_and(|reason| reason.starts_with("topic creation failed")),
        "{:?}",
        sealed.failure_reason
    );
    assert!(
        sealed.subjects.iter().all(|s| s.execution.is_none()),
        "no subject may run without topics"
    );
    let run_phases: Vec<&str> = sealed
        .phases
        .iter()
        .filter(|p| p.name.ends_with(":run"))
        .map(|p| p.name.as_str())
        .collect();
    assert_eq!(run_phases, vec!["subject:first:run", "subject:second:run"]);
    assert!(
        sealed
            .phases
            .iter()
            .filter(|p| p.name.ends_with(":run"))
            .all(|p| p.outcome == bench_schema::PhaseOutcome::Skipped)
    );
    // Cleanup still runs: topics may exist even after a failed create.
    assert!(paths.root().join("topic-cleanup.stdout.log").is_file());
    assert_bundle_is_self_consistent(&paths);
    std::fs::remove_dir_all(&results_root).unwrap();
}

#[test]
fn a_subject_that_dies_on_a_signal_makes_the_attempt_crashed() {
    let (results_root, paths) = workspace("crashed");
    let resolved = experiment(&[("dies", shell("kill -ABRT $$"))]);
    let status = run_attempt(request(&paths, resolved, ClusterTools::default())).unwrap();
    assert_eq!(status, ExecutionStatus::Crashed);
    let sealed = read_status(&paths);
    assert_eq!(
        sealed.subjects[0].execution.and_then(|exit| exit.signal),
        Some(6)
    );
    assert!(!sealed.interrupted);
    assert_bundle_is_self_consistent(&paths);
    std::fs::remove_dir_all(&results_root).unwrap();
}

#[test]
fn a_timeout_outranks_every_other_ending() {
    let (results_root, paths) = workspace("timed-out");
    let mut resolved = experiment(&[("slow", shell("sleep 30")), ("fails", shell("exit 4"))]);
    resolved.budget = BudgetSpec {
        run_timeout_seconds: 1,
        ..BudgetSpec::default()
    };
    let status = run_attempt(request(&paths, resolved, ClusterTools::default())).unwrap();
    assert_eq!(
        status,
        ExecutionStatus::TimedOut,
        "timed_out outranks the partial from the second subject"
    );
    let sealed = read_status(&paths);
    assert!(
        sealed.subjects[0]
            .execution
            .is_some_and(|exit| exit.timed_out)
    );
    assert_eq!(
        sealed.subjects[1].execution.map(|exit| exit.exit_code),
        Some(Some(4)),
        "a failing subject never stops the ones after it"
    );
    assert_bundle_is_self_consistent(&paths);
    std::fs::remove_dir_all(&results_root).unwrap();
}
