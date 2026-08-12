//! The two looping verbs, offline: a three-repetition suite that writes a
//! summary, both renderings, and a packet; and a capacity search that finds the
//! rate the fixture saturates at.
//!
//! Everything here runs the real `benchctl` binary against the `fake-adapter`
//! fixture playing every subject and all three cluster tools, so these are
//! end-to-end checks of the verb surface with no Kafka, no network, and no
//! clock-dependent behavior. The fixture's numbers are a pure function of the
//! resolved experiment, which is what makes both properties below assertable:
//! a suite summary that is identical across two independent runs, and a capacity
//! search that converges on a threshold somebody wrote down.
#![expect(
    clippy::unwrap_used,
    reason = "a fixture that cannot be built must fail the test immediately"
)]

mod common;

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use bench_schema::{
    AnalysisPacket, CapacitySearch, CapacityStatus, LlmSummary, SuiteSummary, Verdict,
};

/// The control-plane binary under test.
const BENCHCTL: &str = env!("CARGO_BIN_EXE_benchctl");

/// The offered rate above which the capacity fixture misses every objective.
const SATURATION_RATE: u64 = 50_000;

/// Repository root, two levels above this crate's manifest.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

/// One scratch directory per test, removed on success.
fn scratch(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "benchctl-suite-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Renders an argv as a TOML array of strings.
fn command_toml(argv: &[String]) -> String {
    let quoted: Vec<String> = argv.iter().map(|part| format!("{part:?}")).collect();
    format!("[{}]", quoted.join(", "))
}

/// Writes the subjects and cluster fixtures, every subject in `mode`.
fn write_fixtures(dir: &Path, mode: &str, subjects: &[&str]) -> (PathBuf, PathBuf) {
    let adapter = common::adapter(mode);
    let tool = |verb: &str| {
        let mut argv = adapter.clone();
        argv.push(verb.to_owned());
        command_toml(&argv)
    };
    let mut subjects_toml = String::new();
    for name in subjects {
        let _ = writeln!(
            subjects_toml,
            "[[subjects]]\nname = \"{name}\"\ncommand = {}\n",
            command_toml(&adapter)
        );
    }
    let subjects_path = dir.join("subjects.toml");
    std::fs::write(&subjects_path, subjects_toml).unwrap();
    let cluster_path = dir.join("cluster.toml");
    std::fs::write(
        &cluster_path,
        format!(
            "name = \"offline\"\n\
             bootstrap = \"127.0.0.1:39092,127.0.0.1:39093,127.0.0.1:39094\"\n\
             broker_version = \"4.3.1\"\nbrokers = 3\nsecurity = \"plaintext\"\n\
             lifecycle = \"externally managed by the caller\"\n\n\
             [tools]\ntopic_create = {create}\ntopic_delete = {delete}\nverify = {verify}\n",
            create = tool("topics-create"),
            delete = tool("topics-delete"),
            verify = tool("verify"),
        ),
    )
    .unwrap();
    (subjects_path, cluster_path)
}

/// The bootstrap every offline fixture binds to.
const BOOTSTRAP: &str = "127.0.0.1:39092,127.0.0.1:39093,127.0.0.1:39094";

/// Runs `benchctl` with `arguments` and returns its exit code, stdout, stderr.
fn benchctl_output(arguments: &[String]) -> (i32, String, String) {
    let output = Command::new(BENCHCTL).args(arguments).output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    let code = output.status.code().unwrap_or(-1);
    assert!(
        code >= 0,
        "benchctl died on a signal; stderr:\n{stderr}\narguments: {arguments:?}"
    );
    (
        code,
        String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr,
    )
}

/// Runs `benchctl` with `arguments` and returns its exit code and stdout.
fn benchctl(arguments: &[String]) -> (i32, String) {
    let (code, stdout, _stderr) = benchctl_output(arguments);
    (code, stdout)
}

/// The argv shared by `suite` and `capacity`.
fn common_arguments(
    verb: &str,
    scenario: &Path,
    subjects: &Path,
    cluster: &Path,
    results: &Path,
    reports: &Path,
) -> Vec<String> {
    [
        verb,
        "--experiment",
        scenario.to_str().unwrap(),
        "--subjects",
        subjects.to_str().unwrap(),
        "--cluster",
        cluster.to_str().unwrap(),
        "--bootstrap",
        BOOTSTRAP,
        "--results",
        results.to_str().unwrap(),
        "--reports",
        reports.to_str().unwrap(),
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

/// The single directory a report root holds, whatever it is named.
fn only_report_directory(reports: &Path) -> PathBuf {
    let mut found = Vec::new();
    let mut stack = vec![reports.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                if std::fs::read_dir(&path)
                    .unwrap()
                    .flatten()
                    .any(|child| child.path().is_file())
                {
                    found.push(path);
                } else {
                    stack.push(path);
                }
            }
        }
    }
    assert_eq!(
        found.len(),
        1,
        "expected one report directory, got {found:?}"
    );
    found.remove(0)
}

/// Runs a three-repetition suite and returns its report directory and exit code.
fn run_suite(dir: &Path, label: &str) -> (i32, PathBuf) {
    let (subjects, cluster) = write_fixtures(dir, "ok", &["kafkars", "librdkafka-c"]);
    let results = dir.join(format!("results-{label}"));
    let reports = dir.join(format!("reports-{label}"));
    let mut arguments = common_arguments(
        "suite",
        &repo_root().join("scenarios/producer/legacy-balanced-1k.toml"),
        &subjects,
        &cluster,
        &results,
        &reports,
    );
    arguments.push("--repetitions".to_owned());
    arguments.push("3".to_owned());
    let (code, _stdout) = benchctl(&arguments);
    (code, only_report_directory(&reports))
}

/// A suite summary with the two per-attempt fields that cannot repeat blanked.
///
/// Attempt ids carry a timestamp and entropy, and a bundle digest covers those
/// ids, so neither can be equal across two independent suites. Everything else
/// in the document — every observation, every median, every interval, every gate
/// — is derived from the resolved experiments and must be identical.
fn without_attempt_identity(bytes: &[u8]) -> Vec<u8> {
    let mut summary = SuiteSummary::from_slice(bytes).unwrap();
    for attempt in &mut summary.attempts {
        "<attempt>".clone_into(&mut attempt.attempt_id);
        "<digest>".clone_into(&mut attempt.bundle_digest);
    }
    bench_schema::pretty_bytes(&summary).unwrap()
}

#[test]
fn a_suite_writes_a_summary_both_renderings_and_a_packet() {
    let dir = scratch("reports");
    let (code, reports) = run_suite(&dir, "once");
    assert_eq!(code, 0, "every attempt completed and enough were valid");

    for name in [
        "suite-summary.json",
        "report.md",
        "report.html",
        "analysis-packet.json",
    ] {
        assert!(reports.join(name).is_file(), "{name} is missing");
    }

    let summary =
        SuiteSummary::from_slice(&std::fs::read(reports.join("suite-summary.json")).unwrap())
            .unwrap();
    assert_eq!(summary.repetitions, 3);
    assert_eq!(summary.attempts.len(), 3);
    assert!(!summary.claim_eligible, "never in this milestone");
    assert!(
        summary.attempts.iter().all(|attempt| attempt.run_valid),
        "the fixture's attempts are all valid: {:?}",
        summary.attempts
    );
    assert_eq!(
        summary.medians.len(),
        2,
        "one median row per subject: {:?}",
        summary.medians
    );

    let packet =
        AnalysisPacket::from_slice(&std::fs::read(reports.join("analysis-packet.json")).unwrap())
            .unwrap();
    assert_eq!(packet.validity.runs_total, 3);
    assert_eq!(packet.validity.runs_valid, 3);
    assert!(
        !packet.metrics.is_empty(),
        "a packet without metrics cites nothing"
    );

    let markdown = std::fs::read_to_string(reports.join("report.md")).unwrap();
    assert!(markdown.contains("kafkars"), "{markdown}");
    let html = std::fs::read_to_string(reports.join("report.html")).unwrap();
    assert!(html.contains("<table") || html.contains("<h1"), "{html}");

    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn two_independent_suites_summarize_to_the_same_bytes() {
    let dir = scratch("deterministic");
    let (first_code, first) = run_suite(&dir, "first");
    let (second_code, second) = run_suite(&dir, "second");
    assert_eq!(first_code, 0);
    assert_eq!(second_code, 0);
    assert_ne!(first, second, "the two suites wrote to different roots");

    let left = without_attempt_identity(&std::fs::read(first.join("suite-summary.json")).unwrap());
    let right =
        without_attempt_identity(&std::fs::read(second.join("suite-summary.json")).unwrap());
    assert_eq!(
        String::from_utf8(left).unwrap(),
        String::from_utf8(right).unwrap(),
        "the same experiment, repeated the same way, must summarize identically"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Writes the fixtures for a suite whose third repetition cannot be sealed.
///
/// The topic-cleanup tool is the sabotage: it runs once at the end of every
/// attempt, so on its second run it replaces `results/pending` — the directory
/// every attempt workspace is born in — with a regular file. The next
/// repetition therefore has nowhere to seal into, which is the one failure
/// [`AttemptEnd::Unsealable`] exists for, and the only one a suite cannot
/// record in a bundle.
fn write_fixtures_that_break_after_two_attempts(
    dir: &Path,
    results: &Path,
    subjects: &[&str],
) -> (PathBuf, PathBuf) {
    let adapter = common::adapter("ok");
    let tool = |verb: &str| {
        let mut argv = adapter.clone();
        argv.push(verb.to_owned());
        command_toml(&argv)
    };
    let marker = dir.join("cleanup-ran-once");
    let sabotage = command_toml(&[
        "/bin/sh".to_owned(),
        "-c".to_owned(),
        format!(
            "if [ -e {marker} ]; then rm -rf {pending}; : > {pending}; fi; : > {marker}; exit 0",
            marker = marker.display(),
            pending = results.join("pending").display(),
        ),
    ]);
    let mut subjects_toml = String::new();
    for name in subjects {
        let _ = writeln!(
            subjects_toml,
            "[[subjects]]\nname = \"{name}\"\ncommand = {}\n",
            command_toml(&adapter)
        );
    }
    let subjects_path = dir.join("subjects.toml");
    std::fs::write(&subjects_path, subjects_toml).unwrap();
    let cluster_path = dir.join("cluster.toml");
    std::fs::write(
        &cluster_path,
        format!(
            "name = \"offline\"\n\
             bootstrap = \"127.0.0.1:39092,127.0.0.1:39093,127.0.0.1:39094\"\n\
             broker_version = \"4.3.1\"\nbrokers = 3\nsecurity = \"plaintext\"\n\
             lifecycle = \"externally managed by the caller\"\n\n\
             [tools]\ntopic_create = {create}\ntopic_delete = {sabotage}\nverify = {verify}\n",
            create = tool("topics-create"),
            verify = tool("verify"),
        ),
    )
    .unwrap();
    (subjects_path, cluster_path)
}

#[test]
fn a_repetition_that_cannot_be_sealed_stops_the_suite_and_is_the_exit_code() {
    let dir = scratch("unsealable");
    let results = dir.join("results");
    let reports = dir.join("reports");
    let (subjects, cluster) =
        write_fixtures_that_break_after_two_attempts(&dir, &results, &["kafkars", "librdkafka-c"]);
    let mut arguments = common_arguments(
        "suite",
        &repo_root().join("scenarios/producer/legacy-balanced-1k.toml"),
        &subjects,
        &cluster,
        &results,
        &reports,
    );
    arguments.push("--repetitions".to_owned());
    arguments.push("3".to_owned());

    let (code, stdout, stderr) = benchctl_output(&arguments);

    assert_eq!(
        code, 70,
        "the exit code is the unsealable repetition's, not the verdict over the two that \
         sealed; stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("the suite stopped after 2 of 3 repetitions"),
        "the stop has to be said, not only exited: {stderr}"
    );
    assert!(
        stderr.contains("cover only the 2 attempts that sealed"),
        "the reports must not be presented as the whole suite: {stderr}"
    );

    // The two attempts that did seal are still summarized: they happened, and a
    // suite that threw them away would be destroying evidence over a workspace
    // it could not claim.
    let directory = only_report_directory(&reports);
    let summary =
        SuiteSummary::from_slice(&std::fs::read(directory.join("suite-summary.json")).unwrap())
            .unwrap();
    assert_eq!(summary.repetitions, 2, "two bundles were summarized");
    assert_eq!(summary.attempts.len(), 2);
    assert!(summary.attempts.iter().all(|attempt| attempt.run_valid));
    assert!(
        stdout.contains("suite-summary.json"),
        "the report paths are still printed: {stdout}"
    );

    std::fs::remove_dir_all(&dir).unwrap();
}

/// A fixed-rate scenario that carries both a `[search]` section and objectives.
fn capacity_scenario(dir: &Path) -> PathBuf {
    let path = dir.join("capacity.toml");
    std::fs::write(
        &path,
        "name = \"offline-capacity\"\n\
         status = \"diagnostic\"\n\
         claim_eligible = false\n\
         load_mode = \"scheduled-open-loop-fixed-rate\"\n\
         records = 2000\n\
         warmup_records = 200\n\
         offered_records_per_second = 20000\n\
         repetitions_per_rate = 2\n\
         \n\
         [search]\n\
         initial_records_per_second = 20000\n\
         minimum_records_per_second = 1000\n\
         maximum_records_per_second = 400000\n\
         growth_factor = 2\n\
         resolution_percent = 5\n\
         derived_fixed_load_percentages = [50, 90]\n\
         \n\
         [slo]\n\
         corrected_p99_ms = 250\n\
         schedule_delay_p99_ms = 50\n\
         \n\
         [application]\n\
         producer_instances = 1\n\
         callers_per_producer = 4\n\
         backpressure = \"block-within-original-offer\"\n\
         queue_bytes = 67108864\n\
         max_outstanding_records = 8192\n\
         \n\
         [application_api]\n\
         admission_shape = \"public-batch\"\n\
         completion_shape = \"aggregate-batch-terminal\"\n\
         batch_records = 256\n\
         \n\
         [payload]\n\
         bytes = 1024\n\
         profile = \"deterministic-ascii-envelope\"\n\
         seed = 44\n\
         \n\
         [producer]\n\
         acks = \"all\"\n\
         idempotence = true\n\
         compression = \"none\"\n\
         linger_ms = 5\n\
         batch_records = 256\n\
         batch_bytes = 65536\n\
         request_bytes = 1048576\n\
         delivery_timeout_ms = 60000\n\
         partitioning = \"explicit-round-robin\"\n\
         retry_max_replacements = 600\n\
         retry_backoff_ms = 100\n\
         \n\
         [cluster]\n\
         brokers = 3\n\
         partitions = 12\n\
         replication_factor = 3\n\
         min_in_sync_replicas = 2\n\
         security = \"plaintext\"\n",
    )
    .unwrap();
    path
}

#[test]
fn a_capacity_search_converges_on_the_rate_the_fixture_saturates_at() {
    let dir = scratch("capacity");
    let scenario = capacity_scenario(&dir);
    let (subjects, cluster) =
        write_fixtures(&dir, &format!("rate-slo:{SATURATION_RATE}"), &["kafkars"]);
    let results = dir.join("results");
    let reports = dir.join("reports");
    let arguments = common_arguments(
        "capacity", &scenario, &subjects, &cluster, &results, &reports,
    );
    let (code, _stdout) = benchctl(&arguments);
    assert_eq!(code, 0, "a converged search exits successfully");

    let directory = only_report_directory(&reports);
    assert!(directory.join("report.md").is_file());
    let document =
        CapacitySearch::from_slice(&std::fs::read(directory.join("capacity-search.json")).unwrap())
            .unwrap();
    assert_eq!(document.status, CapacityStatus::Converged);
    assert_eq!(document.subject, "kafkars");
    let confirmed = document.confirmed_rate.unwrap();
    assert!(
        confirmed <= SATURATION_RATE,
        "a confirmed rate above the saturation point would be a rate that missed: {confirmed}"
    );
    assert!(
        SATURATION_RATE - confirmed <= document.resolution,
        "converged on {confirmed}, which is more than the resolution {} below {SATURATION_RATE}",
        document.resolution
    );
    assert_eq!(
        document.confirmation.len(),
        2,
        "the scenario asks for two confirmations"
    );
    assert!(document.confirmation.iter().all(|probe| probe.satisfied));

    // Every probe is an ordinary sealed attempt, and the ladder brackets the
    // threshold from both sides.
    assert!(document.probes.iter().any(|probe| probe.satisfied));
    assert!(document.probes.iter().any(|probe| !probe.satisfied));
    for probe in document.probes.iter().chain(&document.confirmation) {
        assert_eq!(probe.bundle_digest.len(), 64, "{probe:?}");
        assert!(!probe.attempt_id.is_empty());
    }

    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_probe_the_bundle_calls_invalid_is_never_a_satisfied_rate() {
    // `verifier-invalid` leaves the adapter's `run` alone: every probe writes a
    // healthy result document that meets both declared objectives. Only the
    // read-back verifier disagrees, which is exactly the shape that used to slip
    // through — `evaluate_slo` sees nothing wrong with numbers whose attempt the
    // seal has already refused to vouch for.
    let dir = scratch("capacity-invalid");
    let scenario = capacity_scenario(&dir);
    let (subjects, cluster) = write_fixtures(&dir, "verifier-invalid", &["kafkars"]);
    let results = dir.join("results");
    let reports = dir.join("reports");
    let arguments = common_arguments(
        "capacity", &scenario, &subjects, &cluster, &results, &reports,
    );

    let (code, _stdout) = benchctl(&arguments);

    assert_eq!(
        code, 20,
        "a search that never found a believable rate is not a success"
    );
    let directory = only_report_directory(&reports);
    let document =
        CapacitySearch::from_slice(&std::fs::read(directory.join("capacity-search.json")).unwrap())
            .unwrap();
    assert_eq!(document.status, CapacityStatus::Unconverged);
    assert_eq!(document.confirmed_rate, None, "nothing was confirmed");
    assert!(!document.probes.is_empty(), "the ladder still probed");
    assert!(
        document.probes.iter().all(|probe| !probe.satisfied),
        "no probe may be satisfied while its own bundle says the run is not believable"
    );
    for probe in &document.probes {
        assert!(
            probe
                .reasons
                .iter()
                .any(|reason| reason.starts_with("run validity:")),
            "the failing gate has to be named: {:?}",
            probe.reasons
        );
        assert!(
            probe
                .reasons
                .iter()
                .any(|reason| { reason.contains("did not satisfy the verification contract") }),
            "and the bundle's own reason carried through: {:?}",
            probe.reasons
        );
    }
    // Unsatisfied at the initial rate sends the ladder downwards, so the search
    // ends at the floor rather than at the ceiling.
    assert_eq!(document.bracket_low, None);
    assert!(document.bracket_high.is_some());

    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_capacity_search_refuses_a_scenario_that_declares_no_objectives() {
    let dir = scratch("capacity-no-slo");
    let text = std::fs::read_to_string(capacity_scenario(&dir)).unwrap();
    let stripped = text
        .replace("corrected_p99_ms = 250\n", "")
        .replace("schedule_delay_p99_ms = 50\n", "");
    let scenario = dir.join("no-slo.toml");
    std::fs::write(&scenario, stripped.replace("[slo]\n", "")).unwrap();
    let (subjects, cluster) = write_fixtures(&dir, "ok", &["kafkars"]);
    let arguments = common_arguments(
        "capacity",
        &scenario,
        &subjects,
        &cluster,
        &dir.join("results"),
        &dir.join("reports"),
    );
    let (code, _stdout) = benchctl(&arguments);
    assert_eq!(code, 65, "an unsearchable scenario is invalid input");
    std::fs::remove_dir_all(&dir).unwrap();
}

/// A minimal LLM summary over `packet`, with `verdict` substituted.
///
/// It cites nothing, which is legal: the guardrail bounds what prose *may*
/// claim, not how much it must. That makes it the sharpest test of the one rule
/// that has no escape hatch — the verdict is copied, never concluded.
fn llm_summary(verdict: Verdict) -> Vec<u8> {
    bench_schema::pretty_bytes(&LlmSummary {
        schema: LlmSummary::SCHEMA.to_owned(),
        verdict,
        executive_summary: "Written by a test to exercise the guardrail.".to_owned(),
        findings: Vec::new(),
        hypotheses: Vec::new(),
        next_experiments: Vec::new(),
        caveats: vec!["This summary interprets nothing.".to_owned()],
    })
    .unwrap()
}

/// The verdict that is not `verdict`, so a rejection is always available.
fn other_verdict(verdict: Verdict) -> Verdict {
    if verdict == Verdict::Invalid {
        Verdict::Improved
    } else {
        Verdict::Invalid
    }
}

#[test]
fn packet_binds_prose_to_the_verdict_the_numbers_produced() {
    let dir = scratch("packet");
    let (_code, reports) = run_suite(&dir, "packet");
    let summary_path = reports.join("suite-summary.json");
    let packet =
        AnalysisPacket::from_slice(&std::fs::read(reports.join("analysis-packet.json")).unwrap())
            .unwrap();

    let agreeing = dir.join("agreeing.json");
    std::fs::write(&agreeing, llm_summary(packet.verdict)).unwrap();
    let (code, stdout) = benchctl(&[
        "packet".to_owned(),
        "--suite".to_owned(),
        summary_path.to_str().unwrap().to_owned(),
        "--llm-summary".to_owned(),
        agreeing.to_str().unwrap().to_owned(),
    ]);
    assert_eq!(code, 0, "a summary that copies the verdict is accepted");
    assert!(stdout.contains("bound to the packet"), "{stdout}");

    let overruling = dir.join("overruling.json");
    std::fs::write(&overruling, llm_summary(other_verdict(packet.verdict))).unwrap();
    let (code, _stdout) = benchctl(&[
        "packet".to_owned(),
        "--suite".to_owned(),
        summary_path.to_str().unwrap().to_owned(),
        "--llm-summary".to_owned(),
        overruling.to_str().unwrap().to_owned(),
    ]);
    assert_eq!(
        code, 65,
        "prose may not overrule the deterministic verdict, and saying so is the whole point"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn report_renders_a_sealed_bundle_as_markdown() {
    let dir = scratch("report-verb");
    let (_code, reports) = run_suite(&dir, "render");
    let summary =
        SuiteSummary::from_slice(&std::fs::read(reports.join("suite-summary.json")).unwrap())
            .unwrap();
    let attempt = &summary.attempts[0];
    let bundle = find_bundle(&dir.join("results-render"), &attempt.attempt_id);
    let (code, stdout) = benchctl(&[
        "report".to_owned(),
        "--bundle".to_owned(),
        bundle.to_str().unwrap().to_owned(),
    ]);
    assert_eq!(code, 0);
    assert!(stdout.contains("kafkars"), "{stdout}");

    // `--out` into a directory that does not exist yet. `reports/` is generated
    // output and is not checked in, so on a fresh clone this is the *first*
    // shape a reader tries, and it must not need a `mkdir -p` first.
    let out = dir.join("fresh").join("nested").join("report.md");
    assert!(!out.parent().unwrap().exists());
    let (code, _stdout) = benchctl(&[
        "report".to_owned(),
        "--bundle".to_owned(),
        bundle.to_str().unwrap().to_owned(),
        "--out".to_owned(),
        out.to_str().unwrap().to_owned(),
    ]);
    assert_eq!(code, 0, "a nested --out path creates its own directory");
    let written = std::fs::read_to_string(&out).unwrap();
    assert_eq!(written, stdout, "the file and stdout render identically");

    // A bare filename has no parent to create, and must still work.
    let (code, _stdout) = benchctl(&[
        "report".to_owned(),
        "--bundle".to_owned(),
        bundle.to_str().unwrap().to_owned(),
        "--out".to_owned(),
        dir.join("beside.md").to_str().unwrap().to_owned(),
    ]);
    assert_eq!(code, 0);
    assert!(dir.join("beside.md").is_file());
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Finds the bundle directory an attempt id names, anywhere under `results`.
fn find_bundle(results: &Path, attempt_id: &str) -> PathBuf {
    let mut stack = vec![results.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|name| name == attempt_id) {
                    return path;
                }
                stack.push(path);
            }
        }
    }
    panic!("no bundle named {attempt_id} under {}", results.display());
}
