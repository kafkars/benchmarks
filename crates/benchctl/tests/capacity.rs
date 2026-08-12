//! The capacity verb, offline: a rate ladder that converges on the rate the
//! fixture saturates at, and the two ways a search legitimately does not
//! converge.
//!
//! The fixture's latencies are a step function of the resolved experiment
//! alone, so there is no clock, no load, and no run-to-run variation to bracket
//! around: the search converges on a threshold somebody wrote down, which is
//! what makes the assertion below an equality rather than a tolerance.
#![expect(
    clippy::unwrap_used,
    reason = "a fixture that cannot be built must fail the test immediately"
)]

mod common;

use std::path::{Path, PathBuf};

use bench_schema::{CapacitySearch, CapacityStatus};

use common::harness::{benchctl, common_arguments, only_report_directory, scratch, write_fixtures};

/// The offered rate above which the capacity fixture misses every objective.
const SATURATION_RATE: u64 = 50_000;

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
