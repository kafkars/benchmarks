//! Environment capture: the `kafkars.benchmark-environment.v2` document —
//! repository states, toolchain versions, host facts — with every unavailable
//! probe recorded as the literal string the schema reserves for it.
//!
//! # Never fails
//!
//! [`capture`] returns a document, not a result. A benchmark that refuses to
//! run because it could not read the CPU model has confused its evidence with
//! its measurement; a benchmark that omits the key it could not read invites
//! the reader to assume there was nothing to read. So every probe that fails
//! records [`UNAVAILABLE`], and a count that cannot be read records zero, which
//! the schema documents as "unreadable" rather than as a plausible value.
//!
//! # Faithful to v1, without inheriting its assumptions
//!
//! The legacy Node capture (`legacy/benchctl/environment.mjs`) is the reference
//! for the field set. v2 uses current public repository names (`kafkars`,
//! `kafka_driver`, `kafka_wire`), plus the same `rustc`/`cargo`/`cc` toolchain
//! probes, the same `uname -a`, and the same broker prose — the cluster is
//! "externally managed by the caller", because a harness that starts its own
//! broker is measuring its own startup.
//!
//! Three things deliberately differ. The repository map is open rather than
//! fixed, so this repository itself is captured under `kafka_benchmarks` and a
//! future subject can add its own. The librdkafka artifact pin is gone: it was
//! adapter provenance, and adapter provenance now lives in the subjects lock
//! next to the binary digest. And `node` is not recorded, because no Node
//! process participates in an attempt this control plane runs.
//!
//! A repository whose state cannot be read is recorded as dirty. That matches
//! the legacy capture and is the conservative reading: an unknown working tree
//! is not a clean one, and cleanliness is what claim eligibility would rest on.
//!
//! # Host probes are platform-agnostic at run time
//!
//! Memory and CPU model are read through the Darwin probe first and the Linux
//! probe second, taking whichever answers. Compiling both paths on both
//! platforms means the fallback chain is exercised by the test suite wherever
//! it runs, rather than only on the platform it was written for.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::SystemTime;

use bench_schema::{
    BENCHMARK_ENVIRONMENT_V2, BrokerFacts, EnvironmentDocument, HostFacts, RepositoryState,
    UNAVAILABLE,
};

use crate::time::utc_rfc3339_millis;

/// Lifecycle prose recorded for every cluster this repository measures.
pub const BROKER_LIFECYCLE: &str = "externally managed by the caller";

/// Broker version recorded when the operator did not state one.
pub const BROKER_VERSION_UNKNOWN: &str = "unknown";

/// Environment variable that relocates the Kafkars checkout.
pub const CLIENT_ROOT_VARIABLE: &str = "KAFKA_BENCH_CLIENT_ROOT";

/// Captures the environment an attempt is about to run in.
///
/// `repositories` maps a caller-chosen name to a checkout; `bootstrap` is the
/// endpoint list the subjects will use; `now` is the capture time, passed in so
/// that a test can pin it.
#[must_use]
pub fn capture(
    repositories: &[(String, PathBuf)],
    bootstrap: &str,
    now: SystemTime,
) -> EnvironmentDocument {
    EnvironmentDocument {
        schema: BENCHMARK_ENVIRONMENT_V2.to_owned(),
        captured_at: utc_rfc3339_millis(now),
        source: repositories
            .iter()
            .map(|(name, path)| (name.clone(), repository_state(path)))
            .collect(),
        toolchain: toolchain(),
        host: host(),
        broker: BrokerFacts {
            version: BROKER_VERSION_UNKNOWN.to_owned(),
            bootstrap: bootstrap.to_owned(),
            lifecycle: BROKER_LIFECYCLE.to_owned(),
        },
    }
}

/// Returns the repository list a real attempt captures: this repository, plus
/// the three sibling checkouts the measured clients are built from.
///
/// The sibling layout uses `kafkars`, `kafka-driver`, and `kafka-protocol`; the
/// last path is the public `kafkars/kafka-wire` repository because that is the
/// path Kafkars' reviewed workspace dependency declares. [`CLIENT_ROOT_VARIABLE`]
/// relocates Kafkars alone for compatibility with existing automation.
#[must_use]
pub fn default_repositories(repository_root: &Path) -> Vec<(String, PathBuf)> {
    let sibling = |name: &str| repository_root.join("..").join(name);
    let client =
        std::env::var_os(CLIENT_ROOT_VARIABLE).map_or_else(|| sibling("kafkars"), PathBuf::from);
    vec![
        ("kafka_benchmarks".to_owned(), repository_root.to_path_buf()),
        ("kafkars".to_owned(), client),
        ("kafka_driver".to_owned(), sibling("kafka-driver")),
        ("kafka_wire".to_owned(), sibling("kafka-protocol")),
    ]
}

/// Reads one checkout's commit and cleanliness.
fn repository_state(path: &Path) -> RepositoryState {
    let commit = run_in(path, "git", &["rev-parse", "HEAD"]);
    let porcelain = run_in(path, "git", &["status", "--porcelain"]);
    RepositoryState {
        commit: commit.unwrap_or_else(|| UNAVAILABLE.to_owned()),
        dirty: porcelain.is_none_or(|status| !status.is_empty()),
    }
}

/// Reads the versions of the tools that built whatever is being measured, and
/// the three build-identity facts that decide what those tools produced.
///
/// A compiler version alone does not identify a binary. The same `rustc` with
/// `-C target-cpu=native` and without it emits different code, a debug build
/// and a release build of the same source differ by an order of magnitude on
/// exactly the axis this repository measures, and a build resolved against a
/// drifted lock file is a build of different dependencies. All three are
/// recorded here so that a bundle says which of them was in force rather than
/// leaving a reader to assume the defaults.
fn toolchain() -> BTreeMap<String, String> {
    let mut toolchain: BTreeMap<String, String> = ["rustc", "cargo", "cc"]
        .into_iter()
        .map(|tool| {
            let version = first_line(run(tool, &["--version"]));
            (tool.to_owned(), version)
        })
        .collect();
    toolchain.insert("rustflags".to_owned(), rustflags());
    toolchain.insert("build_profile".to_owned(), BUILD_PROFILE.to_owned());
    // Constant, and deliberately recorded as one. Every build path in this
    // repository — `scripts/build-benchmark-adapters`, `scripts/check-*`, the
    // CI lanes, and the release procedure — passes `--locked`, so a resolution
    // that silently updated a dependency would fail the build rather than
    // produce a bundle. Writing the constant down means a future path that
    // drops the flag has to change this line to stay honest, instead of leaving
    // the bundle quietly claiming something that stopped being true.
    toolchain.insert("cargo_locked".to_owned(), "true".to_owned());
    toolchain
}

/// Which profile the running `benchctl` was compiled with.
///
/// Read off this binary rather than probed, because the question is what built
/// the control plane that is sealing this bundle, and a debug control plane is
/// a signal about the whole attempt.
const BUILD_PROFILE: &str = if cfg!(debug_assertions) {
    "debug"
} else {
    "release"
};

/// The `RUSTFLAGS` in force, or [`UNAVAILABLE`] when the variable is unset.
///
/// Unset and empty are the same fact here — no flags were added — but they are
/// reported as [`UNAVAILABLE`] rather than as an empty string so that the value
/// reads the same way as every other probe that had nothing to report.
fn rustflags() -> String {
    std::env::var("RUSTFLAGS")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| UNAVAILABLE.to_owned())
}

/// Reads the machine the attempt runs on.
fn host() -> HostFacts {
    HostFacts {
        platform: std::env::consts::OS.to_owned(),
        release: run("uname", &["-r"]).unwrap_or_else(|| UNAVAILABLE.to_owned()),
        architecture: std::env::consts::ARCH.to_owned(),
        cpu: cpu_model().unwrap_or_else(|| UNAVAILABLE.to_owned()),
        logical_cpus: std::thread::available_parallelism()
            .ok()
            .and_then(|count| u32::try_from(count.get()).ok())
            .unwrap_or(0),
        memory_bytes: memory_bytes().unwrap_or(0),
        uname: run("uname", &["-a"]).unwrap_or_else(|| UNAVAILABLE.to_owned()),
    }
}

/// Returns the CPU model: Darwin's `sysctl` key first, then `/proc/cpuinfo`.
fn cpu_model() -> Option<String> {
    run("sysctl", &["-n", "machdep.cpu.brand_string"]).or_else(|| {
        let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").ok()?;
        cpuinfo
            .lines()
            .find_map(|line| {
                line.split_once(':')
                    .filter(|(key, _)| key.trim() == "model name")
            })
            .map(|(_, value)| value.trim().to_owned())
    })
}

/// Returns physical memory in bytes: Darwin's `sysctl` key first, then
/// `/proc/meminfo`, whose `MemTotal` is in kibibytes.
fn memory_bytes() -> Option<u64> {
    if let Some(bytes) = run("sysctl", &["-n", "hw.memsize"]).and_then(|value| value.parse().ok()) {
        return Some(bytes);
    }
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let kibibytes: u64 = meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;
    kibibytes.checked_mul(1024)
}

/// Runs a program and returns its trimmed stdout, or `None` if anything at all
/// went wrong — a missing program, a non-zero exit, non-UTF-8 output.
fn run(program: &str, arguments: &[&str]) -> Option<String> {
    run_at(None, program, arguments)
}

/// Runs a program inside a directory.
fn run_in(directory: &Path, program: &str, arguments: &[&str]) -> Option<String> {
    run_at(Some(directory), program, arguments)
}

fn run_at(directory: Option<&Path>, program: &str, arguments: &[&str]) -> Option<String> {
    let mut command = Command::new(program);
    command
        .args(arguments)
        .stdin(Stdio::null())
        .stderr(Stdio::null());
    if let Some(directory) = directory {
        command.current_dir(directory);
    }
    let output = command.output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8(output.stdout).ok()?.trim().to_owned())
}

/// Reduces a multi-line version banner to its first line, or [`UNAVAILABLE`].
fn first_line(output: Option<String>) -> String {
    output
        .and_then(|text| text.lines().next().map(str::trim).map(str::to_owned))
        .filter(|line| !line.is_empty())
        .unwrap_or_else(|| UNAVAILABLE.to_owned())
}
