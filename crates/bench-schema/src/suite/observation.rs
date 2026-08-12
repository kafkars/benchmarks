//! What one subject did — in one attempt, and as a median over attempts.
//!
//! [`SubjectMedians`] mirrors [`SuiteSubjectObservation`] field for field on
//! purpose, so that reading a median and reading the observations it came from
//! is the same act. A field that existed on only one of the two would be a
//! number a reader could not audit against its inputs.
//!
//! Three of those fields are *attribution* rather than comparison: `declared`
//! says what work was actually in the measured path,
//! `p99_intended_to_call_start_ns` says whether the schedule was kept, and
//! `p99_accepted_to_terminal_ns` isolates the client-internal portion of the
//! latency. Attribution locates a difference; it does not establish one. Only
//! `p99_intended_to_terminal_ns` and the goodput beside it can do that, because
//! only they include the time a subject spent refusing to accept work.

use serde::{Deserialize, Serialize};

use crate::result_v2::DeclaredExecution;
use crate::status::ExecutionStatus;

/// One subject's numbers as one attempt reported them.
///
/// Every latency is the value a reader derives from the attempt's histograms,
/// in nanoseconds, and the offer-to-terminal ones include admission wait —
/// these are `intended_to_terminal` percentiles, so a subject cannot look
/// faster by spending longer refusing to accept work.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuiteSubjectObservation {
    /// Subject name, matching the resolved experiment.
    pub name: String,
    /// The subject's role in this comparison, when it declared one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// What the adapter declared it did in the measured path.
    ///
    /// Absent for an attempt whose result document could not be read. Present,
    /// it is what makes `matched-execution-surface` checkable from the summary
    /// alone rather than only from the bundles it was drawn from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared: Option<DeclaredExecution>,
    /// Acknowledged records per second over the measured interval.
    pub acknowledged_records_per_second: f64,
    /// Median offer-to-terminal latency.
    pub p50_intended_to_terminal_ns: u64,
    /// 99th percentile offer-to-terminal latency.
    pub p99_intended_to_terminal_ns: u64,
    /// 99.9th percentile offer-to-terminal latency.
    pub p999_intended_to_terminal_ns: u64,
    /// 99th percentile of the admission wait alone.
    pub p99_admission_wait_ns: u64,
    /// 99th percentile scheduler lateness — `call_start - intended`.
    ///
    /// Present exactly when the load mode had a schedule to be late against.
    /// A closed-loop run reports nothing here rather than zero: there was no
    /// schedule, so there is no lateness, which is a different statement from
    /// "the schedule was kept perfectly".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p99_intended_to_call_start_ns: Option<u64>,
    /// 99th percentile of the client-internal portion — `terminal - accepted`.
    ///
    /// This is the number that *locates* a latency difference inside the
    /// client. It cannot claim one: a client that refuses admission for longer
    /// looks better here by construction, which is exactly the failure v2 was
    /// minted to stop. Compare on `p99_intended_to_terminal_ns` and read this
    /// beside it.
    #[serde(default)]
    pub p99_accepted_to_terminal_ns: u64,
    /// Peak resident set size, when the platform reported one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rss_bytes: Option<u64>,
    /// CPU core-seconds consumed, when the platform reported them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_core_seconds: Option<f64>,
    /// CPU core-seconds per million acknowledged records, when both were
    /// reported.
    ///
    /// Total CPU is not comparable between subjects that moved different
    /// amounts of traffic; this is. Absent rather than zero when the platform
    /// reported no resources, or when nothing was acknowledged to divide by.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_core_seconds_per_million_acknowledged: Option<f64>,
}

/// One attempt of the repeated experiment, named by the bundle it came from.
///
/// Invalid attempts stay in the list. A suite that silently dropped them would
/// report a median over a set nobody can reconstruct, and the count of what was
/// discarded is exactly the fact a sceptical reader wants first.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuiteAttempt {
    /// Attempt id, which also names the bundle directory.
    pub attempt_id: String,
    /// The sealed bundle's digest, so the attempt can be fetched and checked.
    pub bundle_digest: String,
    /// How far the attempt got.
    pub execution_status: ExecutionStatus,
    /// Whether the attempt's evidence may be believed; the separate axis.
    pub run_valid: bool,
    /// One entry per subject the attempt measured.
    pub subjects: Vec<SuiteSubjectObservation>,
}

/// One subject's median across the valid attempts, field for field.
///
/// The fields mirror [`SuiteSubjectObservation`] exactly, so that reading a
/// median and reading the observations it came from is the same act.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectMedians {
    /// Subject name, matching the resolved experiment.
    pub name: String,
    /// The subject's role in this comparison, when it declared one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// The declared execution surface every valid attempt agreed on.
    ///
    /// Absent when the attempts disagreed, which is itself the finding: a
    /// subject that changed what it was doing between repetitions has no one
    /// declaration to carry, and the `matched-execution-surface` gate reads the
    /// per-attempt values rather than this one for exactly that reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared: Option<DeclaredExecution>,
    /// Median acknowledged records per second.
    pub acknowledged_records_per_second: f64,
    /// Median of the per-attempt median offer-to-terminal latencies.
    pub p50_intended_to_terminal_ns: u64,
    /// Median of the per-attempt 99th percentile offer-to-terminal latencies.
    pub p99_intended_to_terminal_ns: u64,
    /// Median of the per-attempt 99.9th percentile offer-to-terminal latencies.
    pub p999_intended_to_terminal_ns: u64,
    /// Median of the per-attempt 99th percentile admission waits.
    pub p99_admission_wait_ns: u64,
    /// Median 99th percentile scheduler lateness, when every attempt had a
    /// schedule to be late against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p99_intended_to_call_start_ns: Option<u64>,
    /// Median 99th percentile of the client-internal portion.
    #[serde(default)]
    pub p99_accepted_to_terminal_ns: u64,
    /// Median peak resident set size, when every attempt reported one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rss_bytes: Option<u64>,
    /// Median CPU core-seconds, when every attempt reported them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_core_seconds: Option<f64>,
    /// Median CPU core-seconds per million acknowledged records, when every
    /// attempt reported them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_core_seconds_per_million_acknowledged: Option<f64>,
}
