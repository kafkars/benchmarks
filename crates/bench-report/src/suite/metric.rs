//! The metrics a suite compares subjects on, in the one order every report
//! lists them in.

use bench_schema::{SubjectMedians, SuiteSubjectObservation};

/// One metric a suite compares subjects on.
///
/// The order of the variants is the order every report lists them in, and the
/// order [`build_packet`](crate::build_packet) assigns metric ids in. It is
/// fixed on purpose: a reader who learns that `M003` is the p99 should not have
/// to re-learn it next release.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SuiteMetric {
    /// Acknowledged records per second; larger is better.
    Goodput,
    /// Median offer-to-terminal latency.
    P50Latency,
    /// 99th percentile offer-to-terminal latency.
    P99Latency,
    /// 99.9th percentile offer-to-terminal latency.
    P999Latency,
    /// 99th percentile admission wait.
    P99AdmissionWait,
    /// CPU core-seconds consumed.
    CpuCoreSeconds,
    /// Peak resident set size.
    MaxRssBytes,
}

impl SuiteMetric {
    /// Every metric, in the fixed reporting order.
    pub const ALL: [Self; 7] = [
        Self::Goodput,
        Self::P50Latency,
        Self::P99Latency,
        Self::P999Latency,
        Self::P99AdmissionWait,
        Self::CpuCoreSeconds,
        Self::MaxRssBytes,
    ];

    /// The `kafkars.suite-summary.v1` field name this metric is carried in.
    #[must_use]
    pub const fn field(self) -> &'static str {
        match self {
            Self::Goodput => "acknowledged_records_per_second",
            Self::P50Latency => "p50_intended_to_terminal_ns",
            Self::P99Latency => "p99_intended_to_terminal_ns",
            Self::P999Latency => "p999_intended_to_terminal_ns",
            Self::P99AdmissionWait => "p99_admission_wait_ns",
            Self::CpuCoreSeconds => "cpu_core_seconds",
            Self::MaxRssBytes => "max_rss_bytes",
        }
    }

    /// The human name a report prints.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Goodput => "acknowledged goodput",
            Self::P50Latency => "p50 offer-to-terminal",
            Self::P99Latency => "p99 offer-to-terminal",
            Self::P999Latency => "p99.9 offer-to-terminal",
            Self::P99AdmissionWait => "p99 admission wait",
            Self::CpuCoreSeconds => "cpu",
            Self::MaxRssBytes => "peak rss",
        }
    }

    /// The unit the metric's values are in.
    #[must_use]
    pub const fn unit(self) -> &'static str {
        match self {
            Self::Goodput => "records/s",
            Self::P50Latency | Self::P99Latency | Self::P999Latency | Self::P99AdmissionWait => {
                "ns"
            }
            Self::CpuCoreSeconds => "core-seconds",
            Self::MaxRssBytes => "bytes",
        }
    }

    /// Whether a larger value is the better outcome.
    ///
    /// Goodput is the only one. Everything else here is a cost.
    #[must_use]
    pub const fn higher_is_better(self) -> bool {
        matches!(self, Self::Goodput)
    }

    /// Reads this metric out of one attempt's observation.
    #[must_use]
    #[expect(
        clippy::cast_precision_loss,
        reason = "latency and byte counts are reporting statistics, not identity arithmetic"
    )]
    pub fn observed(self, observation: &SuiteSubjectObservation) -> Option<f64> {
        match self {
            Self::Goodput => Some(observation.acknowledged_records_per_second),
            Self::P50Latency => Some(observation.p50_intended_to_terminal_ns as f64),
            Self::P99Latency => Some(observation.p99_intended_to_terminal_ns as f64),
            Self::P999Latency => Some(observation.p999_intended_to_terminal_ns as f64),
            Self::P99AdmissionWait => Some(observation.p99_admission_wait_ns as f64),
            Self::CpuCoreSeconds => observation.cpu_core_seconds,
            Self::MaxRssBytes => observation.max_rss_bytes.map(|value| value as f64),
        }
    }

    /// Reads this metric out of one subject's medians.
    #[must_use]
    #[expect(
        clippy::cast_precision_loss,
        reason = "latency and byte counts are reporting statistics, not identity arithmetic"
    )]
    pub fn median_of(self, medians: &SubjectMedians) -> Option<f64> {
        match self {
            Self::Goodput => Some(medians.acknowledged_records_per_second),
            Self::P50Latency => Some(medians.p50_intended_to_terminal_ns as f64),
            Self::P99Latency => Some(medians.p99_intended_to_terminal_ns as f64),
            Self::P999Latency => Some(medians.p999_intended_to_terminal_ns as f64),
            Self::P99AdmissionWait => Some(medians.p99_admission_wait_ns as f64),
            Self::CpuCoreSeconds => medians.cpu_core_seconds,
            Self::MaxRssBytes => medians.max_rss_bytes.map(|value| value as f64),
        }
    }

    /// The sentence a gate over this metric enforces.
    #[must_use]
    pub fn gate_description(self, threshold: f64) -> String {
        if self.higher_is_better() {
            format!(
                "{} improves only when the whole confidence interval is above {:.3} \
                 (larger is better)",
                self.label(),
                1.0 + threshold
            )
        } else {
            format!(
                "{} improves only when the whole confidence interval is below {:.3} \
                 (smaller is better)",
                self.label(),
                1.0 - threshold
            )
        }
    }
}

/// Returns the metric a `kafkars.suite-summary.v1` field name refers to.
#[must_use]
pub fn metric_of_field(field: &str) -> Option<SuiteMetric> {
    SuiteMetric::ALL
        .into_iter()
        .find(|metric| metric.field() == field)
}
