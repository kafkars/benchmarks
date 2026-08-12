//! The `kafkars.producer-benchmark.v2` document the `run` verb writes.
//!
//! Every accounting invariant the schema states holds by construction: every
//! offer is accepted and acknowledged, the three histograms carry exactly the
//! totals the outcome counts imply, scheduler lateness is present exactly for
//! the scheduled open-loop mode, and the run drains completely. Latencies are
//! sixteen distinct values cycled across the records, so the derived 99th
//! percentile is the top of the cycle regardless of how many records the
//! scenario asks for.

use bench_schema::{
    DeclaredExecution, EncodedHistogram, Histogram, LoadMode, MeasuredThroughput, OfferOutcomes,
    OfferTiming, ProcessResources, ProducerBenchmarkV2, QueueObservation, ResolvedExperiment,
};

use super::mode::Mode;

/// This fixture's adapter name.
pub(crate) const ADAPTER_NAME: &str = "fake-adapter";
/// This fixture's adapter version.
pub(crate) const ADAPTER_VERSION: &str = "0.1.0";

/// Distinct latency values cycled across the records of one histogram.
///
/// Sixteen is enough that the derived 99th percentile is the top of the cycle
/// for every record count above a hundred, and small enough that the sparse
/// bucket list stays short whatever the scenario asks for.
const LATENCY_STEPS: u64 = 16;

/// The four latency knobs one run's histograms are built from, all nanoseconds.
#[derive(Debug, Clone, Copy)]
struct LatencyShape {
    /// Smallest `terminal - intended`.
    terminal_base: u64,
    /// Distance between consecutive `terminal - intended` values.
    terminal_step: u64,
    /// Smallest `call_start - intended`.
    lateness_base: u64,
    /// Distance between consecutive `call_start - intended` values.
    lateness_step: u64,
}

impl LatencyShape {
    /// The shape of a run that comfortably meets every objective.
    ///
    /// `spread` makes the numbers differ per subject, so a comparison between
    /// two fixtures is a ratio somebody has to look at rather than a constant
    /// one.
    fn healthy(spread: u64) -> Self {
        Self {
            terminal_base: 200_000 + spread * 1_000,
            terminal_step: 20_000,
            lateness_base: 5_000,
            lateness_step: 1_000,
        }
    }

    /// The shape of a run that misses every objective the experiment declared.
    ///
    /// The top of the cycle lands a full second above the largest declared
    /// ceiling, so no rounding, no bucket width, and no reader's choice of
    /// percentile can make this run look satisfied.
    fn saturated(ceiling_ms: u64) -> Self {
        let target = ceiling_ms.saturating_add(1_000).saturating_mul(1_000_000);
        let step = target / (LATENCY_STEPS * 2);
        Self {
            terminal_base: target - step * (LATENCY_STEPS - 1),
            terminal_step: step,
            lateness_base: target - step * (LATENCY_STEPS - 1),
            lateness_step: step,
        }
    }
}

/// Builds a valid v2 producer measurement for one subject.
///
/// Every accounting invariant the schema states holds by construction: nothing
/// is refused admission, nothing fails, nothing is left unknown, and the three
/// histograms carry exactly the totals the outcome counts imply.
pub(crate) fn result_document(
    experiment: &ResolvedExperiment,
    subject: &str,
    mode: Mode,
) -> ProducerBenchmarkV2 {
    let run_id = experiment
        .runtime
        .as_ref()
        .map_or_else(String::new, |runtime| runtime.run_id.clone());
    let spread = u64::from(subject.bytes().fold(0u8, u8::wrapping_add));
    let records = experiment.records;
    let shape = latency_shape(experiment, mode, spread);
    let lateness = matches!(experiment.load_mode, LoadMode::ScheduledOpenLoopFixedRate)
        .then(|| latencies(records, shape.lateness_base, shape.lateness_step));
    ProducerBenchmarkV2 {
        schema: ProducerBenchmarkV2::SCHEMA.to_owned(),
        adapter: ADAPTER_NAME.to_owned(),
        adapter_version: ADAPTER_VERSION.to_owned(),
        run_id,
        load_mode: experiment.load_mode,
        declared: DeclaredExecution {
            payload_construction: "prebuilt-pool-per-offer-sequence".to_owned(),
            ownership: "copy-in".to_owned(),
            completion_mode: "aggregate-batch-terminal".to_owned(),
            serialization: "excluded".to_owned(),
        },
        outcomes: OfferOutcomes {
            offered: records,
            accepted: records,
            acknowledged: records,
            failed: 0,
            timed_out: 0,
            unknown: 0,
        },
        timing: OfferTiming {
            clock: "monotonic-ns".to_owned(),
            intended_to_terminal: latencies(records, shape.terminal_base, shape.terminal_step),
            accepted_to_terminal: latencies(
                records,
                shape.terminal_base / 2,
                shape.terminal_step / 2,
            ),
            call_start_to_accepted: latencies(records, 10_000, 1_000),
            intended_to_call_start: lateness,
        },
        throughput: throughput(experiment, spread),
        queue: QueueObservation {
            max_outstanding_observed: experiment.application.max_outstanding_records.min(records),
            max_outstanding_bytes_observed: Some(
                experiment
                    .application
                    .max_outstanding_records
                    .min(records)
                    .saturating_mul(u64::from(experiment.payload.bytes)),
            ),
            final_outstanding: 0,
        },
        resources: Some(ProcessResources {
            max_rss_bytes: 64 * 1024 * 1024,
            user_cpu_ns: records * 1_000,
            system_cpu_ns: records * 500,
        }),
        native_metrics_path: None,
        valid: true,
        invalid_reason: None,
    }
}

/// Chooses the latency shape this invocation reports.
///
/// Only `rate-slo:<threshold>` can choose the saturated shape, and it chooses it
/// purely from the resolved experiment: the offered rate above the threshold, and
/// the largest ceiling the experiment's objectives declare.
fn latency_shape(experiment: &ResolvedExperiment, mode: Mode, spread: u64) -> LatencyShape {
    let Mode::RateSlo(threshold) = mode else {
        return LatencyShape::healthy(spread);
    };
    let offered = experiment.offered_records_per_second.unwrap_or(0);
    if offered > threshold {
        let ceiling = experiment
            .slo
            .corrected_p99_ms
            .unwrap_or(0)
            .max(experiment.slo.schedule_delay_p99_ms.unwrap_or(0));
        LatencyShape::saturated(ceiling)
    } else {
        LatencyShape::healthy(spread)
    }
}

/// Records `count` latencies cycling through [`LATENCY_STEPS`] distinct values.
fn latencies(count: u64, base_ns: u64, step_ns: u64) -> EncodedHistogram {
    let mut histogram = Histogram::new();
    for index in 0..count {
        histogram.record(base_ns + (index % LATENCY_STEPS) * step_ns);
    }
    histogram.encode()
}

/// Goodput over the measured interval, deterministic per subject.
#[expect(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "a fixture's throughput is invented evidence, not identity-bearing arithmetic"
)]
fn throughput(experiment: &ResolvedExperiment, spread: u64) -> MeasuredThroughput {
    let acknowledged_records_per_second = 100_000.0 + spread as f64;
    let measured_duration_ns =
        (experiment.records as f64 / acknowledged_records_per_second * 1e9) as u64;
    MeasuredThroughput {
        measured_duration_ns,
        acknowledged_records_per_second,
        acknowledged_payload_bytes_per_second: acknowledged_records_per_second
            * f64::from(experiment.payload.bytes),
    }
}
