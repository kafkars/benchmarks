//! Segmented fixed-load latency evidence tests.

use std::{fs, time::Duration};

use crate::report::ApplicationBatchAdmissionMetrics;

use super::{FixedLatencySample, FixedPhaseResult, write_latencies};

#[test]
fn summaries_read_compact_caller_segments_without_a_merged_sample_copy() {
    let result = result(vec![vec![sample(0, 10, 20)], vec![sample(1, 30, 60)]]);
    let (uncorrected, corrected, schedule_delay) = result.latency_reports();
    assert_eq!(uncorrected.p50, 10);
    assert_eq!(uncorrected.max, 30);
    assert_eq!(corrected.p50, 20);
    assert_eq!(corrected.max, 60);
    assert_eq!(schedule_delay.p50, 10);
    assert_eq!(schedule_delay.max, 30);
}

#[test]
fn csv_writer_merges_sorted_caller_segments_in_sequence_order() {
    let path = std::env::temp_dir().join(format!(
        "kafkars-fixed-latencies-{}-{}.csv",
        std::process::id(),
        std::thread::current().name().unwrap_or("test")
    ));
    let segments = vec![
        vec![sample(0, 10, 20), sample(2, 30, 60)],
        vec![sample(1, 20, 40)],
    ];
    write_latencies(&path, &segments).unwrap_or_else(|error| panic!("write evidence: {error}"));
    let evidence =
        fs::read_to_string(&path).unwrap_or_else(|error| panic!("read evidence: {error}"));
    fs::remove_file(&path).unwrap_or_else(|error| panic!("remove evidence: {error}"));
    let sequences = evidence
        .lines()
        .skip(1)
        .map(|line| line.split(',').next().unwrap_or("missing"))
        .collect::<Vec<_>>();
    assert_eq!(sequences, ["0", "1", "2"]);
}

fn result(samples: Vec<Vec<FixedLatencySample>>) -> FixedPhaseResult {
    let acknowledged = samples.iter().map(Vec::len).sum();
    FixedPhaseResult {
        offered: acknowledged,
        accepted: acknowledged,
        acknowledged,
        failed: 0,
        duration: Duration::from_secs(1),
        samples,
        failure_details: Vec::new(),
        batch_admission: ApplicationBatchAdmissionMetrics::default(),
    }
}

fn sample(sequence: usize, uncorrected: u64, corrected: u64) -> FixedLatencySample {
    let intended_ns = u64::try_from(sequence).unwrap_or(u64::MAX) * 10;
    let admitted_ns = intended_ns + (corrected - uncorrected);
    FixedLatencySample {
        sequence,
        intended_ns,
        admitted_ns,
        completed_ns: intended_ns + corrected,
        uncorrected_latency_ns: uncorrected,
        corrected_latency_ns: corrected,
    }
}
