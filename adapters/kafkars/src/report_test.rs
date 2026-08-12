//! Nearest-rank percentile behavior for normalized adapter output.

use super::report;

#[test]
fn percentiles_use_deterministic_nearest_rank_selection() {
    let mut values = (1..=1_000).rev().collect::<Vec<u128>>();
    let report = report::latencies(&mut values);

    assert_eq!(report.p50, 500);
    assert_eq!(report.p95, 950);
    assert_eq!(report.p99, 990);
    assert_eq!(report.p999, 999);
    assert_eq!(report.max, 1_000);
}
