//! Tests for the fixed metric order and its round trip through field names.

use super::{SuiteMetric, metric_of_field};

#[test]
fn the_metric_order_is_fixed_and_round_trips_through_its_field_names() {
    assert_eq!(SuiteMetric::ALL.len(), 7);
    assert_eq!(SuiteMetric::ALL[0], SuiteMetric::Goodput);
    assert_eq!(SuiteMetric::ALL[2], SuiteMetric::P99Latency);
    for metric in SuiteMetric::ALL {
        assert_eq!(metric_of_field(metric.field()), Some(metric));
    }
    assert_eq!(metric_of_field("not_a_metric"), None);
    assert!(SuiteMetric::Goodput.higher_is_better());
    assert!(!SuiteMetric::P99Latency.higher_is_better());
}
