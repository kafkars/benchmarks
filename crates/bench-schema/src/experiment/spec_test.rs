//! Tests pinning the defaults and the empty-object shape of the document's
//! sections.
#![expect(
    clippy::unwrap_used,
    reason = "experiment fixtures are exact; a bad one must fail the test immediately"
)]

use crate::{BudgetSpec, SloSpec, canonical_bytes};

#[test]
fn the_default_budget_matches_the_control_plane_defaults() {
    let budget = BudgetSpec::default();

    assert_eq!(budget.run_timeout_seconds, 600);
    assert_eq!(budget.tool_timeout_seconds, 120);
    assert_eq!(budget.probe_timeout_seconds, 10);
    assert_eq!(budget.max_captured_output_bytes, 1_048_576);
}

#[test]
fn an_empty_optional_specification_still_serializes_as_an_object() {
    let bytes = canonical_bytes(&SloSpec::default()).unwrap();

    assert_eq!(String::from_utf8(bytes).unwrap(), "{}");
    assert!(SloSpec::default().is_empty());
}
