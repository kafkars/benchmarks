//! Tests pinning the wire strings of the experiment's closed vocabularies.
#![expect(
    clippy::unwrap_used,
    reason = "experiment fixtures are exact; a bad one must fail the test immediately"
)]

use crate::{ArrivalModel, ExperimentKind, LoadMode};

#[test]
fn the_load_mode_wire_strings_are_pinned() {
    assert_eq!(LoadMode::ClosedLoop.as_str(), "closed-loop");
    assert_eq!(
        LoadMode::ScheduledOpenLoopFixedRate.as_str(),
        "scheduled-open-loop-fixed-rate"
    );
    assert_eq!(
        serde_json::to_string(&LoadMode::ClosedLoop).unwrap(),
        r#""closed-loop""#
    );
    assert_eq!(
        serde_json::to_string(&LoadMode::ScheduledOpenLoopFixedRate).unwrap(),
        r#""scheduled-open-loop-fixed-rate""#
    );
}

#[test]
fn the_kind_and_arrival_wire_strings_are_pinned() {
    assert_eq!(
        serde_json::to_string(&ExperimentKind::Producer).unwrap(),
        r#""producer""#
    );
    assert_eq!(
        serde_json::to_string(&ArrivalModel::Deterministic).unwrap(),
        r#""deterministic""#
    );
    assert_eq!(
        serde_json::to_string(&ArrivalModel::Poisson).unwrap(),
        r#""poisson""#
    );
}
