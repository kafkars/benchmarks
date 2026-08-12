//! Tests for the objective evaluation the capacity search bisects on.
#![expect(
    clippy::indexing_slicing,
    reason = "a reason list whose length the test just asserted may be indexed"
)]

use bench_schema::SloSpec;

use crate::fixture::ResultFixture;
use crate::slo::{MAX_FAILED_RECORDS, evaluate_slo};

/// The objectives a capacity probe usually declares.
fn objectives() -> SloSpec {
    SloSpec {
        corrected_p99_ms: Some(250),
        ..SloSpec::default()
    }
}

#[test]
fn the_failure_budget_is_zero_and_says_why() {
    assert_eq!(MAX_FAILED_RECORDS, 0);
}

#[test]
fn a_clean_run_inside_its_latency_budget_is_satisfied() {
    let result = ResultFixture {
        terminal_ns: 200_000_000,
        ..ResultFixture::default()
    }
    .build();

    let verdict = evaluate_slo(&result, &objectives());

    assert!(verdict.satisfied, "{:?}", verdict.reasons);
    assert!(verdict.reasons.is_empty());
}

#[test]
fn a_run_at_exactly_the_budget_is_satisfied() {
    let result = ResultFixture {
        terminal_ns: 250_000_000,
        ..ResultFixture::default()
    }
    .build();

    let verdict = evaluate_slo(&result, &objectives());

    assert!(verdict.satisfied, "{:?}", verdict.reasons);
}

#[test]
fn a_run_over_its_latency_budget_names_the_percentile_and_the_bound() {
    let result = ResultFixture {
        terminal_ns: 400_000_000,
        ..ResultFixture::default()
    }
    .build();

    let verdict = evaluate_slo(&result, &objectives());

    assert!(!verdict.satisfied);
    assert_eq!(verdict.reasons.len(), 1);
    assert!(
        verdict.reasons[0].contains("p99 offer-to-terminal")
            && verdict.reasons[0].contains("250 ms"),
        "{}",
        verdict.reasons[0]
    );
}

#[test]
fn failed_and_timed_out_records_both_break_the_budget() {
    let failed = ResultFixture {
        acknowledged: 990,
        failed: 10,
        ..ResultFixture::default()
    }
    .build();
    let timed_out = ResultFixture {
        acknowledged: 995,
        timed_out: 5,
        ..ResultFixture::default()
    }
    .build();

    assert!(!evaluate_slo(&failed, &objectives()).satisfied);
    assert!(!evaluate_slo(&timed_out, &objectives()).satisfied);
}

#[test]
fn an_offer_without_a_terminal_breaks_the_drain_check() {
    let result = ResultFixture {
        acknowledged: 990,
        unknown: 10,
        ..ResultFixture::default()
    }
    .build();

    let verdict = evaluate_slo(&result, &objectives());

    assert!(!verdict.satisfied);
    assert!(
        verdict
            .reasons
            .iter()
            .any(|reason| reason.contains("no terminal")),
        "{:?}",
        verdict.reasons
    );
}

#[test]
fn an_undrained_queue_breaks_the_drain_check() {
    let result = ResultFixture {
        final_outstanding: 7,
        ..ResultFixture::default()
    }
    .build();

    let verdict = evaluate_slo(&result, &objectives());

    assert!(!verdict.satisfied);
    assert!(
        verdict
            .reasons
            .iter()
            .any(|reason| reason.contains("still outstanding")),
        "{:?}",
        verdict.reasons
    );
}

#[test]
fn an_adapter_that_disqualifies_itself_is_not_satisfied() {
    let result = ResultFixture {
        valid: false,
        ..ResultFixture::default()
    }
    .build();

    let verdict = evaluate_slo(&result, &objectives());

    assert!(!verdict.satisfied);
    assert!(
        verdict.reasons[0].contains("declared the measurement invalid"),
        "{}",
        verdict.reasons[0]
    );
}

#[test]
fn an_empty_objective_set_still_checks_the_structural_conditions() {
    let clean = ResultFixture::default().build();
    let dirty = ResultFixture {
        acknowledged: 999,
        failed: 1,
        ..ResultFixture::default()
    }
    .build();

    assert!(evaluate_slo(&clean, &SloSpec::default()).satisfied);
    assert!(!evaluate_slo(&dirty, &SloSpec::default()).satisfied);
}

#[test]
fn a_schedule_delay_objective_is_judged_against_scheduler_lateness() {
    let inside = ResultFixture {
        lateness_ns: Some(20_000_000),
        ..ResultFixture::default()
    }
    .build();
    let outside = ResultFixture {
        lateness_ns: Some(80_000_000),
        ..ResultFixture::default()
    }
    .build();
    let slo = SloSpec {
        schedule_delay_p99_ms: Some(50),
        ..SloSpec::default()
    };

    assert!(evaluate_slo(&inside, &slo).satisfied);
    let verdict = evaluate_slo(&outside, &slo);
    assert!(!verdict.satisfied);
    assert!(
        verdict.reasons[0].contains("p99 scheduler lateness"),
        "{}",
        verdict.reasons[0]
    );
}

#[test]
fn a_schedule_delay_objective_without_a_schedule_is_a_reason_not_a_pass() {
    let closed_loop = ResultFixture::default().build();
    let slo = SloSpec {
        schedule_delay_p99_ms: Some(50),
        ..SloSpec::default()
    };

    let verdict = evaluate_slo(&closed_loop, &slo);

    assert!(!verdict.satisfied);
    assert!(
        verdict.reasons[0].contains("no scheduler lateness"),
        "{}",
        verdict.reasons[0]
    );
}

#[test]
fn a_measurement_with_no_terminals_cannot_demonstrate_a_latency_objective() {
    let result = ResultFixture {
        acknowledged: 0,
        unknown: 0,
        ..ResultFixture::default()
    }
    .build();

    let verdict = evaluate_slo(&result, &objectives());

    assert!(!verdict.satisfied);
    assert!(
        verdict.reasons[0].contains("no recorded values"),
        "{}",
        verdict.reasons[0]
    );
}

#[test]
fn every_broken_objective_is_reported_not_just_the_first() {
    let result = ResultFixture {
        acknowledged: 900,
        failed: 50,
        unknown: 50,
        final_outstanding: 3,
        terminal_ns: 900_000_000,
        valid: false,
        ..ResultFixture::default()
    }
    .build();

    let verdict = evaluate_slo(&result, &objectives());

    assert!(!verdict.satisfied);
    assert_eq!(verdict.reasons.len(), 5);
}
