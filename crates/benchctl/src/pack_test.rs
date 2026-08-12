//! Manifest parsing and the entry dispatch decision.
//!
//! Both are pure, and both are the parts of a pack that decide what gets run
//! before anything is run. A dispatch rule that quietly turned a search entry
//! into a single attempt would produce evidence that looks like a pack ran and
//! is missing the one entry that takes an hour, so the table below covers every
//! branch rather than the interesting one.
//!
//! The committed packs are parsed from disk rather than from a copy, so a pack
//! file that drifts out of the manifest shape is a test failure here rather
//! than a surprise on the nightly runner.
#![expect(clippy::unwrap_used, reason = "test assertions may unwrap")]

use crate::error::CtlErrorKind;
use crate::pack::{EntryOutcome, EntryPlan, PackManifest, render_table};

/// A manifest with `entries` spliced into the header every pack shares.
fn manifest_text(entries: &str) -> String {
    format!(
        "name = \"fixture\"\n\
         cadence = \"nightly\"\n\
         description = \"A manifest written by a test.\"\n\n{entries}"
    )
}

/// One entry, written the way the committed packs write one.
fn entry(scenario: &str, repetitions: u32) -> String {
    format!("[[entries]]\nscenario = \"{scenario}\"\nrepetitions = {repetitions}\n\n")
}

#[test]
fn a_manifest_parses_into_its_header_and_its_entries_in_order() {
    let text = manifest_text(&format!(
        "{}{}",
        entry("scenarios/producer/headline/latency-floor-128b-1p.toml", 3),
        entry(
            "scenarios/producer/headline/capacity-balanced-1k-12p.toml",
            1
        ),
    ));
    let manifest = PackManifest::from_toml_str(&text).unwrap();
    assert_eq!(manifest.name, "fixture");
    assert_eq!(manifest.cadence, "nightly");
    assert_eq!(manifest.entries.len(), 2);
    assert_eq!(
        manifest.entries[0].scenario,
        "scenarios/producer/headline/latency-floor-128b-1p.toml"
    );
    assert_eq!(manifest.entries[0].repetitions, 3);
    assert_eq!(manifest.entries[1].repetitions, 1);
}

#[test]
fn an_unknown_key_is_refused_rather_than_ignored() {
    // A pack is a declaration of intent, and a mistyped key in one is a
    // scenario somebody believes is in the cadence and that nothing runs.
    let text = manifest_text("[[entries]]\nscenario = \"a.toml\"\nrepititions = 3\n");
    let error = PackManifest::from_toml_str(&text).unwrap_err();
    assert_eq!(error.kind(), CtlErrorKind::InvalidExperiment);
}

#[test]
fn a_manifest_without_entries_is_refused() {
    let error = PackManifest::from_toml_str(&manifest_text("")).unwrap_err();
    assert_eq!(error.kind(), CtlErrorKind::InvalidExperiment);
    assert!(error.message().contains("runs nothing"), "{error}");
}

#[test]
fn an_entry_asking_for_zero_repetitions_is_refused() {
    let text = manifest_text(&entry(
        "scenarios/producer/headline/balanced-1k-12p.toml",
        0,
    ));
    let error = PackManifest::from_toml_str(&text).unwrap_err();
    assert_eq!(error.kind(), CtlErrorKind::InvalidExperiment);
    assert!(error.message().contains("zero repetitions"), "{error}");
}

#[test]
fn an_entry_naming_no_scenario_is_refused() {
    let error = PackManifest::from_toml_str(&manifest_text(&entry("", 1))).unwrap_err();
    assert_eq!(error.kind(), CtlErrorKind::InvalidExperiment);
    assert!(error.message().contains("names nothing"), "{error}");
}

#[test]
fn text_that_is_not_a_manifest_is_refused_as_invalid_input() {
    let error = PackManifest::from_toml_str("this is not toml = = =").unwrap_err();
    assert_eq!(error.kind(), CtlErrorKind::InvalidExperiment);
}

#[test]
fn two_or_more_repetitions_is_a_suite_whatever_the_scenario_carries() {
    // The search section loses to the repetition count on purpose: two attempts
    // of one experiment is a suite, and a ladder is not two attempts of one
    // experiment.
    assert_eq!(EntryPlan::decide(2, false), EntryPlan::Suite);
    assert_eq!(EntryPlan::decide(2, true), EntryPlan::Suite);
    assert_eq!(EntryPlan::decide(3, true), EntryPlan::Suite);
    assert_eq!(EntryPlan::decide(64, false), EntryPlan::Suite);
}

#[test]
fn one_repetition_of_a_search_scenario_is_a_capacity_ladder() {
    assert_eq!(EntryPlan::decide(1, true), EntryPlan::Capacity);
}

#[test]
fn one_repetition_of_anything_else_is_a_single_run() {
    assert_eq!(EntryPlan::decide(1, false), EntryPlan::Run);
}

#[test]
fn every_plan_prints_the_verb_it_dispatches_to() {
    assert_eq!(EntryPlan::Run.as_str(), "run");
    assert_eq!(EntryPlan::Suite.as_str(), "suite");
    assert_eq!(EntryPlan::Capacity.as_str(), "capacity");
}

#[test]
fn the_committed_packs_parse_and_dispatch_the_way_their_headers_claim() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let pull_request = PackManifest::from_toml_str(
        &std::fs::read_to_string(root.join("scenarios/packs/pr.toml")).unwrap(),
    )
    .unwrap();
    assert_eq!(pull_request.name, "pr");
    assert_eq!(pull_request.entries.len(), 1, "the PR pack is one scenario");
    assert_eq!(pull_request.entries[0].repetitions, 1);

    let nightly = PackManifest::from_toml_str(
        &std::fs::read_to_string(root.join("scenarios/packs/nightly.toml")).unwrap(),
    )
    .unwrap();
    assert_eq!(nightly.name, "nightly");
    // Five headline scenarios at three repetitions, then one capacity search:
    // the header says so, and a pack that stopped matching its own header would
    // be a cadence nobody reviewed.
    let searches = nightly
        .entries
        .iter()
        .filter(|entry| entry.repetitions == 1)
        .count();
    assert_eq!(searches, 1, "{:?}", nightly.entries);
    assert!(
        nightly
            .entries
            .iter()
            .filter(|entry| entry.repetitions > 1)
            .all(|entry| entry.repetitions == 3),
        "{:?}",
        nightly.entries
    );
    assert_eq!(
        nightly.entries.last().unwrap().scenario,
        "scenarios/producer/headline/capacity-balanced-1k-12p.toml",
        "the search goes last so a pack that runs out of time still produced \
         every fixed point"
    );
    for entry in &nightly.entries {
        assert!(
            root.join(&entry.scenario).is_file(),
            "{} names a scenario that is not on disk",
            entry.scenario
        );
    }
}

#[test]
fn the_table_names_every_entry_and_counts_the_ones_that_exited_zero() {
    let manifest = PackManifest::from_toml_str(&manifest_text(&format!(
        "{}{}",
        entry("a.toml", 3),
        entry("b.toml", 1)
    )))
    .unwrap();
    let table = render_table(
        &manifest,
        &[
            EntryOutcome {
                scenario: "a.toml".to_owned(),
                repetitions: 3,
                plan: Some(EntryPlan::Suite),
                exit_code: 0,
                evidence: None,
            },
            EntryOutcome {
                scenario: "b.toml".to_owned(),
                repetitions: 1,
                plan: None,
                exit_code: 65,
                evidence: Some("results/abcd1234/20260812T101500Z-0123abcd".to_owned()),
            },
        ],
    );
    assert!(
        table.contains("| 1 | `a.toml` | suite | 3 | 0 | — |"),
        "{table}"
    );
    // An entry that could never be planned has no verb to name, and printing a
    // dash is more honest than guessing which one it would have been.
    assert!(
        table.contains(
            "| 2 | `b.toml` | — | 1 | 65 | `results/abcd1234/20260812T101500Z-0123abcd` |"
        ),
        "{table}"
    );
    assert!(table.contains("1 of 2 entries exited 0."), "{table}");
}
