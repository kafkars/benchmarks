//! Attempt id shape and uniqueness, and the pending → finalized workspace
//! lifecycle including the already-claimed refusals.
#![expect(clippy::unwrap_used, reason = "test assertions may unwrap")]

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::SystemTime;

use crate::attempt::{AttemptId, AttemptPaths, PENDING_DIR};
use crate::error::CtlErrorKind;

fn scratch_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "benchctl-attempt-test-{}-{label}-{}",
        std::process::id(),
        AttemptId::generate(SystemTime::now()).as_str(),
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn generated_ids_have_the_documented_shape_and_reparse() {
    let id = AttemptId::generate(SystemTime::now());
    let text = id.as_str();
    assert_eq!(text.len(), 25, "unexpected length in {text:?}");
    assert_eq!(&text[16..17], "-");
    assert!(text.ends_with(|c: char| c.is_ascii_hexdigit()));
    let reparsed = AttemptId::parse(text).unwrap();
    assert_eq!(reparsed, id);
    assert_eq!(id.to_string(), text);
}

#[test]
fn generated_ids_are_unique_within_one_instant() {
    let now = SystemTime::now();
    let ids: BTreeSet<String> = (0..100)
        .map(|_| AttemptId::generate(now).as_str().to_owned())
        .collect();
    assert_eq!(ids.len(), 100, "collision within a single instant");
}

#[test]
fn malformed_ids_are_rejected() {
    for bad in [
        "",
        "20260812T140305Z",
        "20260812T140305Z-",
        "20260812T140305Z-ABCD1234",
        "20260812T140305Z-12345678extra",
        "2026-08-12T14:03:05Z-12345678",
    ] {
        let error = AttemptId::parse(bad).unwrap_err();
        assert_eq!(
            error.kind(),
            CtlErrorKind::InvalidExperiment,
            "accepted {bad:?}"
        );
    }
}

#[test]
fn pending_workspace_is_created_with_subdirectories() {
    let results_root = scratch_dir("pending");
    let id = AttemptId::generate(SystemTime::now());
    let paths = AttemptPaths::create_pending(&results_root, &id).unwrap();
    assert!(paths.root().starts_with(results_root.join(PENDING_DIR)));
    assert!(paths.adapters_dir().is_dir());
    assert!(paths.verification_dir().is_dir());
    assert_eq!(paths.status_json(), paths.root().join("status.json"));
    assert_eq!(
        paths.verification_json("kafkars", "measured"),
        paths
            .root()
            .join("verification")
            .join("kafkars-measured.json")
    );
    std::fs::remove_dir_all(&results_root).unwrap();
}

#[test]
fn claiming_an_existing_pending_directory_fails() {
    let results_root = scratch_dir("claimed");
    let id = AttemptId::generate(SystemTime::now());
    AttemptPaths::create_pending(&results_root, &id).unwrap();
    let error = AttemptPaths::create_pending(&results_root, &id).unwrap_err();
    assert_eq!(error.kind(), CtlErrorKind::AttemptExists);
    std::fs::remove_dir_all(&results_root).unwrap();
}

#[test]
fn finalize_moves_the_workspace_under_the_experiment_id() {
    let results_root = scratch_dir("finalize");
    let id = AttemptId::generate(SystemTime::now());
    let paths = AttemptPaths::create_pending(&results_root, &id).unwrap();
    std::fs::write(paths.status_json(), b"{}\n").unwrap();
    let finalized = paths.finalize(&results_root, "d2932f88ad348028").unwrap();
    assert_eq!(
        finalized.root(),
        results_root.join("d2932f88ad348028").join(id.as_str())
    );
    assert!(finalized.status_json().is_file());
    assert!(!results_root.join(PENDING_DIR).join(id.as_str()).exists());
    std::fs::remove_dir_all(&results_root).unwrap();
}

#[test]
fn finalize_refuses_an_already_claimed_target() {
    let results_root = scratch_dir("finalize-claimed");
    let id = AttemptId::generate(SystemTime::now());
    let paths = AttemptPaths::create_pending(&results_root, &id).unwrap();
    let occupied = results_root.join("d2932f88ad348028").join(id.as_str());
    std::fs::create_dir_all(&occupied).unwrap();
    let error = paths
        .clone()
        .finalize(&results_root, "d2932f88ad348028")
        .unwrap_err();
    assert_eq!(error.kind(), CtlErrorKind::AttemptExists);
    assert!(
        paths.root().is_dir(),
        "workspace must survive a refused finalize"
    );
    std::fs::remove_dir_all(&results_root).unwrap();
}
