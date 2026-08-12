//! The checksum walk: what it lists, in what order, and what it refuses to
//! list.
#![expect(
    clippy::unwrap_used,
    reason = "a checksum fixture that cannot be written must fail the test immediately"
)]

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use bench_schema::{parse_checksums, sha256_hex};

use crate::attempt::AttemptId;
use crate::checksum::{EXCLUDED_ROOT_FILES, checksum_bundle};

fn scratch_bundle(label: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "benchctl-checksum-test-{}-{label}-{}",
        std::process::id(),
        AttemptId::generate(SystemTime::now()).as_str(),
    ));
    std::fs::create_dir_all(&root).unwrap();
    root
}

fn write(root: &Path, relative: &str, contents: &str) {
    let path = root.join(relative);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, contents).unwrap();
}

#[test]
fn the_manifest_is_sorted_by_path_and_excludes_only_the_two_root_files() {
    let root = scratch_bundle("sorted");
    write(&root, "status.json", "status\n");
    write(&root, "adapters/kafkars/result.json", "result\n");
    write(&root, "adapters/kafkars/stdout.log", "");
    write(&root, "verification/kafkars-measured.json", "verified\n");
    write(&root, "checksums.txt", "stale\n");
    write(&root, "bundle.json", "stale\n");
    // Only the root-level names are excluded: a nested file that shares one is
    // ordinary evidence.
    write(&root, "adapters/kafkars/bundle.json", "nested\n");

    let manifest = checksum_bundle(&root).unwrap();
    let listed: Vec<String> = parse_checksums(&manifest.text)
        .unwrap()
        .iter()
        .map(|entry| entry.path().to_owned())
        .collect();
    assert_eq!(
        listed,
        vec![
            "adapters/kafkars/bundle.json".to_owned(),
            "adapters/kafkars/result.json".to_owned(),
            "adapters/kafkars/stdout.log".to_owned(),
            "status.json".to_owned(),
            "verification/kafkars-measured.json".to_owned(),
        ]
    );
    assert_eq!(manifest.file_count, 5);
    assert_eq!(
        manifest.total_bytes,
        u64::try_from("nested\nresult\nverified\nstatus\n".len()).unwrap()
    );
    assert!(manifest.text.ends_with('\n'));
    std::fs::remove_dir_all(&root).unwrap();
}

#[test]
fn every_listed_digest_is_the_sha256_of_the_file() {
    let root = scratch_bundle("digests");
    write(&root, "status.json", "status\n");
    write(&root, "adapters/fake/result.json", "{\"schema\":\"x\"}\n");
    write(&root, "empty.log", "");

    let manifest = checksum_bundle(&root).unwrap();
    for entry in parse_checksums(&manifest.text).unwrap() {
        let bytes = std::fs::read(root.join(entry.path())).unwrap();
        assert_eq!(entry.digest(), sha256_hex(&bytes), "{}", entry.path());
    }
    std::fs::remove_dir_all(&root).unwrap();
}

#[test]
fn a_large_file_digests_the_same_streamed_as_it_would_whole() {
    let root = scratch_bundle("streamed");
    let contents = "0123456789abcdef".repeat(40_000);
    write(&root, "latency.csv", &contents);

    let manifest = checksum_bundle(&root).unwrap();
    let entries = parse_checksums(&manifest.text).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].digest(), sha256_hex(contents.as_bytes()));
    assert_eq!(
        manifest.total_bytes,
        u64::try_from(contents.len()).unwrap(),
        "chunked reads must still total the whole file"
    );
    std::fs::remove_dir_all(&root).unwrap();
}

#[test]
fn an_empty_bundle_renders_an_empty_manifest() {
    let root = scratch_bundle("empty");
    let manifest = checksum_bundle(&root).unwrap();
    assert_eq!(manifest.text, "");
    assert_eq!(manifest.file_count, 0);
    assert_eq!(manifest.total_bytes, 0);
    std::fs::remove_dir_all(&root).unwrap();
}

#[test]
fn the_exclusion_list_is_exactly_the_two_documented_files() {
    assert_eq!(EXCLUDED_ROOT_FILES, ["checksums.txt", "bundle.json"]);
}

/// Creates a symbolic link, reporting whether the platform allowed it.
#[cfg(unix)]
fn symlink(target: &Path, link: &Path) -> bool {
    std::os::unix::fs::symlink(target, link).is_ok()
}

#[cfg(unix)]
#[test]
fn a_symlinked_file_fails_the_seal_rather_than_being_skipped() {
    let root = scratch_bundle("symlink-file");
    write(&root, "status.json", "status\n");
    let outside = root.join("..").join("outside.txt");
    std::fs::write(&outside, "not part of the bundle\n").unwrap();
    std::fs::create_dir_all(root.join("adapters")).unwrap();
    assert!(symlink(&outside, &root.join("adapters").join("link.json")));

    let error = checksum_bundle(&root).unwrap_err();

    assert_eq!(error.kind(), crate::error::CtlErrorKind::Seal);
    assert!(
        error.message().contains("symbolic link"),
        "the refusal names what it found: {error}"
    );
    assert!(error.message().contains("link.json"), "{error}");
    std::fs::remove_file(&outside).unwrap();
    std::fs::remove_dir_all(&root).unwrap();
}

#[cfg(unix)]
#[test]
fn a_symlinked_directory_fails_the_seal_rather_than_being_walked() {
    // A link to a directory is the more dangerous shape: descending into it
    // would list files under paths that do not exist inside the bundle, and
    // skipping it would leave a whole subtree out of a manifest that looked
    // complete.
    let root = scratch_bundle("symlink-dir");
    write(&root, "status.json", "status\n");
    let elsewhere = root.join("..").join("elsewhere");
    std::fs::create_dir_all(&elsewhere).unwrap();
    std::fs::write(elsewhere.join("secret.log"), "elsewhere\n").unwrap();
    assert!(symlink(&elsewhere, &root.join("adapters")));

    let error = checksum_bundle(&root).unwrap_err();

    assert_eq!(error.kind(), crate::error::CtlErrorKind::Seal);
    assert!(error.message().contains("symbolic link"), "{error}");
    std::fs::remove_dir_all(&elsewhere).unwrap();
    std::fs::remove_dir_all(&root).unwrap();
}
