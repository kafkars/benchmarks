//! Protected-base architecture review workflow contract.

use std::{error::Error, fs, path::PathBuf};

const WORKFLOW: &str = ".github/workflows/architecture-authority.yml";

#[test]
fn proposal_architecture_review_runs_from_trusted_base_without_grant_flags()
-> Result<(), Box<dyn Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let workflow = fs::read_to_string(root.join(WORKFLOW))?;

    assert!(workflow.contains("pull_request_target:"));
    assert!(workflow.contains("ref: ${{ env.ZRAIL_BASE_SHA }}"));
    assert!(workflow.contains("ref: ${{ env.ZRAIL_PROPOSAL_SHA }}"));
    assert!(workflow.contains("path: proposal"));
    assert!(workflow.contains("zrail review"));
    assert!(workflow.contains("--authority-root \"$GITHUB_WORKSPACE\""));
    assert!(workflow.contains("--root \"$GITHUB_WORKSPACE/proposal\""));
    assert!(!workflow.contains("--allow-grants"));
    assert!(!workflow.contains("--accept-grants"));

    Ok(())
}
