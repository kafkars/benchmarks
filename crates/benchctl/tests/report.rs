//! The two reading verbs, offline: `report` renders one sealed bundle, and
//! `packet` binds prose to the verdict the numbers produced.
//!
//! Neither verb runs anything. They are checked against evidence a real suite
//! left on disk, because a reader that only ever sees hand-built fixtures is a
//! reader nobody has pointed at the bundles this repository actually writes.
#![expect(
    clippy::unwrap_used,
    reason = "a fixture that cannot be built must fail the test immediately"
)]

mod common;

use bench_schema::{AnalysisPacket, LlmSummary, SuiteSummary, Verdict};

use common::harness::{benchctl, find_bundle, run_suite, scratch};

/// A minimal LLM summary over `packet`, with `verdict` substituted.
///
/// It cites nothing, which is legal: the guardrail bounds what prose *may*
/// claim, not how much it must. That makes it the sharpest test of the one rule
/// that has no escape hatch — the verdict is copied, never concluded.
fn llm_summary(verdict: Verdict) -> Vec<u8> {
    bench_schema::pretty_bytes(&LlmSummary {
        schema: LlmSummary::SCHEMA.to_owned(),
        verdict,
        executive_summary: "Written by a test to exercise the guardrail.".to_owned(),
        findings: Vec::new(),
        hypotheses: Vec::new(),
        next_experiments: Vec::new(),
        caveats: vec!["This summary interprets nothing.".to_owned()],
    })
    .unwrap()
}

/// The verdict that is not `verdict`, so a rejection is always available.
fn other_verdict(verdict: Verdict) -> Verdict {
    if verdict == Verdict::Invalid {
        Verdict::Improved
    } else {
        Verdict::Invalid
    }
}

#[test]
fn packet_binds_prose_to_the_verdict_the_numbers_produced() {
    let dir = scratch("packet");
    let (_code, reports) = run_suite(&dir, "packet");
    let summary_path = reports.join("suite-summary.json");
    let packet =
        AnalysisPacket::from_slice(&std::fs::read(reports.join("analysis-packet.json")).unwrap())
            .unwrap();

    let agreeing = dir.join("agreeing.json");
    std::fs::write(&agreeing, llm_summary(packet.verdict)).unwrap();
    let (code, stdout) = benchctl(&[
        "packet".to_owned(),
        "--suite".to_owned(),
        summary_path.to_str().unwrap().to_owned(),
        "--llm-summary".to_owned(),
        agreeing.to_str().unwrap().to_owned(),
    ]);
    assert_eq!(code, 0, "a summary that copies the verdict is accepted");
    assert!(stdout.contains("bound to the packet"), "{stdout}");

    let overruling = dir.join("overruling.json");
    std::fs::write(&overruling, llm_summary(other_verdict(packet.verdict))).unwrap();
    let (code, _stdout) = benchctl(&[
        "packet".to_owned(),
        "--suite".to_owned(),
        summary_path.to_str().unwrap().to_owned(),
        "--llm-summary".to_owned(),
        overruling.to_str().unwrap().to_owned(),
    ]);
    assert_eq!(
        code, 65,
        "prose may not overrule the deterministic verdict, and saying so is the whole point"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn report_renders_a_sealed_bundle_as_markdown() {
    let dir = scratch("report-verb");
    let (_code, reports) = run_suite(&dir, "render");
    let summary =
        SuiteSummary::from_slice(&std::fs::read(reports.join("suite-summary.json")).unwrap())
            .unwrap();
    let attempt = &summary.attempts[0];
    let bundle = find_bundle(&dir.join("results-render"), &attempt.attempt_id);
    let (code, stdout) = benchctl(&[
        "report".to_owned(),
        "--bundle".to_owned(),
        bundle.to_str().unwrap().to_owned(),
    ]);
    assert_eq!(code, 0);
    assert!(stdout.contains("kafkars"), "{stdout}");

    // `--out` into a directory that does not exist yet. `reports/` is generated
    // output and is not checked in, so on a fresh clone this is the *first*
    // shape a reader tries, and it must not need a `mkdir -p` first.
    let out = dir.join("fresh").join("nested").join("report.md");
    assert!(!out.parent().unwrap().exists());
    let (code, _stdout) = benchctl(&[
        "report".to_owned(),
        "--bundle".to_owned(),
        bundle.to_str().unwrap().to_owned(),
        "--out".to_owned(),
        out.to_str().unwrap().to_owned(),
    ]);
    assert_eq!(code, 0, "a nested --out path creates its own directory");
    let written = std::fs::read_to_string(&out).unwrap();
    assert_eq!(written, stdout, "the file and stdout render identically");

    // A bare filename has no parent to create, and must still work.
    let (code, _stdout) = benchctl(&[
        "report".to_owned(),
        "--bundle".to_owned(),
        bundle.to_str().unwrap().to_owned(),
        "--out".to_owned(),
        dir.join("beside.md").to_str().unwrap().to_owned(),
    ]);
    assert_eq!(code, 0);
    assert!(dir.join("beside.md").is_file());
    std::fs::remove_dir_all(&dir).unwrap();
}
