//! The rate override: what it rewrites, what it leaves alone, and what it
//! refuses.
//!
//! The ladder is only as trustworthy as this rewrite. Every probe seals the
//! patched scenario as its own `experiment.source.toml` and derives its
//! experiment id from the resolved document, so a patch that silently did
//! nothing would produce a whole search whose bundles all agree with each other
//! about a rate nobody offered.
#![expect(
    clippy::unwrap_used,
    reason = "a scenario fixture that cannot be parsed must fail the test immediately"
)]

use bench_schema::{ClusterProfile, SourceExperiment, SubjectsFile};

use super::rewrite::{probe_inputs, with_offered_rate};
use crate::pipeline::LoadedInputs;

/// A fixed-rate scenario with the offered rate written the ordinary way.
const FIXED_RATE_SCENARIO: &str = "\
name = \"offline-capacity\"
status = \"diagnostic\"
claim_eligible = false
load_mode = \"scheduled-open-loop-fixed-rate\"
records = 2000
offered_records_per_second = 20000

[application]
producer_instances = 1
callers_per_producer = 4
backpressure = \"block-within-original-offer\"
queue_bytes = 67108864
max_outstanding_records = 8192

[application_api]
admission_shape = \"public-batch\"
completion_shape = \"aggregate-batch-terminal\"
batch_records = 256

[payload]
bytes = 1024
profile = \"deterministic-ascii-envelope\"
seed = 44

[producer]
acks = \"all\"
idempotence = true
compression = \"none\"
linger_ms = 5
batch_records = 256
batch_bytes = 65536
request_bytes = 1048576
delivery_timeout_ms = 60000
partitioning = \"explicit-round-robin\"
retry_max_replacements = 600
retry_backoff_ms = 100

[cluster]
brokers = 3
partitions = 12
replication_factor = 3
min_in_sync_replicas = 2
security = \"plaintext\"
";

/// The loaded inputs a probe would be built from, over `source_toml`.
fn loaded(source_toml: &str) -> LoadedInputs {
    LoadedInputs {
        source: SourceExperiment::from_toml_str(source_toml).unwrap(),
        source_toml: source_toml.to_owned(),
        subjects: SubjectsFile::from_toml_str(
            "[[subjects]]\nname = \"kafkars\"\ncommand = [\"/bin/true\"]\n",
        )
        .unwrap(),
        cluster: ClusterProfile::from_toml_str(
            "name = \"offline\"\nbootstrap = \"127.0.0.1:9092\"\n",
        )
        .unwrap(),
    }
}

#[test]
fn the_rewrite_replaces_the_top_level_rate_and_says_it_did() {
    let patched = with_offered_rate(FIXED_RATE_SCENARIO, 31_337);

    assert!(patched.contains("offered_records_per_second = 31337"));
    assert!(
        !patched.contains("offered_records_per_second = 20000"),
        "the original assignment must be gone:\n{patched}"
    );
    assert!(
        patched.contains("# rewritten by benchctl capacity for this probe"),
        "the patched scenario is sealed, so it says who patched it"
    );
    assert!(patched.ends_with('\n'));
    let reparsed = SourceExperiment::from_toml_str(&patched).unwrap();
    assert_eq!(reparsed.offered_records_per_second, Some(31_337));
    assert_eq!(reparsed.records, Some(2_000), "nothing else moved");
    assert_eq!(reparsed.cluster.partitions, 12);
}

#[test]
fn a_same_named_key_inside_a_table_is_none_of_the_rewrites_business() {
    // The rewriter stops at the first table header. A key of this name below one
    // belongs to that table and means something else entirely.
    let scenario = FIXED_RATE_SCENARIO.replace(
        "[cluster]\nbrokers = 3",
        "[cluster]\n# offered_records_per_second = 999\nbrokers = 3",
    );

    let patched = with_offered_rate(&scenario, 4_096);

    assert!(patched.contains("# offered_records_per_second = 999"));
    assert_eq!(
        SourceExperiment::from_toml_str(&patched)
            .unwrap()
            .offered_records_per_second,
        Some(4_096)
    );
}

#[test]
fn a_probe_at_a_rewritten_rate_carries_that_rate() {
    let inputs = probe_inputs(&loaded(FIXED_RATE_SCENARIO), 12_345).unwrap();

    assert_eq!(inputs.source.offered_records_per_second, Some(12_345));
    assert!(
        inputs
            .source_toml
            .contains("offered_records_per_second = 12345")
    );
}

#[test]
fn a_rate_the_rewrite_could_not_set_is_refused_rather_than_probed() {
    // A multi-line value whose interior contains a bracketed line looks, to a
    // line-oriented walk, exactly like the first table header. The head region
    // ends there, so the real assignment is never removed, and the new one is
    // inserted above it — inside the string, where it means nothing. The
    // patched text still parses and still carries the original rate, which is
    // the shape that has to be caught: every probe would offer 20000 while the
    // sealed documents claimed the ladder was moving.
    let corrupting = FIXED_RATE_SCENARIO.replace(
        "name = \"offline-capacity\"",
        "name = \"\"\"\noffline-capacity\n[not a table header]\n\"\"\"",
    );
    assert_eq!(
        SourceExperiment::from_toml_str(&corrupting)
            .unwrap()
            .offered_records_per_second,
        Some(20_000),
        "the fixture itself is a legal scenario"
    );
    let patched = with_offered_rate(&corrupting, 12_345);
    assert_eq!(
        SourceExperiment::from_toml_str(&patched)
            .unwrap()
            .offered_records_per_second,
        Some(20_000),
        "the rewrite silently did nothing, which is why the guard exists"
    );

    let error = probe_inputs(&loaded(&corrupting), 12_345).unwrap_err();

    assert_eq!(error.kind(), crate::error::CtlErrorKind::InvalidExperiment);
    assert!(
        error.message().contains("could not be rewritten to 12345"),
        "{error}"
    );
    assert!(
        error.message().contains("parses back as Some(20000)"),
        "the refusal names what it got instead: {error}"
    );
}
