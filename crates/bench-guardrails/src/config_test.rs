//! The policy document is a reviewed artifact, so parsing it is strict.
#![expect(
    clippy::unwrap_used,
    reason = "a fixture policy that will not parse has nothing left to assert"
)]

use crate::config::{PolicyError, parse_policy};
use crate::files::Category;
use crate::fixture::POLICY;

#[test]
fn the_committed_policy_shape_parses() {
    let policy = parse_policy(POLICY).unwrap();

    assert_eq!(policy.schema, 1);
    assert_eq!(policy.paths.rust_roots, vec!["crates".to_owned()]);
    assert_eq!(policy.budgets.facade.target, 80);
    assert_eq!(policy.budgets.facade.soft, 120);
    assert!(policy.budgets.baseline.is_empty());
    assert_eq!(
        policy.forbidden_transitive_dependencies.crates,
        vec!["tokio".to_owned()]
    );
}

#[test]
fn the_live_policy_file_parses_and_covers_the_live_roots() {
    let policy = crate::load_policy(&crate::repository_root()).unwrap();

    assert!(
        policy
            .paths
            .rust_roots
            .contains(&"adapters/kafkars/src".to_owned()),
        "the adapter's source must be governed even though its lock file is not"
    );
    assert!(
        policy
            .forbidden_transitive_dependencies
            .crates
            .contains(&"tokio".to_owned())
    );
}

#[test]
fn every_category_resolves_to_a_budget() {
    let budgets = parse_policy(POLICY).unwrap().budgets;

    assert_eq!(budgets.for_category(Category::Facade).soft, 120);
    assert_eq!(budgets.for_category(Category::Implementation).soft, 300);
    assert_eq!(budgets.for_category(Category::Test).soft, 500);
    assert_eq!(budgets.for_category(Category::Auxiliary).soft, 500);
}

#[test]
fn an_unrecognised_key_is_refused_rather_than_ignored() {
    let source = format!("{POLICY}\n[budgets.documentation]\ntarget = 10\nsoft = 20\n");

    assert!(
        matches!(parse_policy(&source), Err(PolicyError::Malformed(_))),
        "a rule nobody implemented must not read as a rule that passes"
    );
}

#[test]
fn a_future_schema_is_refused_by_this_reader() {
    let source = POLICY.replace("schema = 1", "schema = 2");

    assert!(matches!(
        parse_policy(&source),
        Err(PolicyError::UnsupportedSchema(2))
    ));
}

#[test]
fn a_missing_policy_file_is_an_error_not_a_pass() {
    let error = crate::load_policy(std::path::Path::new("/nonexistent-guardrails-root"));

    assert!(matches!(error, Err(PolicyError::Unreadable(_))));
}
