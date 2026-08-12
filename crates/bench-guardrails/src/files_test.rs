//! The category rule, stated once as a table and asserted against.

use crate::files::{Category, categorize};

/// Classify a path in a tree where no `fixture.rs` is gated behind `cfg(test)`.
fn plain(relative: &str) -> Category {
    categorize(relative, &|_| false)
}

/// Classify a path in a tree where `mod fixture;` is gated behind `cfg(test)`.
fn gated(relative: &str) -> Category {
    categorize(relative, &|stem| stem == "fixture")
}

#[test]
fn facades_are_the_two_declarative_filenames() {
    assert_eq!(plain("crates/demo/src/lib.rs"), Category::Facade);
    assert_eq!(plain("crates/demo/src/engine/mod.rs"), Category::Facade);
}

#[test]
fn main_and_ordinary_modules_are_implementation() {
    assert_eq!(plain("crates/demo/src/main.rs"), Category::Implementation);
    assert_eq!(plain("crates/demo/src/engine.rs"), Category::Implementation);
    assert_eq!(
        plain("adapters/kafkars/src/producer/v2/engine.rs"),
        Category::Implementation
    );
}

#[test]
fn unit_test_siblings_and_integration_tests_are_tests() {
    assert_eq!(plain("crates/demo/src/engine_test.rs"), Category::Test);
    assert_eq!(plain("crates/demo/tests/end_to_end.rs"), Category::Test);
    assert_eq!(
        plain("crates/demo/tests/golden/case.rs"),
        Category::Test,
        "anything beneath a tests directory is a test, however deep"
    );
}

#[test]
fn binaries_and_shared_integration_helpers_are_auxiliary() {
    assert_eq!(plain("crates/demo/src/bin/tool.rs"), Category::Auxiliary);
    assert_eq!(
        plain("crates/demo/tests/common/mod.rs"),
        Category::Auxiliary,
        "cargo mandates this filename for shared helpers, so it carries real code"
    );
    assert_eq!(
        plain("crates/demo/tests/common/builders.rs"),
        Category::Auxiliary
    );
}

#[test]
fn a_gated_fixture_module_is_auxiliary_and_an_ungated_one_is_not() {
    assert_eq!(gated("crates/demo/src/fixture.rs"), Category::Auxiliary);
    assert_eq!(
        plain("crates/demo/src/fixture.rs"),
        Category::Implementation,
        "a fixture.rs nobody declares behind cfg(test) is ordinary production code"
    );
}

#[test]
fn auxiliary_outranks_facade_and_test_outranks_implementation() {
    assert_eq!(
        plain("crates/demo/tests/common/mod.rs"),
        Category::Auxiliary,
        "the /tests/common/ clause would be unreachable if facade were tested first"
    );
    assert_eq!(
        plain("crates/demo/src/bin/mod.rs"),
        Category::Auxiliary,
        "a mod.rs under a bin directory is still a binary's business"
    );
    assert_eq!(
        plain("crates/demo/tests/support.rs"),
        Category::Test,
        "a tests-directory file is a test before it is an implementation"
    );
}

#[test]
fn a_directory_merely_named_like_a_root_does_not_match() {
    assert_eq!(
        plain("crates/binary-tools/src/run.rs"),
        Category::Implementation,
        "the bin clause matches a path component, not a prefix"
    );
    assert_eq!(
        plain("crates/demo/src/testsuite/run.rs"),
        Category::Implementation
    );
}
