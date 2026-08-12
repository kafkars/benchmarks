//! The argv parse table, exercised through the same entry point `main` uses.
//!
//! The verbs that only print a document are driven end to end here; `run` is
//! covered in `run_test.rs`, where a shell fixture stands in for the C binary.
use crate::arguments::{EXIT_OK, EXIT_USAGE, run};

fn argv(line: &str) -> Vec<String> {
    std::iter::once("bench-adapter-librdkafka".to_owned())
        .chain(line.split_whitespace().map(str::to_owned))
        .collect()
}

#[test]
fn describe_answers_with_or_without_the_json_flag() {
    assert_eq!(run(&argv("--binary /bin/echo describe --json")), EXIT_OK);
    assert_eq!(run(&argv("--binary /bin/echo describe")), EXIT_OK);
}

#[test]
fn describe_does_not_need_the_binary_to_exist() {
    // `describe` is how a control plane finds out whether a subject is usable,
    // so it must answer even when the thing it would drive is missing.
    assert_eq!(
        run(&argv("--binary /nonexistent/librdkafka describe --json")),
        EXIT_OK
    );
}

#[test]
fn help_is_a_flag_of_its_own() {
    assert_eq!(run(&argv("--help")), EXIT_OK);
    assert_eq!(run(&argv("-h")), EXIT_OK);
}

#[test]
fn the_binary_prefix_is_required_and_comes_first() {
    assert_eq!(run(&argv("describe --json")), EXIT_USAGE);
    assert_eq!(run(&argv("--binary")), EXIT_USAGE);
    assert_eq!(run(&argv("--binary=")), EXIT_USAGE);
    assert_eq!(run(&argv("describe --binary /bin/echo")), EXIT_USAGE);
}

#[test]
fn the_binary_may_be_given_with_an_equals_sign() {
    assert_eq!(run(&argv("--binary=/bin/echo describe --json")), EXIT_OK);
}

#[test]
fn an_unknown_verb_is_a_usage_error() {
    assert_eq!(run(&argv("--binary /bin/echo measure")), EXIT_USAGE);
    assert_eq!(run(&argv("--binary /bin/echo")), EXIT_USAGE);
}

#[test]
fn a_verb_missing_its_flags_is_a_usage_error() {
    assert_eq!(run(&argv("--binary /bin/echo validate")), EXIT_USAGE);
    assert_eq!(
        run(&argv("--binary /bin/echo validate --experiment")),
        EXIT_USAGE
    );
    assert_eq!(
        run(&argv("--binary /bin/echo run --experiment /tmp/x.json")),
        EXIT_USAGE
    );
}

#[test]
fn an_unknown_flag_is_a_usage_error() {
    assert_eq!(
        run(&argv(
            "--binary /bin/echo validate --experiment /tmp/x.json --strict yes"
        )),
        EXIT_USAGE
    );
    assert_eq!(
        run(&argv("--binary /bin/echo describe --pretty")),
        EXIT_USAGE
    );
}

#[test]
fn a_repeated_flag_is_a_usage_error() {
    assert_eq!(
        run(&argv(
            "--binary /bin/echo validate --experiment /tmp/a.json --experiment /tmp/b.json"
        )),
        EXIT_USAGE
    );
}
