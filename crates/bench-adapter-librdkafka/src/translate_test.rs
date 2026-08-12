//! Golden argument vectors for both load modes, and the subject-selection rule.
//!
//! These vectors are the entire interface to the reference client, so they are
//! written out here in full rather than assembled by the test. A diff in this
//! file is a diff in what the C program was told to measure.
#![expect(clippy::unwrap_used, reason = "test fixtures are exact")]

use std::path::{Path, PathBuf};

use bench_schema::LoadMode;

use crate::fixture::{OUTPUT, experiment};
use crate::translate::{arguments, subject_of};

fn output() -> PathBuf {
    PathBuf::from(OUTPUT)
}

#[test]
fn the_closed_loop_vector_leads_with_the_v2_output_directory() {
    let argv = arguments(&experiment(LoadMode::ClosedLoop), "librdkafka-c", &output()).unwrap();

    assert_eq!(
        argv,
        vec![
            "--v2-output",
            "/tmp/bundle/adapters/librdkafka-c",
            "127.0.0.1:39092,127.0.0.1:39093",
            "kfb-0123456789abcdef-librdkafka-c-warmup",
            "kfb-0123456789abcdef-librdkafka-c",
            "0123456789abcdef",
            "1000",
            "10000",
            "1024",
            "12",
            "8192",
        ]
    );
}

#[test]
fn the_fixed_rate_vector_puts_the_flag_after_the_directory_and_adds_rate_and_callers() {
    let argv = arguments(
        &experiment(LoadMode::ScheduledOpenLoopFixedRate),
        "librdkafka-c",
        &output(),
    )
    .unwrap();

    assert_eq!(
        argv,
        vec![
            "--v2-output",
            "/tmp/bundle/adapters/librdkafka-c",
            "--fixed-rate",
            "127.0.0.1:39092,127.0.0.1:39093",
            "kfb-0123456789abcdef-librdkafka-c-warmup",
            "kfb-0123456789abcdef-librdkafka-c",
            "0123456789abcdef",
            "1000",
            "10000",
            "1024",
            "12",
            "8192",
            "100000",
            "4",
        ]
    );
}

#[test]
fn the_vector_lengths_match_what_the_c_parser_demands() {
    let closed = arguments(&experiment(LoadMode::ClosedLoop), "librdkafka-c", &output()).unwrap();
    let fixed = arguments(
        &experiment(LoadMode::ScheduledOpenLoopFixedRate),
        "librdkafka-c",
        &output(),
    )
    .unwrap();

    // `bench_parse_v2_config` refuses anything but `argc == 12` for the
    // closed-loop shape and `argc == 15` for the fixed-rate one, which must
    // carry `--fixed-rate` at argv[3]; argv[0] is the program, which this
    // vector deliberately excludes.
    assert_eq!(closed.len(), 11);
    assert_eq!(fixed.len(), 14);
    assert_eq!(fixed[2], "--fixed-rate");
}

#[test]
fn no_vector_names_a_v1_evidence_file() {
    for load_mode in [LoadMode::ClosedLoop, LoadMode::ScheduledOpenLoopFixedRate] {
        let argv = arguments(&experiment(load_mode), "librdkafka-c", &output()).unwrap();

        // The C program refuses a vector that mixes the contracts, and it
        // derives both of its own output paths from the directory, so naming
        // one here would be both a refusal and a second opinion about where
        // the evidence lives.
        assert!(
            !argv.iter().any(|argument| argument.contains("latency.csv")),
            "{argv:?}"
        );
        assert!(
            !argv
                .iter()
                .any(|argument| argument.contains("client-metrics.jsonl")),
            "{argv:?}"
        );
    }
}

#[test]
fn an_output_directory_whose_derived_paths_would_not_fit_is_refused() {
    let deep = PathBuf::from(format!("/tmp/{}", "d".repeat(4_090)));

    let error = arguments(&experiment(LoadMode::ClosedLoop), "librdkafka-c", &deep).unwrap_err();

    assert!(error.contains("derived output path"), "{error}");
}

#[test]
fn an_experiment_without_a_runtime_binding_cannot_be_translated() {
    let mut document = experiment(LoadMode::ClosedLoop);
    document.runtime = None;

    let error = arguments(&document, "librdkafka-c", &output()).unwrap_err();

    assert!(error.contains("runtime binding"), "{error}");
}

#[test]
fn a_subject_with_no_topics_cannot_be_translated() {
    let error = arguments(&experiment(LoadMode::ClosedLoop), "sarama", &output()).unwrap_err();

    assert!(error.contains("sarama"), "{error}");
}

#[test]
fn a_path_the_c_parser_would_refuse_is_caught_here() {
    let error = arguments(
        &experiment(LoadMode::ClosedLoop),
        "librdkafka-c",
        Path::new("/tmp/quote\"directory"),
    )
    .unwrap_err();

    assert!(error.contains("quotes"), "{error}");
}

#[test]
fn the_subject_is_the_name_of_the_output_directory() {
    let document = experiment(LoadMode::ClosedLoop);

    let subject = subject_of(&document, Some(Path::new("/tmp/bundle/adapters/kafkars"))).unwrap();

    assert_eq!(subject.name, "kafkars");
}

#[test]
fn without_an_output_directory_the_subject_is_the_only_one_this_adapter_could_be() {
    let document = experiment(LoadMode::ClosedLoop);

    let subject = subject_of(&document, None).unwrap();

    assert_eq!(subject.name, "librdkafka-c");
    assert_eq!(subject.adapter_name, "librdkafka-c");
}

#[test]
fn an_unrecognised_output_directory_falls_back_to_the_adapter_name() {
    let document = experiment(LoadMode::ClosedLoop);

    let subject = subject_of(&document, Some(Path::new("/tmp/somewhere-else"))).unwrap();

    assert_eq!(subject.name, "librdkafka-c");
}

#[test]
fn two_subjects_of_this_adapter_without_an_output_directory_are_ambiguous() {
    let mut document = experiment(LoadMode::ClosedLoop);
    let mut twin = document.subjects[1].clone();
    twin.name = "librdkafka-c-baseline".to_owned();
    document.subjects.push(twin);

    let error = subject_of(&document, None).unwrap_err();

    assert!(error.contains("more than one"), "{error}");
}
