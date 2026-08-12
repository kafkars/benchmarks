//! A test fixture that speaks the whole adapter protocol, doubles as the topic
//! tool and the verifier, and can fail in every way the control plane has to
//! survive.
//!
//! The always-seal guarantee is a claim about behavior under failure, and a
//! claim about failure that is only tested against success is not tested. This
//! binary is how the integration tests produce a real non-zero exit, a real
//! `SIGABRT`, a real process that ignores its deadline, and a real verifier that
//! disagrees with its adapter — without a Kafka cluster, without a network, and
//! without the flakiness of arranging those conditions for real.
//!
//! # Verbs
//!
//! Protocol: `describe --json`, `validate --experiment <file>`,
//! `run --experiment <file> --output <dir>`.
//!
//! Configured tools, in the legacy positional shapes the control plane appends:
//! `topics-create <bootstrap> <partitions> <replication-factor> <topic...>`,
//! `topics-delete <bootstrap> <topic...>`, and
//! `verify <bootstrap> <topic> <run-id> <records> <payload-bytes> <partitions>`.
//!
//! # Which subject am I?
//!
//! The resolved experiment names topics per subject, and a subject's adapter is
//! told where to write rather than who it is. This fixture takes its subject
//! name from the last component of `--output`, which is `adapters/<subject>/` by
//! the bundle layout. It is a fixture convention, documented here so that no
//! reader mistakes it for a protocol rule.
//!
//! # Layout
//!
//! - `mode` — how this invocation should misbehave, if at all.
//! - `verbs` — one function per verb the control plane can ask for.
//! - `document` — the `kafkars.producer-benchmark.v2` the `run` verb writes.
//! - `io` — reading the experiment, writing documents, and reading flags.
#![forbid(unsafe_code)]

mod document;
mod io;
mod mode;
mod verbs;

use self::mode::Mode;

/// Exit code for a usage error, matching the control plane's table.
const EXIT_USAGE: i32 = 64;

fn main() {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let (mode, rest) = take_mode(&arguments);
    let code = if let Some((verb, tail)) = rest.split_first() {
        dispatch(mode, verb, tail)
    } else {
        eprintln!("usage: fake-adapter [--mode <mode>] <verb> [arguments...]");
        EXIT_USAGE
    };
    std::process::exit(code);
}

/// Splits a leading `--mode <mode>` off the argument vector.
fn take_mode(arguments: &[String]) -> (Mode, Vec<String>) {
    let fallback = std::env::var("FAKE_ADAPTER_MODE").unwrap_or_default();
    if let Some(name) = arguments.first() {
        if name == "--mode" {
            let mode = arguments.get(1).map_or(Mode::Ok, |text| Mode::parse(text));
            return (mode, arguments.iter().skip(2).cloned().collect());
        }
    }
    (Mode::parse(&fallback), arguments.to_vec())
}

/// Routes one verb.
fn dispatch(mode: Mode, verb: &str, tail: &[String]) -> i32 {
    match verb {
        "describe" => verbs::describe(mode),
        "validate" => verbs::validate(),
        "run" => verbs::run(mode, tail),
        "topics-create" => verbs::topics_create(mode, tail),
        "topics-delete" => 0,
        "verify" => verbs::verify(mode, tail),
        other => {
            eprintln!("fake-adapter: unknown verb {other}");
            EXIT_USAGE
        }
    }
}
