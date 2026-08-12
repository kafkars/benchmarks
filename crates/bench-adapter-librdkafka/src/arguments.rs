//! The argument surface: `--binary <path>` followed by one protocol verb.
//!
//! Parsing is hand-rolled and strict. An adapter is spawned by a control plane
//! that appends flags it was configured with, so a silently ignored argument is
//! a setting somebody believes is in effect; every unknown flag is a usage
//! error instead. The three verbs and their flags are fixed by the protocol and
//! are not configurable here.
//!
//! Exit codes are the adapter's half of the contract: `0` when the verb
//! answered, `64` when the command line was wrong, and `1` when the adapter
//! could not do what it was asked. `run` additionally always leaves a status
//! document behind, which is what distinguishes "the adapter failed and said
//! so" from "the adapter was killed".

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use bench_schema::{ResolvedExperiment, pretty_bytes};

use crate::{describe, run, translate, validate};

/// Exit code for a verb that answered.
pub(crate) const EXIT_OK: i32 = 0;

/// Exit code for a verb that could not do what it was asked.
pub(crate) const EXIT_FAILURE: i32 = 1;

/// Exit code for a command line this adapter does not understand.
pub(crate) const EXIT_USAGE: i32 = 64;

/// The usage text, printed for `--help` and on a usage error.
pub(crate) const USAGE: &str = "\
usage: bench-adapter-librdkafka --binary <path-to-librdkafka-producer-benchmark> <verb>

verbs:
  describe --json
  validate --experiment <resolved.json>
  run      --experiment <resolved.json> --output <dir>";

/// Runs the adapter and returns the process exit code.
///
/// `argv` includes the program name, so `main` can hand over
/// `std::env::args()` unchanged.
pub(crate) fn run(argv: &[String]) -> i32 {
    let arguments = argv.get(1..).unwrap_or_default();
    if matches!(arguments, [only] if only == "--help" || only == "-h") {
        println!("{USAGE}");
        return EXIT_OK;
    }
    let Some((binary, rest)) = binary_prefix(arguments) else {
        return usage("the first argument must be --binary <path>");
    };
    let Some((verb, flags)) = rest.split_first() else {
        return usage("no verb; expected describe, validate, or run");
    };
    match verb.as_str() {
        "describe" => describe_verb(flags),
        "validate" => validate_verb(flags),
        "run" => run_verb(&binary, flags),
        other => usage(&format!(
            "unknown verb {other:?}; expected describe, validate, or run"
        )),
    }
}

/// Splits the leading `--binary <path>` (or `--binary=<path>`) prefix.
fn binary_prefix(arguments: &[String]) -> Option<(PathBuf, &[String])> {
    let (first, rest) = arguments.split_first()?;
    if let Some(path) = first.strip_prefix("--binary=") {
        return (!path.is_empty()).then(|| (PathBuf::from(path), rest));
    }
    if first != "--binary" {
        return None;
    }
    let (path, rest) = rest.split_first()?;
    (!path.is_empty()).then(|| (PathBuf::from(path), rest))
}

/// `describe --json`: print the capability document.
///
/// The flag is accepted rather than required: JSON is the only dialect this
/// adapter speaks, so `--json` is a statement of the obvious that the protocol
/// spells out and a caller may reasonably omit.
fn describe_verb(flags: &[String]) -> i32 {
    match flags {
        [] => {}
        [only] if only == "--json" => {}
        _ => return usage("describe takes only --json"),
    }
    match pretty_bytes(&describe::description()) {
        Ok(bytes) => print_bytes(&bytes),
        Err(error) => failed(&error.to_string()),
    }
}

/// `validate --experiment <path>`: print the verdict on one experiment.
fn validate_verb(flags: &[String]) -> i32 {
    let mut parsed = match collect_flags(flags) {
        Ok(parsed) => parsed,
        Err(reason) => return usage(&reason),
    };
    let Some(path) = parsed.remove("experiment") else {
        return usage("validate needs --experiment <resolved.json>");
    };
    if let Some(unknown) = parsed.keys().next() {
        return usage(&format!("validate does not take --{unknown}"));
    }
    let experiment = match read_experiment(Path::new(&path)) {
        Ok(experiment) => experiment,
        Err(reason) => return failed(&reason),
    };
    // A subject this adapter cannot identify is not a reason to refuse the
    // experiment: `validate` has no output directory, so the per-subject topic
    // check simply does not apply.
    let subject = translate::subject_of(&experiment, None)
        .ok()
        .map(|subject| subject.name.clone());
    match pretty_bytes(&validate::report(&experiment, subject.as_deref())) {
        Ok(bytes) => print_bytes(&bytes),
        Err(error) => failed(&error.to_string()),
    }
}

/// `run --experiment <path> --output <dir>`: drive the C binary.
fn run_verb(binary: &Path, flags: &[String]) -> i32 {
    let mut parsed = match collect_flags(flags) {
        Ok(parsed) => parsed,
        Err(reason) => return usage(&reason),
    };
    let (Some(experiment), Some(output)) = (parsed.remove("experiment"), parsed.remove("output"))
    else {
        return usage("run needs --experiment <resolved.json> and --output <dir>");
    };
    if let Some(unknown) = parsed.keys().next() {
        return usage(&format!("run does not take --{unknown}"));
    }
    run::execute(binary, Path::new(&experiment), Path::new(&output))
}

/// Splits `--name value` and `--name=value` pairs, refusing repeats.
fn collect_flags(flags: &[String]) -> Result<BTreeMap<String, String>, String> {
    let mut parsed = BTreeMap::new();
    let mut index = 0;
    while index < flags.len() {
        let argument = &flags[index];
        let Some(flag) = argument.strip_prefix("--") else {
            return Err(format!(
                "unexpected argument {argument:?}; every option starts with --"
            ));
        };
        let (name, value) = if let Some((name, value)) = flag.split_once('=') {
            index += 1;
            (name.to_owned(), value.to_owned())
        } else {
            let value = flags
                .get(index + 1)
                .ok_or_else(|| format!("--{flag} needs a value, and none followed it"))?;
            index += 2;
            (flag.to_owned(), value.clone())
        };
        if name.is_empty() || value.is_empty() {
            return Err(format!("--{name} needs a value"));
        }
        if parsed.insert(name.clone(), value).is_some() {
            return Err(format!("--{name} was given more than once"));
        }
    }
    Ok(parsed)
}

/// Reads and parses a resolved experiment.
fn read_experiment(path: &Path) -> Result<ResolvedExperiment, String> {
    let bytes = std::fs::read(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    bench_schema::parse_json_slice(&bytes)
        .map_err(|error| format!("{} is not a resolved experiment: {error}", path.display()))
}

/// Writes a document to stdout.
fn print_bytes(bytes: &[u8]) -> i32 {
    match std::io::stdout().write_all(bytes) {
        Ok(()) => EXIT_OK,
        Err(error) => failed(&format!("write stdout: {error}")),
    }
}

/// Reports an adapter-side failure on stderr and returns [`EXIT_FAILURE`].
fn failed(reason: &str) -> i32 {
    eprintln!("bench-adapter-librdkafka: {reason}");
    EXIT_FAILURE
}

/// Reports a usage error on stderr and returns [`EXIT_USAGE`].
fn usage(reason: &str) -> i32 {
    eprintln!("bench-adapter-librdkafka: {reason}\n{USAGE}");
    EXIT_USAGE
}
