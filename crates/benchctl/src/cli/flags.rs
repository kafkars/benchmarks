//! Splitting an argument vector into named values, and reading those values
//! back as the types the verbs need.
//!
//! Both `--name value` and `--name=value` are accepted, and a repeated flag is
//! a usage error rather than a last-one-wins surprise: a command line that says
//! `--results` twice has two different intentions in it, and picking one
//! silently is how a run lands somewhere nobody looked.
//!
//! # The name is checked before the value
//!
//! [`collect_flags`] takes the verb's own set of option names and rejects a name
//! that is not in it *before* looking for a value. The order is the whole point.
//! `--turbo` with nothing after it used to report "`--turbo` needs a value",
//! which tells a reader the option exists and they got the syntax wrong — the
//! opposite of the truth, and the kind of message that sends somebody looking
//! for the value it wants instead of for the flag that does exist.

use std::collections::BTreeMap;

use crate::error::{CtlError, CtlResult};

/// Splits `--name value` and `--name=value` pairs, refusing repeats and any
/// name outside `known`.
pub(super) fn collect_flags(
    arguments: &[String],
    known: &[&str],
) -> CtlResult<BTreeMap<String, String>> {
    let mut flags = BTreeMap::new();
    let mut index = 0;
    while index < arguments.len() {
        let argument = &arguments[index];
        let Some(flag) = argument.strip_prefix("--") else {
            return Err(CtlError::usage(format!(
                "unexpected argument {argument:?}; every option starts with --"
            )));
        };
        let name = flag.split_once('=').map_or(flag, |(name, _)| name);
        if name.is_empty() {
            return Err(CtlError::usage("-- is not an option"));
        }
        if !known.contains(&name) {
            return Err(CtlError::usage(format!(
                "unknown option --{name}; this verb takes {}",
                known
                    .iter()
                    .map(|option| format!("--{option}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        let (name, value) = if let Some((name, value)) = flag.split_once('=') {
            index += 1;
            (name.to_owned(), value.to_owned())
        } else {
            let value = arguments.get(index + 1).ok_or_else(|| {
                CtlError::usage(format!("--{flag} needs a value, and none followed it"))
            })?;
            index += 2;
            (flag.to_owned(), value.clone())
        };
        if flags.insert(name.clone(), value).is_some() {
            return Err(CtlError::usage(format!(
                "--{name} was given more than once"
            )));
        }
    }
    Ok(flags)
}

pub(super) fn required(flags: &mut BTreeMap<String, String>, name: &str) -> CtlResult<String> {
    flags
        .remove(name)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| CtlError::usage(format!("--{name} is required")))
}

pub(super) fn seconds(
    flags: &mut BTreeMap<String, String>,
    name: &str,
    fallback: u64,
) -> CtlResult<u64> {
    match flags.remove(name) {
        None => Ok(fallback),
        Some(value) => {
            let parsed = unsigned(name, &value)?;
            if parsed == 0 {
                return Err(CtlError::usage(format!("--{name} must be positive")));
            }
            Ok(parsed)
        }
    }
}

pub(super) fn unsigned(name: &str, value: &str) -> CtlResult<u64> {
    value
        .parse()
        .map_err(|_| CtlError::usage(format!("--{name} expects a whole number, found {value:?}")))
}

pub(super) fn order(value: &str) -> CtlResult<Vec<String>> {
    let names: Vec<String> = value.split(',').map(str::trim).map(str::to_owned).collect();
    if names.iter().any(String::is_empty) {
        return Err(CtlError::usage(format!(
            "--order expects comma-separated subject names, found {value:?}"
        )));
    }
    Ok(names)
}

/// Reads `--bootstrap` and checks every endpoint in it looks like `host:port`.
///
/// Checked here rather than left to the client, because this string is written
/// into the runtime binding and the environment document of every bundle the
/// attempt seals. A typo that reaches those documents is a sealed, immutable
/// record of a cluster nobody ran against; the same typo caught at parse time
/// costs one line of stderr.
///
/// The check is deliberately shallow — a name that resolves is a question for
/// the network, not for an argument parser — but a value with no port, an empty
/// host, or a port that is not a number cannot be an endpoint under any
/// resolution.
pub(super) fn bootstrap(flags: &mut BTreeMap<String, String>) -> CtlResult<String> {
    let value = required(flags, "bootstrap")?;
    for endpoint in value.split(',') {
        let endpoint = endpoint.trim();
        let invalid = |why: &str| {
            CtlError::usage(format!(
                "--bootstrap expects comma-separated host:port endpoints; {endpoint:?} {why}"
            ))
        };
        let (host, port) = endpoint
            .rsplit_once(':')
            .ok_or_else(|| invalid("has no port"))?;
        if host.is_empty() {
            return Err(invalid("has no host"));
        }
        if port.parse::<u16>().is_err() {
            return Err(invalid("has no port a broker could listen on"));
        }
    }
    Ok(value)
}
