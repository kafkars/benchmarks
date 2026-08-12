//! Splitting an argument vector into named values, and reading those values
//! back as the types the verbs need.
//!
//! Both `--name value` and `--name=value` are accepted, and a repeated flag is
//! a usage error rather than a last-one-wins surprise: a command line that says
//! `--results` twice has two different intentions in it, and picking one
//! silently is how a run lands somewhere nobody looked.

use std::collections::BTreeMap;

use crate::error::{CtlError, CtlResult};

/// Splits `--name value` and `--name=value` pairs, refusing repeats.
pub(super) fn collect_flags(arguments: &[String]) -> CtlResult<BTreeMap<String, String>> {
    let mut flags = BTreeMap::new();
    let mut index = 0;
    while index < arguments.len() {
        let argument = &arguments[index];
        let Some(flag) = argument.strip_prefix("--") else {
            return Err(CtlError::usage(format!(
                "unexpected argument {argument:?}; every option starts with --"
            )));
        };
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
        if name.is_empty() {
            return Err(CtlError::usage("-- is not an option"));
        }
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

pub(super) fn reject_unknown(flags: &BTreeMap<String, String>) -> CtlResult<()> {
    if let Some(name) = flags.keys().next() {
        return Err(CtlError::usage(format!("unknown option --{name}")));
    }
    Ok(())
}
