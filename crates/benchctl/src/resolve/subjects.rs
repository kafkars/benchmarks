//! Probed subjects into subject specifications, and the identity tokens that
//! may be hashed into an experiment id.
//!
//! A subject whose adapter declined the experiment is refused with the
//! adapter's own reasons, because running the comparison without it would
//! quietly answer a different question. That refusal lives one module up, where
//! the lock is built; what lives here is the mapping and the identity check
//! every subject has to pass before it can be part of an experiment at all.

use bench_schema::SubjectSpec;

use crate::error::{CtlError, CtlResult};

use super::inputs::ResolveInputs;

/// Turns probed subjects into subject specifications, in subjects-file order.
pub(super) fn subjects(inputs: &ResolveInputs) -> CtlResult<Vec<SubjectSpec>> {
    if inputs.subjects.is_empty() {
        return Err(CtlError::invalid(
            "the subjects file names nothing to measure",
        ));
    }
    inputs
        .subjects
        .iter()
        .map(|probe| {
            check_identity_token(&probe.subject.name, "adapter_name", &probe.describe.name)?;
            check_identity_token(
                &probe.subject.name,
                "adapter_version",
                &probe.describe.version,
            )?;
            Ok(SubjectSpec {
                name: probe.subject.name.clone(),
                adapter_name: probe.describe.name.clone(),
                adapter_version: probe.describe.version.clone(),
                command: probe.subject.command.clone(),
                // Carried, not invented: the role is the operator's statement
                // about what the subject is for, and it participates in the
                // experiment id.
                role: probe.subject.role.clone(),
            })
        })
        .collect()
}

/// Refuses an adapter identity string that cannot be part of an experiment id.
///
/// `adapter_name` and `adapter_version` are hashed into the identity, which is
/// what makes two attempts on two machines repetitions of one experiment. A
/// value carrying a path separator or whitespace is almost always a build path
/// or a compiler invocation that an adapter stamped into its own version — and
/// a version containing `/Users/somebody/src` gives every checkout a different
/// experiment id for the same experiment, so the suite that aggregates them
/// finds one attempt each and refuses to compare them. The failure would appear
/// as a statistics problem, days later, nowhere near its cause.
///
/// Refusing at resolution is the last moment this is cheap: the adapter has
/// answered `describe`, no topic exists yet, and nothing has been sealed.
fn check_identity_token(subject: &str, field: &str, value: &str) -> CtlResult<()> {
    if value.is_empty() {
        return Err(CtlError::invalid(format!(
            "subject {subject:?} reported an empty {field}, which cannot identify anything"
        )));
    }
    let offending = value.chars().find(|character| {
        *character == '/'
            || *character == '\\'
            || character.is_whitespace()
            || character.is_control()
    });
    if let Some(character) = offending {
        return Err(CtlError::invalid(format!(
            "subject {subject:?} reported {field} {value:?}, which contains {character:?}. \
             This field is hashed into the experiment id, so a path separator or whitespace in \
             it — a build directory or a compiler line stamped into a version — would give the \
             same experiment a different identity on every machine"
        )));
    }
    Ok(())
}
