//! The names an experiment may use: topic and subject length limits, the
//! subject role vocabulary, and the character set a topic name survives.
//!
//! These belong together because they are all consequences of one fact: a
//! subject name becomes part of a topic name, and a topic name becomes part of
//! a path inside an evidence bundle.

/// Longest topic name Kafka accepts.
pub const MAX_TOPIC_NAME_LENGTH: usize = 249;

/// Longest subject name this repository accepts.
///
/// Subject names become part of topic names, so they are bounded well below the
/// Kafka limit to leave room for the prefix, the run id, and the `-warmup`
/// suffix.
pub const MAX_SUBJECT_NAME_LENGTH: usize = 48;

/// The role label meaning "the subject a comparison divides by".
pub const SUBJECT_ROLE_BASE: &str = "base";

/// The role label meaning "the subject a comparison is about".
pub const SUBJECT_ROLE_HEAD: &str = "head";

/// The role label meaning "a third subject held fixed across attempts".
///
/// An anchor is not compared against; it is the known quantity that says
/// whether the machine itself moved between two attempts.
pub const SUBJECT_ROLE_ANCHOR: &str = "anchor";

/// Every role label a subject may carry, in comparison order.
///
/// A subject with no role is unlabeled, which is the only shape this
/// repository's scenarios produced before roles existed. Absence is therefore
/// never an error, and a labeled subject list is strictly more informative than
/// an unlabeled one rather than a different kind of document.
pub const SUBJECT_ROLES: [&str; 3] = [SUBJECT_ROLE_BASE, SUBJECT_ROLE_HEAD, SUBJECT_ROLE_ANCHOR];

/// Reports whether `role` is one of the three labels a subject may carry.
pub fn is_subject_role(role: &str) -> bool {
    SUBJECT_ROLES.contains(&role)
}

/// Reports whether `name` uses only characters Kafka accepts in a topic name.
///
/// The legal set is ASCII alphanumerics plus `.`, `_`, and `-`. The two names
/// `.` and `..` are excluded because they are legal path components and a topic
/// name ends up in file paths inside an evidence bundle.
pub fn is_topic_charset_safe(name: &str) -> bool {
    if name.is_empty() || name == "." || name == ".." {
        return false;
    }
    name.chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-'))
}
