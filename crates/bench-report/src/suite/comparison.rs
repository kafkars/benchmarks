//! Who is compared with whom, and what the pair is called.
//!
//! # Pairing
//!
//! When the experiment labels subjects with roles, the comparisons are
//! `head/base` and `head/anchor`: the thing being tested over the thing it is
//! being tested against, and over the fixed reference that says whether the
//! machine itself moved. When no roles are declared, every subject is compared
//! against the first one in declaration order, because a ratio needs a named
//! denominator and declaration order is the only order the experiment states.

use bench_schema::{SUBJECT_ROLE_ANCHOR, SUBJECT_ROLE_BASE, SUBJECT_ROLE_HEAD};

/// A subject as the experiment declares it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SubjectIdentity {
    /// Subject name.
    pub(super) name: String,
    /// Declared role, when there is one.
    pub(super) role: Option<String>,
}

/// One directed comparison the suite will report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Comparison {
    /// Subject on top of the ratio.
    pub(super) numerator: String,
    /// Subject the ratio divides by.
    pub(super) denominator: String,
}

/// The comparisons this suite reports, role-aware when roles were declared.
pub(super) fn comparison_pairs(subjects: &[SubjectIdentity]) -> Vec<Comparison> {
    let with_role = |role: &str| {
        subjects
            .iter()
            .find(|subject| subject.role.as_deref() == Some(role))
            .map(|subject| subject.name.clone())
    };
    if let Some(head) = with_role(SUBJECT_ROLE_HEAD) {
        let mut pairs = Vec::new();
        for role in [SUBJECT_ROLE_BASE, SUBJECT_ROLE_ANCHOR] {
            if let Some(denominator) = with_role(role) {
                pairs.push(Comparison {
                    numerator: head.clone(),
                    denominator,
                });
            }
        }
        if !pairs.is_empty() {
            return pairs;
        }
    }
    let Some(first) = subjects.first() else {
        return Vec::new();
    };
    subjects
        .iter()
        .skip(1)
        .map(|subject| Comparison {
            numerator: subject.name.clone(),
            denominator: first.name.clone(),
        })
        .collect()
}

/// How a paired ratio series is named in the dispersion table.
///
/// The slash is the label: a row named `head/base` is a ratio series, a row
/// named `head` is that subject's own values. Subject names cannot contain a
/// slash, so the two can never be confused.
pub(super) fn ratio_label(comparison: &Comparison) -> String {
    format!("{}/{}", comparison.numerator, comparison.denominator)
}
