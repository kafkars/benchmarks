//! The one failure type this crate returns: a stable kind plus the context that
//! makes the kind actionable.
//!
//! The house rule is that an error names what went wrong in terms a reader of
//! sealed evidence understands, not in terms of the library that noticed. A
//! `serde_json` message about a trailing comma is not useful on its own; the
//! same message attached to [`SchemaErrorKind::Parse`] and the document that
//! failed is.
//!
//! The type is hand-rolled rather than derived from a macro crate. This crate
//! has four dependencies and none of them exist to shorten an error enum.

use core::fmt;

/// Stable category for a schema failure.
///
/// Callers switch on the kind; the context string is for humans and may change
/// wording without notice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SchemaErrorKind {
    /// Bytes did not parse as the document format they claimed to be.
    Parse,
    /// A document parsed but declared a schema id the caller did not ask for.
    WrongSchema,
    /// A document parsed but a field is missing, empty, out of range, or
    /// contradicts another field.
    InvalidField,
    /// A value that has to participate in an identity carried a number that
    /// cannot be hashed reproducibly — in practice, a floating-point number.
    NonCanonicalNumber,
    /// An identity string was malformed, or an identity could not be computed
    /// from a document that was supposed to supply one.
    Identity,
}

impl SchemaErrorKind {
    /// Returns the stable lowercase label used when formatting the error.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Parse => "parse",
            Self::WrongSchema => "wrong schema",
            Self::InvalidField => "invalid field",
            Self::NonCanonicalNumber => "non-canonical number",
            Self::Identity => "identity",
        }
    }
}

impl fmt::Display for SchemaErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.label())
    }
}

/// A schema failure: what kind of failure it was, and enough context to act.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaError {
    kind: SchemaErrorKind,
    context: String,
}

impl SchemaError {
    /// Creates an error of `kind` carrying `context`.
    pub fn new(kind: SchemaErrorKind, context: impl Into<String>) -> Self {
        Self {
            kind,
            context: context.into(),
        }
    }

    /// Creates a [`SchemaErrorKind::Parse`] error.
    pub fn parse(context: impl Into<String>) -> Self {
        Self::new(SchemaErrorKind::Parse, context)
    }

    /// Creates a [`SchemaErrorKind::WrongSchema`] error.
    pub fn wrong_schema(context: impl Into<String>) -> Self {
        Self::new(SchemaErrorKind::WrongSchema, context)
    }

    /// Creates a [`SchemaErrorKind::InvalidField`] error naming the offending
    /// field.
    ///
    /// The field path is written the way it appears in the document, so
    /// `subjects[0].name` rather than a Rust path.
    pub fn invalid_field(field: &str, reason: &str) -> Self {
        Self::new(SchemaErrorKind::InvalidField, format!("{field}: {reason}"))
    }

    /// Creates a [`SchemaErrorKind::NonCanonicalNumber`] error.
    pub fn non_canonical_number(context: impl Into<String>) -> Self {
        Self::new(SchemaErrorKind::NonCanonicalNumber, context)
    }

    /// Creates a [`SchemaErrorKind::Identity`] error.
    pub fn identity(context: impl Into<String>) -> Self {
        Self::new(SchemaErrorKind::Identity, context)
    }

    /// Returns the stable failure category.
    pub const fn kind(&self) -> SchemaErrorKind {
        self.kind
    }

    /// Returns the human-readable context.
    pub fn context(&self) -> &str {
        &self.context
    }
}

impl fmt::Display for SchemaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.kind, self.context)
    }
}

impl core::error::Error for SchemaError {}

/// Result alias for the fallible operations in this crate.
pub type SchemaResult<T> = Result<T, SchemaError>;
