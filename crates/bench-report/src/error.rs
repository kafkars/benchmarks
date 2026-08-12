//! The one failure type this crate returns: a stable kind plus the context that
//! makes it actionable.
//!
//! Reporting fails for three reasons, and a caller wants to tell them apart. A
//! bundle that is not on disk is an operator mistake; a document that does not
//! parse is a broken or truncated bundle; a sample that cannot support a
//! statistic is neither, it is evidence that is simply too thin to summarize.
//! Collapsing the three into one string would leave `benchctl` unable to decide
//! whether to exit non-zero or to print a report that says less.
//!
//! Reporting never *reduces* evidence to make a summary possible. An empty or
//! degenerate sample produces an absent statistic or an error, never a zero.

use core::fmt;

/// Stable category for a reporting failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReportErrorKind {
    /// A bundle file could not be read.
    Io,
    /// A document was read but did not parse, or declared the wrong schema.
    Document,
    /// A sample was empty, degenerate, or otherwise unable to carry the
    /// statistic asked of it.
    Sample,
}

impl ReportErrorKind {
    /// Returns the stable lowercase label used when formatting the error.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Io => "io",
            Self::Document => "document",
            Self::Sample => "sample",
        }
    }
}

impl fmt::Display for ReportErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.label())
    }
}

/// A reporting failure: what kind it was, and enough context to act.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportError {
    kind: ReportErrorKind,
    context: String,
}

impl ReportError {
    /// Creates an error of `kind` carrying `context`.
    pub fn new(kind: ReportErrorKind, context: impl Into<String>) -> Self {
        Self {
            kind,
            context: context.into(),
        }
    }

    /// Creates a [`ReportErrorKind::Io`] error naming the path that failed.
    pub fn io(path: &std::path::Path, cause: &impl fmt::Display) -> Self {
        Self::new(ReportErrorKind::Io, format!("{}: {cause}", path.display()))
    }

    /// Creates a [`ReportErrorKind::Document`] error.
    pub fn document(context: impl Into<String>) -> Self {
        Self::new(ReportErrorKind::Document, context)
    }

    /// Creates a [`ReportErrorKind::Sample`] error.
    pub fn sample(context: impl Into<String>) -> Self {
        Self::new(ReportErrorKind::Sample, context)
    }

    /// Returns the stable failure category.
    pub const fn kind(&self) -> ReportErrorKind {
        self.kind
    }

    /// Returns the human-readable context.
    pub fn context(&self) -> &str {
        &self.context
    }
}

impl fmt::Display for ReportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.kind, self.context)
    }
}

impl core::error::Error for ReportError {}

impl From<bench_schema::SchemaError> for ReportError {
    /// Every schema failure is a failure to read a document, whatever the
    /// schema crate's own finer classification said.
    fn from(error: bench_schema::SchemaError) -> Self {
        Self::document(error.to_string())
    }
}

/// Result alias for the fallible operations in this crate.
pub type ReportResult<T> = Result<T, ReportError>;
