//! The one place TOML text becomes either a document or a schema error.
//!
//! Every source file in this module tree parses through here so that the error
//! a person sees names the document they were editing rather than the type the
//! parser happened to be building.

use crate::error::{SchemaError, SchemaResult};

pub(super) fn parse_toml<T: serde::de::DeserializeOwned>(
    text: &str,
    document: &str,
) -> SchemaResult<T> {
    toml::from_str(text).map_err(|error| SchemaError::parse(format!("{document}: {error}")))
}
