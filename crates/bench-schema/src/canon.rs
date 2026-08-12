//! Canonical JSON: the exact bytes a document hashes to, and the exact bytes it
//! is written to disk as.
//!
//! Two byte forms exist and they are not interchangeable.
//!
//! - **Canonical** bytes are compact, have no trailing newline, and are what
//!   gets hashed. They are byte-sorted by key because every object goes through
//!   [`serde_json::Value`], whose map is a `BTreeMap`. The sort is a property of
//!   the data structure, not of a serializer setting, which is why this crate
//!   takes `serde_json` without the `preserve_order` feature and guards that
//!   choice with a test: with that feature on, the map becomes insertion
//!   ordered and every identity in this repository would silently change.
//! - **Pretty** bytes are the canonical value with two-space indentation and a
//!   trailing newline, matching the shape the legacy Node control plane's
//!   `JSON.stringify(value, null, 2) + "\n"` produced: same key order, same
//!   indentation, same terminator, and byte-identical strings, so sealed bundles
//!   from both harnesses read the same way in a diff.
//!
//!   The agreement is not total, and the exception is floating point. Rust
//!   writes an `f64` that happens to be integral as `2.0`; `JSON.stringify`
//!   writes `2`. Both are the same number and both round-trip, but they are not
//!   the same bytes, so a document carrying a float cannot be compared to its
//!   Node-written counterpart with `diff` or with a digest. That is not a defect
//!   to be repaired here: it is the same fact [`canonical_bytes`] refuses floats
//!   over, and it is precisely why no identity-bearing document carries one.
//!   Floats appear only in evidence — comparison ratios, suite summaries — which
//!   is read, not hashed.
//!
//! Because the pretty form is the canonical value with whitespace, a document
//! read back from disk and re-written is stable, and the pretty file and the
//! hashed bytes can never disagree about key order.
//!
//! Floating-point numbers are rejected by [`canonical_bytes`]. A `f64` has no
//! single textual form across writers — `1.0` versus `1`, shortest round-trip
//! versus fixed precision — so a document that carries one cannot be hashed
//! reproducibly. Every identity-bearing document in this crate is integers by
//! construction; ratios live in comparison documents, which are evidence and
//! are never hashed into an identity. [`pretty_bytes`] therefore allows floats:
//! it writes evidence, it does not name it.

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::{SchemaError, SchemaResult};

/// Converts a value into the `serde_json` tree used for canonicalization.
///
/// Every object in the result is byte-sorted by key; see the module contract.
pub fn to_canonical_value<T: Serialize + ?Sized>(value: &T) -> SchemaResult<Value> {
    serde_json::to_value(value)
        .map_err(|error| SchemaError::invalid_field("<document>", &error.to_string()))
}

/// Returns the compact, key-sorted bytes a document hashes to.
///
/// Fails with [`crate::SchemaErrorKind::NonCanonicalNumber`] if the document
/// contains a floating-point number anywhere.
pub fn canonical_bytes<T: Serialize + ?Sized>(value: &T) -> SchemaResult<Vec<u8>> {
    canonical_bytes_of_value(&to_canonical_value(value)?)
}

/// Returns the compact, key-sorted bytes of an already-built value.
pub fn canonical_bytes_of_value(value: &Value) -> SchemaResult<Vec<u8>> {
    reject_floats(value)?;
    serde_json::to_vec(value)
        .map_err(|error| SchemaError::invalid_field("<document>", &error.to_string()))
}

/// Returns the two-space indented, newline-terminated bytes a document is
/// written to disk as.
///
/// Floating-point numbers are allowed here: this is the evidence writer, not
/// the identity function.
pub fn pretty_bytes<T: Serialize + ?Sized>(value: &T) -> SchemaResult<Vec<u8>> {
    pretty_bytes_of_value(&to_canonical_value(value)?)
}

/// Returns the two-space indented, newline-terminated bytes of an already-built
/// value.
pub fn pretty_bytes_of_value(value: &Value) -> SchemaResult<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| SchemaError::invalid_field("<document>", &error.to_string()))?;
    bytes.push(b'\n');
    Ok(bytes)
}

/// Fails if `value` contains a floating-point number anywhere in its tree.
///
/// The context names the JSON pointer of the offending number so that a
/// rejected document can be fixed without bisecting it.
pub fn reject_floats(value: &Value) -> SchemaResult<()> {
    reject_floats_at(value, "")
}

fn reject_floats_at(value: &Value, path: &str) -> SchemaResult<()> {
    match value {
        Value::Number(number) => {
            if number.is_f64() {
                let location = if path.is_empty() { "/" } else { path };
                return Err(SchemaError::non_canonical_number(format!(
                    "{location} carries the floating-point number {number}, \
                     which has no reproducible textual form"
                )));
            }
            Ok(())
        }
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                reject_floats_at(item, &format!("{path}/{index}"))?;
            }
            Ok(())
        }
        Value::Object(entries) => {
            for (key, entry) in entries {
                reject_floats_at(entry, &format!("{path}/{key}"))?;
            }
            Ok(())
        }
        Value::Null | Value::Bool(_) | Value::String(_) => Ok(()),
    }
}

/// Parses JSON bytes into a document, reporting failures as
/// [`crate::SchemaErrorKind::Parse`].
pub fn parse_json_slice<T: DeserializeOwned>(bytes: &[u8]) -> SchemaResult<T> {
    serde_json::from_slice(bytes).map_err(|error| SchemaError::parse(error.to_string()))
}

/// Parses JSON text into a document, reporting failures as
/// [`crate::SchemaErrorKind::Parse`].
pub fn parse_json_str<T: DeserializeOwned>(text: &str) -> SchemaResult<T> {
    serde_json::from_str(text).map_err(|error| SchemaError::parse(error.to_string()))
}
