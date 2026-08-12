//! The histogram conformance verb: values in, one encoded histogram out.
//!
//! The log-linear layout is a byte contract between two languages, and the
//! only way to hold two implementations to it is to make each one produce
//! bytes a third party pinned. This verb is that producer: it reads values,
//! records them through the same [`bench_schema::Histogram`] the measured path
//! uses, and prints the compact serde encoding — the reference form the C
//! adapter must reproduce exactly.
//!
//! It deliberately shares no code with the C side and no fixture with itself:
//! the committed input under `conformance/histogram/` is read from standard
//! input, so the vector proves the *encoding* agrees rather than proving two
//! copies of one constant are equal.

use std::{
    error::Error,
    io::{Read, Write},
};

use bench_schema::Histogram;

/// Reads whitespace-separated `u64` values and prints their encoded histogram.
pub(crate) fn emit(input: &mut impl Read, output: &mut impl Write) -> Result<(), Box<dyn Error>> {
    let mut text = String::new();
    input.read_to_string(&mut text)?;
    let mut histogram = Histogram::new();
    for (position, field) in text.split_ascii_whitespace().enumerate() {
        let value = field.parse::<u64>().map_err(|error| {
            format!("value {position} ({field:?}) is not an unsigned 64-bit integer: {error}")
        })?;
        histogram.record(value);
    }
    let encoded = serde_json::to_string(&histogram.encode())?;
    writeln!(output, "{encoded}")?;
    Ok(())
}
