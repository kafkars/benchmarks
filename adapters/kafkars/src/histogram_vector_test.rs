//! The conformance verb's exact output shape.
#![expect(clippy::unwrap_used, reason = "test fixtures are exact")]

use crate::histogram_vector::emit;

fn vector(input: &str) -> String {
    let mut output = Vec::new();
    emit(&mut input.as_bytes(), &mut output).unwrap();
    String::from_utf8(output).unwrap()
}

#[test]
fn the_vector_is_the_compact_encoding_plus_one_newline() {
    let text = vector("0 1 127 128 255 256 1000000");

    assert_eq!(
        text,
        "{\"layout\":\"kafkars.log-linear.v1\",\"unit\":\"ns\",\"sub_bucket_bits\":7,\
         \"total\":7,\"min\":0,\"max\":1000000,\"sum\":1000767,\
         \"counts\":[[0,1],[1,1],[127,1],[128,1],[255,1],[256,1],[1780,1]]}\n",
        "this is the byte-pinned encoding the schema crate's own vector fixes"
    );
}

#[test]
fn values_may_be_separated_by_any_ascii_whitespace() {
    assert_eq!(vector("1 2 3"), vector("1\n2\n3\n"));
    assert_eq!(vector("1 2 3"), vector("  1\t2 \n 3  \n"));
}

#[test]
fn duplicates_and_saturating_sums_are_carried_exactly() {
    let text = vector("18446744073709551615 18446744073709551615 5");

    assert!(text.contains("\"total\":3"), "{text}");
    assert!(text.contains("\"min\":5"), "{text}");
    assert!(text.contains("\"max\":18446744073709551615"), "{text}");
    assert!(
        text.contains("\"sum\":18446744073709551615"),
        "the sum saturates rather than wrapping, and the C side must agree: {text}"
    );
    assert!(
        text.contains("[7423,2]"),
        "the duplicate lands twice in the topmost bucket: {text}"
    );
}

#[test]
fn no_values_is_an_empty_histogram_rather_than_an_error() {
    let text = vector("   \n  ");

    assert_eq!(
        text,
        "{\"layout\":\"kafkars.log-linear.v1\",\"unit\":\"ns\",\"sub_bucket_bits\":7,\
         \"total\":0,\"min\":null,\"max\":null,\"sum\":0,\"counts\":[]}\n"
    );
}

#[test]
fn a_value_that_is_not_an_unsigned_integer_names_its_position() {
    let mut output = Vec::new();

    let error = emit(&mut "1 2 -3".as_bytes(), &mut output)
        .unwrap_err()
        .to_string();

    assert!(error.contains("value 2"), "{error}");
    assert!(error.contains("\"-3\""), "{error}");
}
