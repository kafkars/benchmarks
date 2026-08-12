//! Tests pinning canonical and pretty JSON bytes.
#![expect(
    clippy::unwrap_used,
    reason = "canonicalization fixtures are exact; a bad one must fail the test immediately"
)]

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    SchemaErrorKind, canonical_bytes, parse_json_slice, parse_json_str, pretty_bytes,
    reject_floats, to_canonical_value,
};

/// A struct whose fields are declared in an order no sort would produce.
///
/// This is the guard against `serde_json`'s `preserve_order` feature arriving
/// through feature unification. With that feature on, `Value`'s map becomes
/// insertion ordered, this test's expected bytes change, and every identity in
/// the repository silently moves with them.
#[derive(Debug, Serialize, Deserialize)]
struct UnsortedDeclaration {
    zulu: u32,
    alpha: u32,
    mike: u32,
    bravo: Nested,
}

#[derive(Debug, Serialize, Deserialize)]
struct Nested {
    yankee: u32,
    charlie: u32,
}

fn unsorted() -> UnsortedDeclaration {
    UnsortedDeclaration {
        zulu: 1,
        alpha: 2,
        mike: 3,
        bravo: Nested {
            yankee: 4,
            charlie: 5,
        },
    }
}

#[test]
fn canonical_bytes_are_sorted_by_key_at_every_depth() {
    let bytes = canonical_bytes(&unsorted()).unwrap();

    assert_eq!(
        String::from_utf8(bytes).unwrap(),
        r#"{"alpha":2,"bravo":{"charlie":5,"yankee":4},"mike":3,"zulu":1}"#
    );
}

#[test]
fn the_sort_comes_from_the_value_tree_and_not_from_the_derive() {
    // Serializing the struct directly keeps declaration order, which is exactly
    // why canonicalization routes through `serde_json::Value` first.
    let direct = serde_json::to_string(&unsorted()).unwrap();

    assert!(
        direct.starts_with(r#"{"zulu":1,"alpha":2"#),
        "the derive should keep declaration order: {direct}"
    );
}

#[test]
fn pretty_bytes_are_two_space_indented_and_newline_terminated() {
    let bytes = pretty_bytes(&json!({"b": 1, "a": [1, 2]})).unwrap();

    assert_eq!(
        String::from_utf8(bytes).unwrap(),
        "{\n  \"a\": [\n    1,\n    2\n  ],\n  \"b\": 1\n}\n"
    );
}

#[test]
fn pretty_bytes_write_empty_containers_the_way_the_legacy_writer_did() {
    let bytes = pretty_bytes(&json!({"array": [], "object": {}})).unwrap();

    assert_eq!(
        String::from_utf8(bytes).unwrap(),
        "{\n  \"array\": [],\n  \"object\": {}\n}\n"
    );
}

#[test]
fn a_float_is_rejected_from_canonical_bytes() {
    let error = canonical_bytes(&json!({"ratio": 1.5})).unwrap_err();

    assert_eq!(error.kind(), SchemaErrorKind::NonCanonicalNumber);
    assert!(
        error.context().contains("/ratio"),
        "the rejection should point at the number: {error}"
    );
}

#[test]
fn a_float_nested_in_an_array_is_rejected_with_its_position() {
    let error = canonical_bytes(&json!({"pairs": [{"ratio": 0.5}]})).unwrap_err();

    assert!(
        error.context().contains("/pairs/0/ratio"),
        "the rejection should point at the number: {error}"
    );
}

#[test]
fn a_float_valued_integer_is_still_a_float() {
    // `1.0` and `1` are the same number and different bytes, which is the whole
    // reason identity documents are integers only.
    assert!(canonical_bytes(&json!({"value": 1.0_f64})).is_err());
}

#[test]
fn a_float_is_allowed_in_pretty_bytes() {
    let bytes = pretty_bytes(&json!({"ratio": 1.5})).unwrap();

    assert_eq!(
        String::from_utf8(bytes).unwrap(),
        "{\n  \"ratio\": 1.5\n}\n"
    );
}

#[test]
fn the_largest_unsigned_integer_survives_canonicalization() {
    let bytes = canonical_bytes(&json!({"sequence": u64::MAX})).unwrap();

    assert_eq!(
        String::from_utf8(bytes).unwrap(),
        r#"{"sequence":18446744073709551615}"#
    );
    assert!(reject_floats(&to_canonical_value(&json!(u64::MAX)).unwrap()).is_ok());
}

#[test]
fn a_negative_integer_is_canonical() {
    let bytes = canonical_bytes(&json!({"signal": -9})).unwrap();

    assert_eq!(String::from_utf8(bytes).unwrap(), r#"{"signal":-9}"#);
}

#[test]
fn strings_null_and_booleans_pass_the_float_check() {
    let value = to_canonical_value(&json!({"a": "x", "b": null, "c": true, "d": []})).unwrap();

    assert!(reject_floats(&value).is_ok());
}

#[test]
fn a_parse_failure_is_reported_as_a_parse_error() {
    let error = parse_json_str::<serde_json::Value>("{not json").unwrap_err();

    assert_eq!(error.kind(), SchemaErrorKind::Parse);
}

#[test]
fn json_parses_from_bytes_and_from_text_alike() {
    let from_bytes: UnsortedDeclaration =
        parse_json_slice(&canonical_bytes(&unsorted()).unwrap()).unwrap();
    let from_text: UnsortedDeclaration =
        parse_json_str(&String::from_utf8(canonical_bytes(&unsorted()).unwrap()).unwrap()).unwrap();

    assert_eq!(from_bytes.zulu, 1);
    assert_eq!(from_text.bravo.charlie, 5);
}

#[test]
fn pretty_bytes_are_the_canonical_bytes_with_whitespace() {
    let canonical = String::from_utf8(canonical_bytes(&unsorted()).unwrap()).unwrap();
    let pretty = String::from_utf8(pretty_bytes(&unsorted()).unwrap()).unwrap();

    let compacted: String = pretty
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect();

    assert_eq!(compacted, canonical);
    assert!(pretty.ends_with("}\n"));
}
