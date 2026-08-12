//! Tests pinning the shape of the crate's single failure type.

use crate::{SchemaError, SchemaErrorKind};

#[test]
fn a_kind_keeps_its_stable_label() {
    assert_eq!(SchemaErrorKind::Parse.label(), "parse");
    assert_eq!(SchemaErrorKind::WrongSchema.label(), "wrong schema");
    assert_eq!(SchemaErrorKind::InvalidField.label(), "invalid field");
    assert_eq!(
        SchemaErrorKind::NonCanonicalNumber.label(),
        "non-canonical number"
    );
    assert_eq!(SchemaErrorKind::Identity.label(), "identity");
}

#[test]
fn an_error_displays_its_kind_and_context() {
    let error = SchemaError::parse("trailing comma at line 3");

    assert_eq!(error.kind(), SchemaErrorKind::Parse);
    assert_eq!(error.context(), "trailing comma at line 3");
    assert_eq!(error.to_string(), "parse: trailing comma at line 3");
}

#[test]
fn an_invalid_field_error_names_the_field_first() {
    let error = SchemaError::invalid_field("subjects[0].name", "must not be empty");

    assert_eq!(error.kind(), SchemaErrorKind::InvalidField);
    assert_eq!(
        error.to_string(),
        "invalid field: subjects[0].name: must not be empty"
    );
}

#[test]
fn every_constructor_carries_its_kind() {
    assert_eq!(
        SchemaError::wrong_schema("x").kind(),
        SchemaErrorKind::WrongSchema
    );
    assert_eq!(
        SchemaError::non_canonical_number("x").kind(),
        SchemaErrorKind::NonCanonicalNumber
    );
    assert_eq!(SchemaError::identity("x").kind(), SchemaErrorKind::Identity);
    assert_eq!(
        SchemaError::new(SchemaErrorKind::Parse, "x").kind(),
        SchemaErrorKind::Parse
    );
}

#[test]
fn the_error_is_a_standard_error() {
    fn assert_error<T: core::error::Error>(_value: &T) {}

    assert_error(&SchemaError::identity("x"));
}
