//! Deterministic cross-adapter payload vectors.

use super::payload;

#[test]
fn payload_identity_and_filler_are_stable() {
    let value = payload::make("0123456789abcdef", 42, 64);

    assert_eq!(&value[..36], b"KFB10123456789abcdef000000000000002a");
    assert_eq!(&value[36..], b"630da741eb852fc9630da741eb85");
}

#[test]
fn caller_owned_storage_can_be_filled_without_an_intermediate_allocation() {
    let mut value = vec![0; 64];

    payload::write(&mut value, "0123456789abcdef", 42);

    assert_eq!(value, payload::make("0123456789abcdef", 42, 64));
}
