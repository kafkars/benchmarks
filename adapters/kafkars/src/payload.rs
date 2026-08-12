//! Cross-adapter deterministic payload construction.
//!
//! # The filler repeats every sixteen sequences
//!
//! Every byte after the identity prefix is `HEX[(SEED + 17*sequence + 13*index)
//! & 0x0f]`, and `17 ≡ 1 (mod 16)`, so the filler depends on `sequence` only
//! through `sequence % 16`. Sixteen payloads therefore contain every filler
//! this function can produce, which is what lets the measured path prebuild a
//! template pool and rewrite nothing but [`SEQUENCE_RANGE`] per record. The
//! bytes are unchanged either way; `producer::v2::pool` asserts that against
//! [`make`] rather than trusting the arithmetic above.

use std::ops::Range;

const PREFIX_BYTES: usize = 36;
const HEX: &[u8; 16] = b"0123456789abcdef";
const SEED: u64 = 44;

/// Distinct filler patterns the identity function can produce.
pub(crate) const FILLER_PERIOD: u64 = 16;

/// Bytes holding the record's zero-padded hexadecimal sequence number.
pub(crate) const SEQUENCE_RANGE: Range<usize> = 20..36;

pub(crate) fn make(run_id: &str, sequence: u64, size: usize) -> Vec<u8> {
    let mut value = vec![b'0'; size];
    write(&mut value, run_id, sequence);
    value
}

/// Rewrites only the sequence field of a payload whose filler already belongs
/// to `sequence`'s residue class.
pub(crate) fn write_sequence(value: &mut [u8], sequence: u64) {
    write_hex(sequence, &mut value[SEQUENCE_RANGE]);
}

pub(crate) fn write(value: &mut [u8], run_id: &str, sequence: u64) {
    value[..4].copy_from_slice(b"KFB1");
    value[4..20].copy_from_slice(run_id.as_bytes());
    write_hex(sequence, &mut value[20..36]);
    for (index, byte) in value[PREFIX_BYTES..].iter_mut().enumerate() {
        let selector = SEED
            .wrapping_add(sequence.wrapping_mul(17))
            .wrapping_add((index as u64).wrapping_mul(13));
        *byte = HEX[(selector & 0x0f) as usize];
    }
}

fn write_hex(value: u64, target: &mut [u8]) {
    let width = target.len();
    for (index, byte) in target.iter_mut().enumerate() {
        let shift = (width - index - 1) * 4;
        *byte = HEX[((value >> shift) & 0x0f) as usize];
    }
}
