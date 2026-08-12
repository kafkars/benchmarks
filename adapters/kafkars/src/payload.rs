//! Cross-adapter deterministic payload construction.

const PREFIX_BYTES: usize = 36;
const HEX: &[u8; 16] = b"0123456789abcdef";
const SEED: u64 = 44;

pub(crate) fn make(run_id: &str, sequence: u64, size: usize) -> Vec<u8> {
    let mut value = vec![b'0'; size];
    write(&mut value, run_id, sequence);
    value
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
