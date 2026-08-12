//! Vectors and distribution checks pinning the generator's stream.
//!
//! The vectors were produced from an independent transcription of the published
//! `splitmix64` and `xoshiro256**` reference implementations, not from this
//! code, so a rewrite that changes the stream fails here rather than silently
//! moving every confidence interval in the repository.

use crate::rng::{Rng, split_mix64};

#[test]
fn split_mix64_matches_the_published_vectors() {
    let mut state = 0u64;

    let stream: Vec<u64> = (0..4).map(|_| split_mix64(&mut state)).collect();

    assert_eq!(
        stream,
        vec![
            0xE220_A839_7B1D_CDAF,
            0x6E78_9E6A_A1B9_65F4,
            0x06C4_5D18_8009_454F,
            0xF88B_B8A8_724C_81EC,
        ]
    );
}

#[test]
fn a_zero_seed_produces_the_pinned_stream() {
    let mut rng = Rng::from_seed(0);

    let stream: Vec<u64> = (0..8).map(|_| rng.next_u64()).collect();

    assert_eq!(
        stream,
        vec![
            0x99EC_5F36_CB75_F2B4,
            0xBF6E_1F78_4956_452A,
            0x1A5F_849D_4933_E6E0,
            0x6AA5_94F1_262D_2D2C,
            0xBBA5_AD4A_1F84_2E59,
            0xFFEF_8375_D9EB_CACA,
            0x6C16_0DEE_D2F5_4C98,
            0x8920_AD64_8FC3_0A3F,
        ]
    );
}

#[test]
fn the_legacy_seed_produces_the_pinned_stream() {
    // 0x6b61666b is the ASCII "kafk" the legacy control plane seeded with; the
    // seed carries over even though the generator behind it does not.
    let mut rng = Rng::from_seed(0x6B61_666B);

    let stream: Vec<u64> = (0..4).map(|_| rng.next_u64()).collect();

    assert_eq!(
        stream,
        vec![
            0x9B3D_4113_946D_8622,
            0xD2C1_48DA_056D_CC1A,
            0xF279_1D3F_8354_AD78,
            0x77F5_C936_457D_9E0E,
        ]
    );
}

#[test]
fn the_same_seed_replays_the_same_stream() {
    let mut first = Rng::from_seed(7);
    let mut second = Rng::from_seed(7);

    let left: Vec<u64> = (0..64).map(|_| first.next_u64()).collect();
    let right: Vec<u64> = (0..64).map(|_| second.next_u64()).collect();

    assert_eq!(left, right);
    assert_eq!(first, second);
}

#[test]
fn different_seeds_diverge() {
    let mut first = Rng::from_seed(1);
    let mut second = Rng::from_seed(2);

    let left: Vec<u64> = (0..8).map(|_| first.next_u64()).collect();
    let right: Vec<u64> = (0..8).map(|_| second.next_u64()).collect();

    assert_ne!(left, right);
}

#[test]
fn a_degenerate_bound_is_always_zero() {
    let mut rng = Rng::from_seed(3);

    assert_eq!(rng.below(0), 0);
    assert_eq!(rng.below(1), 0);
    assert_eq!(rng.index_below(0), 0);
    assert_eq!(rng.index_below(1), 0);
}

#[test]
fn bounded_draws_stay_inside_the_bound() {
    let mut rng = Rng::from_seed(11);

    for bound in [2u64, 3, 5, 7, 1000, u64::MAX / 3] {
        for _ in 0..200 {
            assert!(rng.below(bound) < bound);
        }
    }
}

#[test]
fn bounded_draws_cover_every_residue_roughly_evenly() {
    // Not a statistical test with a threshold anyone should trust: a coarse
    // check that no residue is starved, which is what a modulo bias would look
    // like on a bound that does not divide the range.
    let mut rng = Rng::from_seed(0x5EED);
    let mut buckets = [0usize; 7];

    for _ in 0..70_000 {
        buckets[rng.index_below(buckets.len())] += 1;
    }

    for count in buckets {
        assert!(
            (9_000..=11_000).contains(&count),
            "bucket count {count} is far from the expected 10000"
        );
    }
}
