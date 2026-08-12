//! A seeded, hand-rolled pseudo-random generator, because a resampling result
//! that cannot be reproduced from its seed is an opinion rather than evidence.
//!
//! # Why this exists at all
//!
//! The bootstrap needs random draws, and every draw has to be reconstructible
//! from a number that appears in the sealed output. An external generator would
//! make the confidence intervals depend on a dependency version: upgrade the
//! crate, get different numbers from the same evidence, and no reader can tell
//! whether the measurement moved or the library did. So the generator is
//! written out here, pinned by vectors, and versioned with this repository.
//!
//! # The construction
//!
//! `splitmix64` expands the caller's seed into four words of state, and
//! `xoshiro256**` produces the stream. Both are the published reference
//! algorithms, restated verbatim: splitmix64 because a single-word seed must
//! not leave the state correlated (a seed of `0` must not yield an all-zero
//! state, which xoshiro cannot escape), and xoshiro256** because it is small
//! enough to restate in twenty lines and has a period far beyond any resample
//! count this crate will ever draw.
//!
//! The legacy Node control plane used a 32-bit xorshift with the fixed seed
//! `0x6b61666b`. This is a deliberate departure: a 32-bit generator has a
//! period of 2^32 - 1, which is smaller than the 50,000 x n draws a bootstrap
//! makes on a large sample, and `state % n` over 32 bits carries a modulo bias
//! the legacy code never corrected. What is preserved from the legacy is the
//! *method* — see [`bootstrap`](crate::bootstrap) — not the bit stream, so
//! intervals from the two implementations agree in distribution rather than
//! digit for digit.

/// The additive constant `splitmix64` advances its state by (the odd 64-bit
/// approximation of the golden ratio).
const SPLIT_MIX_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

/// Advances a `splitmix64` state and returns the next value of its stream.
///
/// Exposed because seeding a second generator from a first is the only honest
/// way to derive an independent stream for a nested resampling loop.
pub fn split_mix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(SPLIT_MIX_GAMMA);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A deterministic `xoshiro256**` generator seeded from one 64-bit value.
///
/// Two generators built from the same seed produce the same stream on every
/// platform this workspace targets: the arithmetic is wrapping integer
/// arithmetic only, with no floating point and no address-dependent state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rng {
    state: [u64; 4],
}

impl Rng {
    /// Creates a generator whose whole future is determined by `seed`.
    #[must_use]
    pub fn from_seed(seed: u64) -> Self {
        let mut mixer = seed;
        let state = [
            split_mix64(&mut mixer),
            split_mix64(&mut mixer),
            split_mix64(&mut mixer),
            split_mix64(&mut mixer),
        ];
        Self { state }
    }

    /// Returns the next 64 bits of the stream.
    pub fn next_u64(&mut self) -> u64 {
        let result = self.state[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let shifted = self.state[1] << 17;
        self.state[2] ^= self.state[0];
        self.state[3] ^= self.state[1];
        self.state[1] ^= self.state[2];
        self.state[0] ^= self.state[3];
        self.state[2] ^= shifted;
        self.state[3] = self.state[3].rotate_left(45);
        result
    }

    /// Returns a value uniformly distributed over `0..bound`, or `0` when
    /// `bound` is zero or one.
    ///
    /// Rejection sampling rather than a bare modulo: a bootstrap draws indexes
    /// into a sample of arbitrary size, and `next_u64() % bound` over-weights
    /// the low indexes whenever `bound` does not divide the generator's range.
    /// The bias is small, but it is a bias in the direction of the first
    /// repetition, which is exactly the kind of thing a reader of a confidence
    /// interval is entitled to assume is absent.
    pub fn below(&mut self, bound: u64) -> u64 {
        if bound <= 1 {
            return 0;
        }
        // Largest multiple of `bound` that fits, so every residue is drawn from
        // the same number of source values. Discarding the partial block at the
        // top costs at most one extra draw per call.
        let zone = (u64::MAX / bound) * bound;
        loop {
            let value = self.next_u64();
            if value < zone {
                return value % bound;
            }
        }
    }

    /// Returns a value uniformly distributed over `0..bound` as a `usize`.
    ///
    /// A convenience for indexing a sample; `bound` comes from a slice length,
    /// so the conversion back cannot lose information.
    pub fn index_below(&mut self, bound: usize) -> usize {
        let drawn = self.below(u64::try_from(bound).unwrap_or(u64::MAX));
        usize::try_from(drawn).unwrap_or(0)
    }
}
