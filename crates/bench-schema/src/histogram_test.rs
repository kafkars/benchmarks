//! Pins log-linear buckets, conservative percentiles, and the cross-language bytes.
#![expect(clippy::unwrap_used, reason = "test assertions may unwrap")]

use crate::histogram::{
    EncodedHistogram, HISTOGRAM_LAYOUT_V1, Histogram, MAX_BUCKET_INDEX, SUB_BUCKET_BITS,
    SUB_BUCKET_COUNT, bucket_high, bucket_index, bucket_low,
};

#[test]
fn small_values_get_exact_buckets() {
    for value in 0..SUB_BUCKET_COUNT {
        let index = bucket_index(value);
        assert_eq!(u64::from(index), value);
        assert_eq!(bucket_low(index), value);
        assert_eq!(bucket_high(index), value);
    }
}

#[test]
fn every_value_falls_inside_its_bucket_bounds() {
    let probes = [
        128u64,
        129,
        255,
        256,
        257,
        1_000,
        4_095,
        4_096,
        65_535,
        1_000_000,
        123_456_789,
        u64::from(u32::MAX),
        1 << 62,
        u64::MAX - 1,
        u64::MAX,
    ];
    for value in probes {
        let index = bucket_index(value);
        assert!(
            bucket_low(index) <= value && value <= bucket_high(index),
            "value {value} outside bucket {index}: [{}, {}]",
            bucket_low(index),
            bucket_high(index)
        );
    }
}

#[test]
fn bucket_relative_error_is_bounded() {
    assert_eq!(
        SUB_BUCKET_COUNT, 128,
        "the 1/128 literal below tracks the layout"
    );
    for value in [200u64, 5_000, 1_000_000, 987_654_321, 1 << 50] {
        let index = bucket_index(value);
        let width = bucket_high(index) - bucket_low(index) + 1;
        #[expect(clippy::cast_precision_loss, reason = "bounds check only")]
        let relative = width as f64 / bucket_low(index) as f64;
        assert!(
            relative <= 1.0 / 128.0 + f64::EPSILON,
            "bucket for {value} has relative width {relative}"
        );
    }
}

#[test]
fn buckets_tile_the_line_without_gaps() {
    let mut previous_high = None;
    for index in 0..2_048u32 {
        let low = bucket_low(index);
        let high = bucket_high(index);
        assert!(low <= high);
        if let Some(previous) = previous_high {
            assert_eq!(low, previous + 1, "gap or overlap before bucket {index}");
        }
        previous_high = Some(high);
    }
}

#[test]
fn percentiles_report_conservative_upper_bounds() {
    let mut histogram = Histogram::new();
    for value in 1..=1_000u64 {
        histogram.record(value * 1_000);
    }
    let p50 = histogram.value_at_quantile(0.50).unwrap();
    let p99 = histogram.value_at_quantile(0.99).unwrap();
    let p100 = histogram.value_at_quantile(1.0).unwrap();
    assert!(p50 >= 500_000, "p50 {p50} must not understate");
    assert!(p50 <= 504_000, "p50 {p50} exceeds one bucket of slack");
    assert!((990_000..=998_000).contains(&p99), "p99 was {p99}");
    assert_eq!(p100, 1_000_000, "the maximum is exact");
    assert_eq!(
        histogram.value_at_quantile(0.0).unwrap(),
        histogram.min().unwrap()
    );
}

#[test]
fn min_max_sum_and_mean_are_exact() {
    let mut histogram = Histogram::new();
    for value in [5u64, 1_000_003, 77] {
        histogram.record(value);
    }
    assert_eq!(histogram.total(), 3);
    assert_eq!(histogram.min(), Some(5));
    assert_eq!(histogram.max(), Some(1_000_003));
    assert_eq!(histogram.sum(), 1_000_085);
    let mean = histogram.mean().unwrap();
    assert!((mean - 333_361.666).abs() < 0.01);
}

#[test]
fn merge_is_lossless_over_buckets() {
    let mut left = Histogram::new();
    let mut right = Histogram::new();
    let mut both = Histogram::new();
    for value in [3u64, 200, 999_999] {
        left.record(value);
        both.record(value);
    }
    for value in [3u64, 4_096, u64::from(u32::MAX)] {
        right.record(value);
        both.record(value);
    }
    left.merge(&right);
    assert_eq!(left, both);
}

#[test]
fn the_serialized_form_is_byte_pinned() {
    let mut histogram = Histogram::new();
    for value in [0u64, 1, 127, 128, 255, 256, 1_000_000] {
        histogram.record(value);
    }
    let encoded = histogram.encode();
    let json = serde_json::to_string(&encoded).unwrap();
    assert_eq!(
        json,
        "{\"layout\":\"kafkars.log-linear.v1\",\"unit\":\"ns\",\"sub_bucket_bits\":7,\
         \"total\":7,\"min\":0,\"max\":1000000,\"sum\":1000767,\
         \"counts\":[[0,1],[1,1],[127,1],[128,1],[255,1],[256,1],[1780,1]]}"
    );
    let reparsed: EncodedHistogram = serde_json::from_str(&json).unwrap();
    assert_eq!(Histogram::decode(&reparsed).unwrap(), histogram);
}

#[test]
fn decode_rejects_broken_invariants() {
    let good = {
        let mut h = Histogram::new();
        h.record(42);
        h.encode()
    };
    let mut wrong_layout = good.clone();
    wrong_layout.layout = "hdr".to_owned();
    assert!(Histogram::decode(&wrong_layout).is_err());
    let mut zero_count = good.clone();
    zero_count.counts = vec![(42, 0)];
    zero_count.total = 0;
    assert!(Histogram::decode(&zero_count).is_err());
    let mut unsorted = good.clone();
    unsorted.counts = vec![(9, 1), (3, 1)];
    unsorted.total = 2;
    assert!(Histogram::decode(&unsorted).is_err());
    let mut mismatched_total = good.clone();
    mismatched_total.total = 5;
    assert!(Histogram::decode(&mismatched_total).is_err());
    let mut empty_with_min = good;
    empty_with_min.counts = Vec::new();
    empty_with_min.total = 0;
    assert!(Histogram::decode(&empty_with_min).is_err());
    assert_eq!(HISTOGRAM_LAYOUT_V1, "kafkars.log-linear.v1");
    assert_eq!(SUB_BUCKET_BITS, 7);
}

#[test]
fn the_index_ceiling_is_the_index_of_the_largest_value() {
    assert_eq!(MAX_BUCKET_INDEX, bucket_index(u64::MAX));
    assert_eq!(MAX_BUCKET_INDEX, 7_423, "the layout has not moved");
    assert_eq!(
        bucket_high(MAX_BUCKET_INDEX),
        u64::MAX,
        "the top bucket must reach the top of the domain"
    );
}

#[test]
fn bucket_bounds_are_total_over_every_index() {
    for index in [
        MAX_BUCKET_INDEX,
        MAX_BUCKET_INDEX + 1,
        8_319,
        8_320,
        u32::MAX / 2,
        u32::MAX - 1,
        u32::MAX,
    ] {
        let low = bucket_low(index);
        let high = bucket_high(index);
        assert!(low <= high, "bucket {index} has bounds [{low}, {high}]");
        assert_eq!(
            (low, high),
            (bucket_low(MAX_BUCKET_INDEX), bucket_high(MAX_BUCKET_INDEX)),
            "an impossible index saturates to the top bucket"
        );
    }
}

fn one_recording() -> EncodedHistogram {
    let mut histogram = Histogram::new();
    histogram.record(42);
    histogram.encode()
}

#[test]
fn decode_rejects_an_index_no_value_can_reach() {
    for index in [MAX_BUCKET_INDEX + 1, 8_319, 8_320, u32::MAX] {
        let mut hostile = one_recording();
        hostile.counts = vec![(index, 1)];
        let error = Histogram::decode(&hostile).unwrap_err();
        assert!(
            error.to_string().contains(&MAX_BUCKET_INDEX.to_string()),
            "the rejection must name the ceiling: {error}"
        );
    }
    let mut at_the_ceiling = one_recording();
    at_the_ceiling.counts = vec![(MAX_BUCKET_INDEX, 1)];
    at_the_ceiling.min = Some(u64::MAX);
    at_the_ceiling.max = Some(u64::MAX);
    at_the_ceiling.sum = u64::MAX;
    assert!(
        Histogram::decode(&at_the_ceiling).is_ok(),
        "the ceiling itself is a real bucket"
    );
}

#[test]
fn decode_rejects_an_extreme_that_disagrees_with_emptiness() {
    let mut empty_with_only_min = one_recording();
    empty_with_only_min.counts = Vec::new();
    empty_with_only_min.total = 0;
    empty_with_only_min.sum = 0;
    empty_with_only_min.max = None;
    assert!(Histogram::decode(&empty_with_only_min).is_err());

    let mut empty_with_only_max = one_recording();
    empty_with_only_max.counts = Vec::new();
    empty_with_only_max.total = 0;
    empty_with_only_max.sum = 0;
    empty_with_only_max.min = None;
    assert!(Histogram::decode(&empty_with_only_max).is_err());

    let mut recorded_without_min = one_recording();
    recorded_without_min.min = None;
    assert!(Histogram::decode(&recorded_without_min).is_err());

    let mut recorded_without_max = one_recording();
    recorded_without_max.max = None;
    assert!(Histogram::decode(&recorded_without_max).is_err());

    let mut wholly_empty = one_recording();
    wholly_empty.counts = Vec::new();
    wholly_empty.total = 0;
    wholly_empty.sum = 0;
    wholly_empty.min = None;
    wholly_empty.max = None;
    assert!(
        Histogram::decode(&wholly_empty).is_ok(),
        "a histogram that recorded nothing is legal"
    );
}

#[test]
fn a_percentile_of_counts_that_overflow_a_u64_is_a_number_not_a_panic() {
    let half = u64::MAX / 2 + 1;
    let hostile = EncodedHistogram {
        layout: HISTOGRAM_LAYOUT_V1.to_owned(),
        unit: "ns".to_owned(),
        sub_bucket_bits: SUB_BUCKET_BITS,
        total: u64::MAX,
        min: Some(1),
        max: Some(2),
        sum: u64::MAX,
        counts: vec![(1, half), (2, half)],
    };
    let histogram = Histogram::decode(&hostile).unwrap();
    assert!(histogram.value_at_quantile(0.99).is_some());
    assert!(histogram.value_at_quantile(1.0).is_some());
}

#[test]
fn decode_rejects_a_minimum_above_its_maximum() {
    let mut inverted = one_recording();
    inverted.min = Some(99);
    inverted.max = Some(7);
    let error = Histogram::decode(&inverted).unwrap_err();
    assert!(error.to_string().contains("above maximum"), "{error}");
}
