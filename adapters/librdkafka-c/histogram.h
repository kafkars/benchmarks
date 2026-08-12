/* Bounded log-linear latency histogram, byte-identical to the Rust schema. */
#ifndef KAFKARS_HISTOGRAM_H
#define KAFKARS_HISTOGRAM_H

#include <stdint.h>
#include <stdio.h>

/*
 * This header is deliberately free of every other dependency in this adapter,
 * including <rdkafka.h>: the same translation unit backs the measured path and
 * the standalone conformance vector, and the vector has to build before the
 * librdkafka bootstrap has produced anything to link against.
 *
 * The layout is `crates/bench-schema/src/histogram.rs` restated in C. Values
 * below 128 get their own exact bucket; larger values are brought into
 * [128, 256) by k halvings and land in bucket (k + 1) << 7 | ((value >> k) -
 * 128). `min`, `max`, and `sum` are tracked exactly, outside the buckets, and
 * `sum` saturates exactly where the Rust `saturating_add` does.
 */

/* Layout discriminator every encoded histogram carries. */
#define BENCH_HISTOGRAM_LAYOUT "kafkars.log-linear.v1"

/* Linear sub-buckets per power of two, as a bit count. */
#define BENCH_HISTOGRAM_SUB_BUCKET_BITS 7U

/* Linear sub-buckets per power of two. */
#define BENCH_HISTOGRAM_SUB_BUCKET_COUNT \
        (UINT64_C(1) << BENCH_HISTOGRAM_SUB_BUCKET_BITS)

/*
 * Every index the layout can reach: 128 exact buckets, then 57 scales
 * (k = 0..56, the widest a 64-bit value reaches) of 128 sub-buckets each, so
 * the largest index is (56 + 1) << 7 | 127 = 7423.
 */
#define BENCH_HISTOGRAM_BUCKETS 7424U

typedef struct bench_histogram_s {
        uint64_t counts[BENCH_HISTOGRAM_BUCKETS];
        uint64_t total;
        uint64_t min;
        uint64_t max;
        uint64_t sum;
} bench_histogram_t;

/* Empties a histogram; required before the first record. */
void bench_histogram_reset(bench_histogram_t *histogram);

/* Returns the bucket index a value falls in. */
uint32_t bench_histogram_bucket_index(uint64_t value);

/* Records one value. */
void bench_histogram_record(bench_histogram_t *histogram, uint64_t value);

/*
 * Writes the embedded JSON object with no surrounding whitespace and no
 * trailing newline, byte-identical to compact serde output of the Rust
 * `EncodedHistogram`. Returns 0 on success and -1 when the stream refused a
 * write.
 */
int bench_histogram_write(const bench_histogram_t *histogram, FILE *output);

#endif
