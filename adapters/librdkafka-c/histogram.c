/* Log-linear bucketing and the byte-exact encoding of one histogram. */
#include "histogram.h"

#include <inttypes.h>
#include <string.h>

/*
 * The number of significant bits in a value, without a compiler builtin: the
 * encoding is a cross-language contract, so it is spelled in portable C rather
 * than delegated to an intrinsic whose availability varies by toolchain.
 */
static uint32_t bit_length(uint64_t value) {
        uint32_t length = 0;

        if (value >> 32U) {
                length += 32U;
                value >>= 32U;
        }
        if (value >> 16U) {
                length += 16U;
                value >>= 16U;
        }
        if (value >> 8U) {
                length += 8U;
                value >>= 8U;
        }
        if (value >> 4U) {
                length += 4U;
                value >>= 4U;
        }
        if (value >> 2U) {
                length += 2U;
                value >>= 2U;
        }
        if (value >> 1U) {
                length += 1U;
                value >>= 1U;
        }
        return length + (uint32_t)(value & UINT64_C(1));
}

/* Addition that stops at the maximum, matching Rust's `saturating_add`. */
static uint64_t saturating_add(uint64_t left, uint64_t right) {
        return left > UINT64_MAX - right ? UINT64_MAX : left + right;
}

void bench_histogram_reset(bench_histogram_t *histogram) {
        memset(histogram, 0, sizeof(*histogram));
}

uint32_t bench_histogram_bucket_index(uint64_t value) {
        uint32_t scale;
        uint32_t sub;

        if (value < BENCH_HISTOGRAM_SUB_BUCKET_COUNT)
                return (uint32_t)value;
        scale = bit_length(value) - 1U - BENCH_HISTOGRAM_SUB_BUCKET_BITS;
        sub   = (uint32_t)((value >> scale) - BENCH_HISTOGRAM_SUB_BUCKET_COUNT);
        return ((scale + 1U) << BENCH_HISTOGRAM_SUB_BUCKET_BITS) | sub;
}

void bench_histogram_record(bench_histogram_t *histogram, uint64_t value) {
        uint32_t index = bench_histogram_bucket_index(value);

        histogram->counts[index]++;
        if (histogram->total == 0 || value < histogram->min)
                histogram->min = value;
        if (histogram->total == 0 || value > histogram->max)
                histogram->max = value;
        histogram->total = saturating_add(histogram->total, UINT64_C(1));
        histogram->sum   = saturating_add(histogram->sum, value);
}

int bench_histogram_write(const bench_histogram_t *histogram, FILE *output) {
        uint32_t index;
        int written = 0;

        if (fprintf(output,
                    "{\"layout\":\"%s\",\"unit\":\"ns\","
                    "\"sub_bucket_bits\":%u,\"total\":%" PRIu64 ",",
                    BENCH_HISTOGRAM_LAYOUT, BENCH_HISTOGRAM_SUB_BUCKET_BITS,
                    histogram->total) < 0)
                return -1;
        if (histogram->total == 0) {
                if (fputs("\"min\":null,\"max\":null,", output) == EOF)
                        return -1;
        } else if (fprintf(output,
                           "\"min\":%" PRIu64 ",\"max\":%" PRIu64 ",",
                           histogram->min, histogram->max) < 0) {
                return -1;
        }
        if (fprintf(output, "\"sum\":%" PRIu64 ",\"counts\":[",
                    histogram->sum) < 0)
                return -1;
        for (index = 0; index < BENCH_HISTOGRAM_BUCKETS; ++index) {
                if (histogram->counts[index] == 0)
                        continue;
                if (fprintf(output, "%s[%" PRIu32 ",%" PRIu64 "]",
                            written ? "," : "", index,
                            histogram->counts[index]) < 0)
                        return -1;
                written = 1;
        }
        return fputs("]}", output) == EOF ? -1 : 0;
}
