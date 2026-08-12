/* Exact integer nanosecond offsets for the canonical open-loop schedule. */
#include "benchmark.h"

#include <limits.h>

int bench_schedule_offset_ns(uint64_t sequence,
                             uint64_t rate,
                             uint64_t *offset_ns) {
        const uint64_t nanos_per_second = 1000000000U;
        uint64_t seconds;
        uint64_t remainder;
        uint64_t whole;
        uint64_t fraction;

        if (!offset_ns || rate == 0 || rate > nanos_per_second)
                return -1;
        seconds   = sequence / rate;
        remainder = sequence % rate;
        if (seconds > UINT64_MAX / nanos_per_second)
                return -1;
        whole = seconds * nanos_per_second;
        if (remainder > UINT64_MAX / nanos_per_second)
                return -1;
        fraction = (remainder * nanos_per_second) / rate;
        if (whole > UINT64_MAX - fraction)
                return -1;
        *offset_ns = whole + fraction;
        return 0;
}
