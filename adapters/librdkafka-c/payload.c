/* Deterministic payload construction shared by every raw C run. */
#include "benchmark.h"

#include <time.h>

static const char HEX[] = "0123456789abcdef";

uint64_t bench_now_ns(void) {
        struct timespec now;

        if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
                return 0;
        return ((uint64_t)now.tv_sec * UINT64_C(1000000000)) +
               (uint64_t)now.tv_nsec;
}

void bench_payload(char *target,
                   size_t size,
                   const char *run_id,
                   uint64_t sequence) {
        size_t index;

        target[0] = 'K';
        target[1] = 'F';
        target[2] = 'B';
        target[3] = '1';
        for (index = 0; index < BENCH_RUN_ID_BYTES; ++index)
                target[4 + index] = run_id[index];
        for (index = 0; index < 16U; ++index) {
                size_t shift       = (15U - index) * 4U;
                target[20 + index] = HEX[(sequence >> shift) & UINT64_C(0x0f)];
        }
        for (index = 36U; index < size; ++index) {
                uint64_t selector = UINT64_C(44) + (sequence * UINT64_C(17)) +
                                    ((index - 36U) * UINT64_C(13));
                target[index] = HEX[selector & UINT64_C(0x0f)];
        }
}
