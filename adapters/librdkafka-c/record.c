/* Kafka record key encoding shared by the raw producer adapter. */
#include "benchmark.h"

void bench_encode_key(uint64_t sequence, unsigned char key[8]) {
        size_t index;

        for (index = 0; index < 8U; ++index) {
                size_t shift = (7U - index) * 8U;
                key[index]   = (unsigned char)((sequence >> shift) & 0xffU);
        }
}
