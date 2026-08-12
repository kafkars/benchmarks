/* Standalone canonical schedule emitter for cross-adapter conformance. */
#include "benchmark.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>

static int parse_positive(const char *value, uint64_t *target) {
        char *end = NULL;
        unsigned long long parsed;

        errno  = 0;
        parsed = strtoull(value, &end, 10);
        if (errno != 0 || end == value || *end != '\0' || parsed == 0)
                return -1;
        *target = (uint64_t)parsed;
        return 0;
}

int main(int argc, char **argv) {
        uint64_t rate;
        uint64_t records;
        uint64_t batch_records;
        uint64_t callers;
        uint64_t batch_count;
        uint64_t index;

        if (argc != 5 || parse_positive(argv[1], &rate) ||
            parse_positive(argv[2], &records) ||
            parse_positive(argv[3], &batch_records) ||
            parse_positive(argv[4], &callers)) {
                fprintf(stderr,
                        "usage: %s offered-rate records batch-records callers\n",
                        argv[0]);
                return EXIT_FAILURE;
        }
        if (records > UINT64_MAX - (batch_records - 1U))
                return EXIT_FAILURE;
        batch_count = (records + batch_records - 1U) / batch_records;
        puts("batch_index,caller,first_sequence,count,intended_ns");
        for (index = 0; index < batch_count; ++index) {
                uint64_t first;
                uint64_t count;
                uint64_t intended;

                if (index > UINT64_MAX / batch_records)
                        return EXIT_FAILURE;
                first = index * batch_records;
                count = records - first;
                if (count > batch_records)
                        count = batch_records;
                if (bench_schedule_offset_ns(first + count - 1U, rate,
                                             &intended))
                        return EXIT_FAILURE;
                printf("%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64
                       ",%" PRIu64 "\n",
                       index, index % callers, first, count, intended);
        }
        return EXIT_SUCCESS;
}
