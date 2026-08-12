/* Standalone payload-vector emitter for cross-adapter conformance. */
#include "benchmark.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv) {
        unsigned long long sequence;
        unsigned long long size;
        char *end = NULL;
        char *payload;

        if (argc != 4) {
                fprintf(stderr, "usage: %s run-id sequence payload-bytes\n", argv[0]);
                return EXIT_FAILURE;
        }
        errno    = 0;
        sequence = strtoull(argv[2], &end, 10);
        if (errno != 0 || end == argv[2] || *end != '\0')
                return EXIT_FAILURE;
        errno = 0;
        size  = strtoull(argv[3], &end, 10);
        if (errno != 0 || end == argv[3] || *end != '\0' ||
            size < BENCH_MIN_PAYLOAD_BYTES || size > SIZE_MAX)
                return EXIT_FAILURE;
        payload = malloc((size_t)size);
        if (!payload)
                return EXIT_FAILURE;
        bench_payload(payload, (size_t)size, argv[1], (uint64_t)sequence);
        if (fwrite(payload, 1, (size_t)size, stdout) != (size_t)size ||
            fputc('\n', stdout) == EOF) {
                free(payload);
                return EXIT_FAILURE;
        }
        free(payload);
        return EXIT_SUCCESS;
}
