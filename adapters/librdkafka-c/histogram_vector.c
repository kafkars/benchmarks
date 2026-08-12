/* Standalone histogram encoder for cross-language conformance vectors. */
#include "histogram.h"

#include <stdio.h>
#include <stdlib.h>

/*
 * Reads whitespace-separated unsigned decimal values from standard input,
 * records each one, and prints the encoded histogram followed by exactly one
 * newline. Empty input is a legitimate vector: it prints the empty histogram,
 * whose `min` and `max` are null.
 *
 * The token scanner is hand-rolled rather than `scanf("%" SCNu64)` because
 * that conversion silently accepts a leading sign and wraps a negative value
 * into the u64 space, which would turn a malformed vector into a passing one.
 */

static int is_separator(int character) {
        return character == ' ' || character == '\t' || character == '\n' ||
               character == '\r' || character == '\v' || character == '\f';
}

int main(int argc, char **argv) {
        bench_histogram_t histogram;
        int character;

        if (argc != 1) {
                fprintf(stderr,
                        "usage: %s < whitespace-separated-u64-values\n",
                        argv[0]);
                return EXIT_FAILURE;
        }
        bench_histogram_reset(&histogram);
        for (;;) {
                uint64_t value = 0;

                while ((character = getchar()) != EOF && is_separator(character))
                        ;
                if (character == EOF)
                        break;
                for (; character != EOF && !is_separator(character);
                     character = getchar()) {
                        uint64_t digit;

                        if (character < '0' || character > '9') {
                                fprintf(stderr,
                                        "not an unsigned decimal value: %c\n",
                                        character);
                                return EXIT_FAILURE;
                        }
                        digit = (uint64_t)(character - '0');
                        if (value > UINT64_MAX / UINT64_C(10) ||
                            value * UINT64_C(10) > UINT64_MAX - digit) {
                                fprintf(stderr,
                                        "value exceeds the 64-bit range\n");
                                return EXIT_FAILURE;
                        }
                        value = (value * UINT64_C(10)) + digit;
                }
                bench_histogram_record(&histogram, value);
        }
        if (ferror(stdin)) {
                fprintf(stderr, "read standard input\n");
                return EXIT_FAILURE;
        }
        if (bench_histogram_write(&histogram, stdout) != 0 ||
            fputc('\n', stdout) == EOF || fflush(stdout) != 0) {
                fprintf(stderr, "write the encoded histogram\n");
                return EXIT_FAILURE;
        }
        return EXIT_SUCCESS;
}
