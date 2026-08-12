/* Independent broker-visible verifier for both producer adapters. */
#include "benchmark.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define VERIFY_TIMEOUT_NS UINT64_C(60000000000)

static int parse_size(const char *value, size_t *target) {
        char *end = NULL;
        unsigned long long parsed;

        errno  = 0;
        parsed = strtoull(value, &end, 10);
        if (errno != 0 || end == value || *end != '\0' || parsed > SIZE_MAX)
                return -1;
        *target = (size_t)parsed;
        return 0;
}

static int parse_sequence(const unsigned char *value, uint64_t *sequence) {
        size_t index;
        uint64_t parsed = 0;

        for (index = 20U; index < 36U; ++index) {
                unsigned char digit = value[index];
                uint64_t nibble;
                if (digit >= '0' && digit <= '9')
                        nibble = digit - '0';
                else if (digit >= 'a' && digit <= 'f')
                        nibble = digit - 'a' + 10U;
                else
                        return -1;
                parsed = (parsed << 4U) | nibble;
        }
        *sequence = parsed;
        return 0;
}

int main(int argc, char **argv) {
        const char *bootstrap;
        const char *topic;
        const char *run_id;
        size_t records;
        size_t payload_bytes;
        size_t partition_count;
        int32_t partitions;
        rd_kafka_t *consumer = NULL;
        unsigned char *expected = NULL;
        unsigned char *seen = NULL;
        unsigned char *eof = NULL;
        uint64_t *last_sequence = NULL;
        size_t verified = 0;
        size_t duplicates = 0;
        size_t corrupt = 0;
        size_t unexpected = 0;
        size_t eof_count = 0;
        size_t missing;
        uint64_t deadline;
        int result = EXIT_FAILURE;

        if (argc != 7 || parse_size(argv[4], &records) ||
            parse_size(argv[5], &payload_bytes) ||
            parse_size(argv[6], &partition_count) ||
            payload_bytes < BENCH_MIN_PAYLOAD_BYTES || partition_count == 0 ||
            partition_count > INT32_MAX) {
                fprintf(stderr,
                        "usage: %s bootstrap topic run-id records "
                        "payload-bytes partitions\n",
                        argv[0]);
                return EXIT_FAILURE;
        }
        bootstrap = argv[1];
        topic = argv[2];
        run_id = argv[3];
        partitions = (int32_t)partition_count;
        consumer = bench_create_verifier(bootstrap, run_id);
        expected = malloc(payload_bytes);
        seen = calloc(records, sizeof(*seen));
        eof = calloc(partition_count, sizeof(*eof));
        last_sequence = malloc(partition_count * sizeof(*last_sequence));
        if (!consumer || !expected || (records > 0 && !seen) || !eof ||
            !last_sequence)
                goto cleanup;
        for (size_t index = 0; index < partition_count; ++index)
                last_sequence[index] = UINT64_MAX;
        if (bench_assign_beginning(consumer, topic, partitions) != 0)
                goto cleanup;

        deadline = bench_now_ns() + VERIFY_TIMEOUT_NS;
        while ((verified < records || eof_count < partition_count) &&
               bench_now_ns() < deadline) {
                rd_kafka_message_t *message =
                    rd_kafka_consumer_poll(consumer, 100);
                uint64_t sequence;
                size_t index;
                int32_t expected_partition;
                if (!message)
                        continue;
                if (message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                        if (message->partition >= 0 &&
                            message->partition < partitions &&
                            !eof[message->partition]) {
                                eof[message->partition] = 1;
                                eof_count++;
                        }
                        rd_kafka_message_destroy(message);
                        continue;
                }
                if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                        fprintf(stderr, "verifier consume failed: %s\n",
                                rd_kafka_message_errstr(message));
                        rd_kafka_message_destroy(message);
                        goto cleanup;
                }
                if (message->partition >= 0 &&
                    message->partition < partitions &&
                    eof[message->partition]) {
                        eof[message->partition] = 0;
                        eof_count--;
                }
                if (message->len != payload_bytes ||
                    memcmp(message->payload, "KFB1", 4) != 0 ||
                    memcmp((unsigned char *)message->payload + 4, run_id,
                           BENCH_RUN_ID_BYTES) != 0 ||
                    parse_sequence(message->payload, &sequence) != 0) {
                        corrupt++;
                        rd_kafka_message_destroy(message);
                        continue;
                }
                if (sequence >= records) {
                        unexpected++;
                        rd_kafka_message_destroy(message);
                        continue;
                }
                bench_payload((char *)expected, payload_bytes, run_id, sequence);
                if (memcmp(message->payload, expected, payload_bytes) != 0) {
                        corrupt++;
                        rd_kafka_message_destroy(message);
                        continue;
                }
                index = (size_t)sequence;
                expected_partition = (int32_t)(index % partition_count);
                if (message->partition != expected_partition ||
                    (last_sequence[message->partition] != UINT64_MAX &&
                     last_sequence[message->partition] >= sequence)) {
                        unexpected++;
                } else if (seen[index]) {
                        duplicates++;
                } else {
                        seen[index] = 1;
                        last_sequence[message->partition] = sequence;
                        verified++;
                }
                rd_kafka_message_destroy(message);
        }
        missing = records - verified;
        if (missing > 0) {
                size_t reported = 0;
                fprintf(stderr, "missing sequences:");
                for (size_t index = 0; index < records && reported < 64U;
                     ++index) {
                        if (!seen[index]) {
                                fprintf(stderr, " %zu", index);
                                reported++;
                        }
                }
                fprintf(stderr, "\n");
        }
        printf("{\"schema\":\"kafkars.producer-verification.v1\","
               "\"topic\":\"%s\",\"expected_records\":%zu,"
               "\"verified_records\":%zu,\"duplicates\":%zu,"
               "\"missing_records\":%zu,\"corrupt\":%zu,"
               "\"unexpected\":%zu,\"eof_partitions\":%zu,\"valid\":%s}\n",
               topic, records, verified, duplicates, missing, corrupt, unexpected,
               eof_count,
               verified == records && duplicates == 0 && corrupt == 0 &&
                       unexpected == 0 && eof_count == partition_count
                   ? "true"
                   : "false");
        if (verified == records && duplicates == 0 && corrupt == 0 &&
            unexpected == 0 && eof_count == partition_count)
                result = EXIT_SUCCESS;

cleanup:
        if (consumer) {
                (void)rd_kafka_consumer_close(consumer);
                rd_kafka_destroy(consumer);
        }
        free(expected);
        free(seen);
        free(eof);
        free(last_sequence);
        return result;
}
