/* Bounded in-memory capture of librdkafka's native statistics JSON. */
#include "benchmark.h"

#include <ctype.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *phase_name(bench_statistics_phase_t phase) {
        switch (phase) {
        case BENCH_STATISTICS_SETUP:
                return "setup";
        case BENCH_STATISTICS_WARMUP:
                return "warmup";
        case BENCH_STATISTICS_BASELINE:
                return "baseline";
        case BENCH_STATISTICS_MEASURED:
                return "measured";
        case BENCH_STATISTICS_FINAL:
                return "final";
        }
        return NULL;
}

int bench_statistics_open(bench_statistics_t *statistics, const char *path) {
        memset(statistics, 0, sizeof(*statistics));
        statistics->buffer = malloc(BENCH_STATISTICS_BUFFER_BYTES);
        if (!statistics->buffer) {
                perror("allocate librdkafka statistics buffer");
                return -1;
        }
        statistics->path     = path;
        statistics->capacity = BENCH_STATISTICS_BUFFER_BYTES;
        statistics->phase    = BENCH_STATISTICS_SETUP;
        return 0;
}

void bench_statistics_set_phase(bench_statistics_t *statistics,
                                bench_statistics_phase_t phase) {
        statistics->phase = phase;
}

int bench_statistics_capture(rd_kafka_t *producer,
                             bench_statistics_t *statistics,
                             bench_statistics_phase_t phase) {
        const uint64_t timeout_ns =
            (uint64_t)BENCH_STATISTICS_INTERVAL_MS * 3000000U;
        const uint64_t started = bench_now_ns();
        const size_t previous  = statistics->snapshots;

        if (started == 0)
                return -1;
        bench_statistics_set_phase(statistics, phase);
        while (statistics->snapshots == previous && !statistics->failed) {
                uint64_t now;

                (void)rd_kafka_poll(producer, 10);
                now = bench_now_ns();
                if (now == 0 || now < started || now - started >= timeout_ns) {
                        fprintf(stderr,
                                "librdkafka statistics callback did not arrive "
                                "inside the capture deadline\n");
                        return -1;
                }
        }
        return statistics->failed ? -1 : 0;
}

int bench_statistics_write(bench_statistics_t *statistics) {
        FILE *output;

        if (statistics->failed || statistics->snapshots < 2U) {
                fprintf(stderr, "librdkafka statistics evidence is incomplete\n");
                return -1;
        }
        output = fopen(statistics->path, "wb");
        if (!output) {
                perror("open librdkafka statistics output");
                return -1;
        }
        if (fwrite(statistics->buffer, 1U, statistics->length, output) !=
            statistics->length) {
                perror("write librdkafka statistics output");
                (void)fclose(output);
                return -1;
        }
        if (fclose(output) != 0) {
                perror("close librdkafka statistics output");
                return -1;
        }
        return 0;
}

void bench_statistics_destroy(bench_statistics_t *statistics) {
        free(statistics->buffer);
        memset(statistics, 0, sizeof(*statistics));
}

int bench_statistics_callback(rd_kafka_t *producer,
                              char *json,
                              size_t json_len,
                              void *opaque) {
        bench_statistics_t *statistics = opaque;
        char header[192];
        const char *phase;
        uint64_t captured;
        size_t header_len;
        size_t required;
        int written;

        (void)producer;
        if (!statistics || !json || statistics->failed)
                return 0;
        phase    = phase_name(statistics->phase);
        captured = bench_now_ns();
        while (json_len > 0U &&
               (json[json_len - 1U] == '\0' ||
                isspace((unsigned char)json[json_len - 1U])))
                json_len--;
        if (!phase || captured == 0 || json_len < 2U || json[0] != '{' ||
            json[json_len - 1U] != '}') {
                statistics->failed = 1;
                return 0;
        }
        written = snprintf(
            header, sizeof(header),
            "{\"schema\":\"kafkars.librdkafka-statistics.v1\","
            "\"phase\":\"%s\",\"captured_ns\":%" PRIu64
            ",\"statistics\":",
            phase, captured);
        if (written < 0 || (size_t)written >= sizeof(header)) {
                statistics->failed = 1;
                return 0;
        }
        header_len = (size_t)written;
        if (header_len > SIZE_MAX - json_len - 2U) {
                statistics->failed = 1;
                return 0;
        }
        required = header_len + json_len + 2U;
        if (statistics->length > statistics->capacity ||
            required > statistics->capacity - statistics->length) {
                statistics->failed = 1;
                return 0;
        }
        memcpy(statistics->buffer + statistics->length, header, header_len);
        statistics->length += header_len;
        memcpy(statistics->buffer + statistics->length, json, json_len);
        statistics->length += json_len;
        statistics->buffer[statistics->length++] = '}';
        statistics->buffer[statistics->length++] = '\n';
        statistics->snapshots++;
        return 0;
}
