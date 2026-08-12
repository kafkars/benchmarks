/* Strict positional configuration parsing for the raw C adapter. */
#include "benchmark.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int parse_size(const char *name,
                      const char *value,
                      int allow_zero,
                      size_t *target) {
        char *end = NULL;
        unsigned long long parsed;

        errno  = 0;
        parsed = strtoull(value, &end, 10);
        if (errno != 0 || end == value || *end != '\0' ||
            (!allow_zero && parsed == 0) || parsed > SIZE_MAX) {
                fprintf(stderr, "invalid %s: %s\n", name, value);
                return -1;
        }
        *target = (size_t)parsed;
        return 0;
}

static int valid_run_id(const char *value) {
        size_t index;

        if (strlen(value) != BENCH_RUN_ID_BYTES)
                return 0;
        for (index = 0; index < BENCH_RUN_ID_BYTES; ++index) {
                if (!((value[index] >= '0' && value[index] <= '9') ||
                      (value[index] >= 'a' && value[index] <= 'f')))
                        return 0;
        }
        return 1;
}

static int valid_text(const char *value) {
        const unsigned char *cursor = (const unsigned char *)value;

        if (*cursor == '\0')
                return 0;
        for (; *cursor != '\0'; ++cursor) {
                if (*cursor < 0x20 || *cursor == '"' || *cursor == '\\')
                        return 0;
        }
        return 1;
}

int bench_parse_config(int argc, char **argv, bench_config_t *config) {
        size_t partitions;

        if (argc != 12) {
                fprintf(stderr,
                        "usage: %s bootstrap warmup-topic topic run-id "
                        "warmup-records records payload-bytes partitions "
                        "max-outstanding latency.csv client-metrics.jsonl\n",
                        argv[0]);
                return -1;
        }
        if (!valid_text(argv[1]) || !valid_text(argv[2]) ||
            !valid_text(argv[3]) || !valid_run_id(argv[4]) ||
            !valid_text(argv[10]) || !valid_text(argv[11])) {
                fprintf(stderr, "invalid text or run ID argument\n");
                return -1;
        }
        config->bootstrap       = argv[1];
        config->warmup_topic    = argv[2];
        config->topic           = argv[3];
        config->run_id          = argv[4];
        config->latency_path    = argv[10];
        config->statistics_path = argv[11];
        config->offered_records_per_second = 0;
        config->callers                     = 1U;
        config->fixed_rate                  = 0;
        if (parse_size("warmup records", argv[5], 1, &config->warmup_records) ||
            parse_size("records", argv[6], 0, &config->records) ||
            parse_size("payload bytes", argv[7], 0, &config->payload_bytes) ||
            parse_size("partitions", argv[8], 0, &partitions) ||
            parse_size("max outstanding", argv[9], 0,
                       &config->max_outstanding))
                return -1;
        if (config->payload_bytes < BENCH_MIN_PAYLOAD_BYTES ||
            partitions > INT32_MAX ||
            config->max_outstanding > BENCH_CLIENT_RECORD_CAPACITY) {
                fprintf(stderr, "workload exceeds the adapter's declared bounds\n");
                return -1;
        }
        config->partitions = (int32_t)partitions;
        return 0;
}

int bench_parse_fixed_config(int argc, char **argv, bench_config_t *config) {
        char *base[12];
        size_t offered_rate;
        size_t callers;

        if (argc != 15 || strcmp(argv[1], "--fixed-rate") != 0) {
                fprintf(stderr,
                        "usage: %s --fixed-rate bootstrap warmup-topic topic "
                        "run-id warmup-records records payload-bytes partitions "
                        "max-outstanding offered-rate callers latency.csv "
                        "client-metrics.jsonl\n",
                        argv[0]);
                return -1;
        }
        base[0] = argv[0];
        for (size_t index = 1; index <= 9U; ++index)
                base[index] = argv[index + 1U];
        base[10] = argv[13];
        base[11] = argv[14];
        if (bench_parse_config(12, base, config) != 0 ||
            parse_size("offered rate", argv[11], 0, &offered_rate) ||
            parse_size("callers", argv[12], 0, &callers))
                return -1;
        if (offered_rate > 1000000000U || callers != 4U) {
                fprintf(stderr, "fixed-load headline requires rate <= 1e9 and four callers\n");
                return -1;
        }
        config->offered_records_per_second = (uint64_t)offered_rate;
        config->callers                     = callers;
        config->fixed_rate                  = 1;
        return 0;
}
