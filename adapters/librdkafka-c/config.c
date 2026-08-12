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

/*
 * The nine workload arguments every shape carries, in the one order they are
 * ever written, plus the evidence paths that follow them. `latency` is NULL on
 * the v2 path, which writes no per-record file; every other check is shared,
 * so a bound can never hold on one shape and not another.
 */
static int parse_workload(char **workload,
                          const char *latency,
                          const char *statistics,
                          bench_config_t *config) {
        size_t partitions;

        if (!valid_text(workload[0]) || !valid_text(workload[1]) ||
            !valid_text(workload[2]) || !valid_run_id(workload[3]) ||
            (latency && !valid_text(latency)) || !valid_text(statistics)) {
                fprintf(stderr, "invalid text or run ID argument\n");
                return -1;
        }
        config->bootstrap       = workload[0];
        config->warmup_topic    = workload[1];
        config->topic           = workload[2];
        config->run_id          = workload[3];
        config->latency_path    = latency;
        config->statistics_path = statistics;
        config->offered_records_per_second = 0;
        config->callers                     = 1U;
        config->fixed_rate                  = 0;
        config->v2_output                   = NULL;
        if (parse_size("warmup records", workload[4], 1,
                       &config->warmup_records) ||
            parse_size("records", workload[5], 0, &config->records) ||
            parse_size("payload bytes", workload[6], 0,
                       &config->payload_bytes) ||
            parse_size("partitions", workload[7], 0, &partitions) ||
            parse_size("max outstanding", workload[8], 0,
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

/* The two arguments the fixed-rate shapes add, with their shared bounds. */
static int parse_fixed_rate(const char *rate_text,
                            const char *callers_text,
                            bench_config_t *config) {
        size_t offered_rate;
        size_t callers;

        if (parse_size("offered rate", rate_text, 0, &offered_rate) ||
            parse_size("callers", callers_text, 0, &callers))
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

/*
 * The v2 output flag leads its own argv shape rather than trailing a legacy
 * one, so it can never be mistaken for a strictly positional argument. Seeing
 * it inside a legacy vector means the caller mixed the two evidence contracts,
 * which is refused rather than silently resolved in favour of one of them.
 */
static int mentions_v2_output(int argc, char **argv) {
        int index;

        for (index = 1; index < argc; ++index) {
                if (strcmp(argv[index], "--v2-output") == 0) {
                        fprintf(stderr,
                                "--v2-output cannot be combined with the v1 "
                                "latency.csv and client-metrics.jsonl "
                                "positional arguments\n");
                        return 1;
                }
        }
        return 0;
}

/*
 * `--fixed-rate` names a whole argv shape rather than a value, so it may only
 * appear at the one position that shape puts it in. Anywhere inside the
 * twelve-argument closed-loop vector it means the caller wrote the fifteen
 * argument shape and lost arguments on the way — and the positional parser
 * would otherwise install the flag itself as the bootstrap string and run a
 * closed-loop measurement against a broker address nobody named.
 */
static int mentions_fixed_rate(int argc, char **argv) {
        int index;

        for (index = 1; index < argc; ++index) {
                if (strcmp(argv[index], "--fixed-rate") == 0) {
                        fprintf(stderr,
                                "--fixed-rate must directly follow the "
                                "--v2-output directory and be followed by an "
                                "offered rate and a caller count\n");
                        return 1;
                }
        }
        return 0;
}

/* Renders `<directory>/<leaf>` into a bounded buffer. */
static int join_path(char *target,
                     size_t capacity,
                     const char *directory,
                     const char *leaf) {
        int written = snprintf(target, capacity, "%s/%s", directory, leaf);

        if (written < 0 || (size_t)written >= capacity) {
                fprintf(stderr, "v2 output path is too long: %s/%s\n",
                        directory, leaf);
                return -1;
        }
        return 0;
}

int bench_parse_config(int argc, char **argv, bench_config_t *config) {
        if (argc != 12) {
                fprintf(stderr,
                        "usage: %s bootstrap warmup-topic topic run-id "
                        "warmup-records records payload-bytes partitions "
                        "max-outstanding latency.csv client-metrics.jsonl\n",
                        argv[0]);
                return -1;
        }
        if (mentions_v2_output(argc, argv))
                return -1;
        return parse_workload(&argv[1], argv[10], argv[11], config);
}

int bench_parse_fixed_config(int argc, char **argv, bench_config_t *config) {
        if (argc != 15 || strcmp(argv[1], "--fixed-rate") != 0) {
                fprintf(stderr,
                        "usage: %s --fixed-rate bootstrap warmup-topic topic "
                        "run-id warmup-records records payload-bytes partitions "
                        "max-outstanding offered-rate callers latency.csv "
                        "client-metrics.jsonl\n",
                        argv[0]);
                return -1;
        }
        if (mentions_v2_output(argc, argv))
                return -1;
        if (parse_workload(&argv[2], argv[13], argv[14], config) != 0)
                return -1;
        return parse_fixed_rate(argv[11], argv[12], config);
}

int bench_parse_v2_config(int argc, char **argv, bench_config_t *config) {
        int fixed = argc == 15 && strcmp(argv[3], "--fixed-rate") == 0;

        if (argc < 3 || strcmp(argv[1], "--v2-output") != 0 ||
            (!fixed && argc != 12)) {
                fprintf(stderr,
                        "usage: %s --v2-output dir [--fixed-rate] bootstrap "
                        "warmup-topic topic run-id warmup-records records "
                        "payload-bytes partitions max-outstanding "
                        "[offered-rate callers]\n",
                        argv[0]);
                return -1;
        }
        if (!valid_text(argv[2])) {
                fprintf(stderr, "invalid v2 output directory\n");
                return -1;
        }
        if (!fixed && mentions_fixed_rate(argc, argv))
                return -1;
        /* Both side files are derived from the one directory the caller names,
           so the v2 shape has no path arguments of its own to disagree with. */
        if (join_path(config->v2_statistics_path,
                      sizeof(config->v2_statistics_path), argv[2],
                      BENCH_V2_STATISTICS_FILE) ||
            join_path(config->v2_result_path, sizeof(config->v2_result_path),
                      argv[2], BENCH_V2_RESULT_FILE))
                return -1;
        if (parse_workload(&argv[fixed ? 4 : 3], NULL,
                           config->v2_statistics_path, config) != 0)
                return -1;
        if (fixed && parse_fixed_rate(argv[13], argv[14], config) != 0)
                return -1;
        /* The closed-loop v2 phase is written around exactly one caller: its
           submitter numbers batches from zero, so a second caller would claim
           submission index zero as well and collide on the order barrier.
           `parse_workload` fixes the count at one; this is that assumption
           stated where the argument vector is decided rather than only where
           the phase runs. */
        if (!fixed && config->callers != 1) {
                fprintf(stderr,
                        "closed-loop v2 admits from exactly one caller\n");
                return -1;
        }
        config->v2_output = argv[2];
        return 0;
}
