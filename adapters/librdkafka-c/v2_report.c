/* The v2 measurement document, written where the caller asked for it. */
#if defined(__APPLE__)
#define _DARWIN_C_SOURCE 1
#elif !defined(_DEFAULT_SOURCE)
#define _DEFAULT_SOURCE 1
#endif

#include "benchmark.h"

#include <inttypes.h>
#include <stdio.h>
#include <sys/resource.h>
#include <sys/time.h>

/*
 * The bytes here are the whole interface to every reader, so the field order
 * is the one `crates/bench-schema/src/result_v2.rs` declares and the encoding
 * is compact with a single trailing newline. Nothing is derived that a reader
 * could derive better: the percentiles live in the histograms, and this file
 * never computes one.
 *
 * # What the declaration says, and why it says that
 *
 * `payload_construction` is `prebuilt-pool-per-offer-sequence` rather than plain
 * `prebuilt-pool`: the record buffers are allocated before the measured
 * interval starts and reused for every batch, but each record's bytes carry
 * its own sequence number, so they are written per offer — before
 * `call_start`, and therefore outside every interval this document reports.
 * Claiming the bytes existed before the run would be a claim a reader could
 * act on and this adapter could not honour.
 */

/* What the measured path actually did, stated for a reader deciding whether
   two runs are comparable. */
#define V2_PAYLOAD_CONSTRUCTION "prebuilt-pool-per-offer-sequence"
#define V2_OWNERSHIP "copy-in-reused-buffer"
#define V2_COMPLETION_MODE "delivery-callback"
#define V2_SERIALIZATION "excluded"

/* Bundle-relative name of the librdkafka statistics stream. */
#define V2_NATIVE_METRICS_PATH BENCH_V2_STATISTICS_FILE

/* Self-reported process resources, normalized to bytes and nanoseconds. */
static int process_resources(uint64_t *max_rss_bytes,
                             uint64_t *user_cpu_ns,
                             uint64_t *system_cpu_ns) {
        struct rusage usage;

        if (getrusage(RUSAGE_SELF, &usage) != 0)
                return -1;
#if defined(__APPLE__)
        /* Darwin reports a maximum resident set in bytes; Linux reports it in
           kibibytes, and this document is always in bytes. */
        *max_rss_bytes = (uint64_t)usage.ru_maxrss;
#else
        *max_rss_bytes = (uint64_t)usage.ru_maxrss * UINT64_C(1024);
#endif
        *user_cpu_ns =
            ((uint64_t)usage.ru_utime.tv_sec * UINT64_C(1000000000)) +
            ((uint64_t)usage.ru_utime.tv_usec * UINT64_C(1000));
        *system_cpu_ns =
            ((uint64_t)usage.ru_stime.tv_sec * UINT64_C(1000000000)) +
            ((uint64_t)usage.ru_stime.tv_usec * UINT64_C(1000));
        return 0;
}

/*
 * Why this measurement is not the one the experiment asked for, or NULL when
 * it is. The order is the order a reader would want to hear it in: what went
 * wrong first.
 */
static const char *invalid_reason(const bench_config_t *config,
                                  const bench_v2_phase_t *phase) {
        if (phase->fatal)
                return "the measured phase failed before it finished offering";
        if (phase->offered != (uint64_t)config->records)
                return "the adapter did not offer every record asked for";
        if (phase->accepted != phase->offered)
                return "the client did not accept every offered record";
        if (phase->unknown != 0)
                return "accepted records had no terminal by the drain deadline";
        if (phase->timed_out != 0)
                return "accepted records reached a timeout terminal";
        if (phase->failed != 0)
                return "accepted records reached a failure terminal";
        if (phase->acknowledged != (uint64_t)config->records)
                return "the broker did not acknowledge every record";
        return NULL;
}

static int write_declared(FILE *output) {
        return fprintf(output,
                       "\"declared\":{\"payload_construction\":\"%s\","
                       "\"ownership\":\"%s\",\"completion_mode\":\"%s\","
                       "\"serialization\":\"%s\"},",
                       V2_PAYLOAD_CONSTRUCTION, V2_OWNERSHIP,
                       V2_COMPLETION_MODE, V2_SERIALIZATION) < 0
                   ? -1
                   : 0;
}

static int write_outcomes(FILE *output, const bench_v2_phase_t *phase) {
        return fprintf(output,
                       "\"outcomes\":{\"offered\":%" PRIu64
                       ",\"accepted\":%" PRIu64 ",\"acknowledged\":%" PRIu64
                       ",\"failed\":%" PRIu64 ",\"timed_out\":%" PRIu64
                       ",\"unknown\":%" PRIu64 "},",
                       phase->offered, phase->accepted, phase->acknowledged,
                       phase->failed, phase->timed_out, phase->unknown) < 0
                   ? -1
                   : 0;
}

static int write_timing(FILE *output,
                        const bench_config_t *config,
                        const bench_v2_phase_t *phase) {
        if (fputs("\"timing\":{\"clock\":\"monotonic-ns\","
                  "\"intended_to_terminal\":",
                  output) == EOF ||
            bench_histogram_write(&phase->intended_to_terminal, output) != 0 ||
            fputs(",\"accepted_to_terminal\":", output) == EOF ||
            bench_histogram_write(&phase->accepted_to_terminal, output) != 0 ||
            fputs(",\"call_start_to_accepted\":", output) == EOF ||
            bench_histogram_write(&phase->call_start_to_accepted, output) != 0)
                return -1;
        /* Scheduler lateness exists exactly when a schedule did. */
        if (config->fixed_rate) {
                if (fputs(",\"intended_to_call_start\":", output) == EOF ||
                    bench_histogram_write(&phase->intended_to_call_start,
                                          output) != 0)
                        return -1;
        }
        return fputs("},", output) == EOF ? -1 : 0;
}

static int write_throughput(FILE *output,
                            const bench_config_t *config,
                            const bench_v2_phase_t *phase) {
        uint64_t duration_ns  = phase->last_terminal_ns;
        uint64_t payload_total = phase->acknowledged *
                                 (uint64_t)config->payload_bytes;
        double records_per_second = 0.0;
        double bytes_per_second   = 0.0;

        if (duration_ns > 0) {
                double seconds = (double)duration_ns / 1000000000.0;

                records_per_second = (double)phase->acknowledged / seconds;
                bytes_per_second   = (double)payload_total / seconds;
        }
        return fprintf(output,
                       "\"throughput\":{\"measured_duration_ns\":%" PRIu64
                       ",\"acknowledged_records_per_second\":%.6f,"
                       "\"acknowledged_payload_bytes_per_second\":%.6f},",
                       duration_ns, records_per_second, bytes_per_second) < 0
                   ? -1
                   : 0;
}

static int write_resources(FILE *output) {
        uint64_t max_rss_bytes = 0;
        uint64_t user_cpu_ns   = 0;
        uint64_t system_cpu_ns = 0;

        if (process_resources(&max_rss_bytes, &user_cpu_ns, &system_cpu_ns) !=
            0)
                return 0;
        return fprintf(output,
                       "\"resources\":{\"max_rss_bytes\":%" PRIu64
                       ",\"user_cpu_ns\":%" PRIu64
                       ",\"system_cpu_ns\":%" PRIu64 "},",
                       max_rss_bytes, user_cpu_ns, system_cpu_ns) < 0
                   ? -1
                   : 0;
}

int bench_write_v2_report(const bench_config_t *config,
                          const bench_v2_phase_t *phase,
                          const char *adapter_version) {
        const char *reason = invalid_reason(config, phase);
        FILE *output       = fopen(config->v2_result_path, "wb");
        int failed         = 0;

        if (!output) {
                perror("open v2 result output");
                return -1;
        }
        failed |= fprintf(output,
                          "{\"schema\":\"kafkars.producer-benchmark.v2\","
                          "\"adapter\":\"librdkafka-c\","
                          "\"adapter_version\":\"%s\",\"run_id\":\"%s\","
                          "\"load_mode\":\"%s\",",
                          adapter_version, config->run_id,
                          config->fixed_rate ? "scheduled-open-loop-fixed-rate"
                                             : "closed-loop") < 0;
        failed |= write_declared(output) != 0;
        failed |= write_outcomes(output, phase) != 0;
        failed |= write_timing(output, config, phase) != 0;
        failed |= write_throughput(output, config, phase) != 0;
        failed |= fprintf(output,
                          "\"queue\":{\"max_outstanding_observed\":%" PRIu64
                          ",\"final_outstanding\":%" PRIu64 "},",
                          phase->max_outstanding_observed,
                          phase->outstanding) < 0;
        failed |= write_resources(output) != 0;
        failed |= fprintf(output, "\"native_metrics_path\":\"%s\",\"valid\":%s",
                          V2_NATIVE_METRICS_PATH,
                          reason ? "false" : "true") < 0;
        if (reason)
                failed |= fprintf(output, ",\"invalid_reason\":\"%s\"",
                                  reason) < 0;
        failed |= fputs("}\n", output) == EOF;
        if (fclose(output) != 0) {
                perror("close v2 result output");
                return -1;
        }
        if (failed) {
                fprintf(stderr, "write the v2 result document\n");
                return -1;
        }
        return reason ? -1 : 0;
}
