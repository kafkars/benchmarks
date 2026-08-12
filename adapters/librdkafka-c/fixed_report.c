/* Stable fixed-load JSON summary and corrected latency evidence. */
#include "benchmark.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>

static int compare_u64(const void *left, const void *right) {
        uint64_t lhs = *(const uint64_t *)left;
        uint64_t rhs = *(const uint64_t *)right;
        return (lhs > rhs) - (lhs < rhs);
}

static uint64_t percentile(uint64_t *values,
                           size_t count,
                           size_t per_thousand) {
        size_t rank;

        qsort(values, count, sizeof(*values), compare_u64);
        rank = ((count * per_thousand) + 999U) / 1000U;
        if (rank == 0)
                rank = 1;
        if (rank > count)
                rank = count;
        return values[rank - 1U];
}

static int write_latencies(const bench_config_t *config,
                           const bench_phase_t *phase) {
        FILE *output = fopen(config->latency_path, "w");

        if (!output)
                return -1;
        fprintf(output,
                "sequence,intended_ns,admitted_ns,completed_ns,"
                "uncorrected_latency_ns,corrected_latency_ns\n");
        for (size_t sequence = 0; sequence < phase->accepted; ++sequence) {
                fprintf(output,
                        "%zu,%" PRIu64 ",%" PRIu64 ",%" PRIu64
                        ",%" PRIu64 ",%" PRIu64 "\n",
                        sequence, phase->intended_ns[sequence],
                        phase->admitted_ns[sequence],
                        phase->completed_ns[sequence],
                        phase->latency_ns[sequence],
                        phase->corrected_latency_ns[sequence]);
        }
        return fclose(output) == 0 ? 0 : -1;
}

static void print_latency(const char *name,
                          uint64_t *values,
                          size_t count) {
        printf("\"%s\":{\"p50\":%" PRIu64 ",\"p95\":%" PRIu64
               ",\"p99\":%" PRIu64 ",\"p999\":%" PRIu64
               ",\"max\":%" PRIu64 "}",
               name, percentile(values, count, 500),
               percentile(values, count, 950),
               percentile(values, count, 990),
               percentile(values, count, 999),
               percentile(values, count, 1000));
}

int bench_write_fixed_report(const bench_config_t *config,
                             const bench_phase_t *phase,
                             const char *adapter_version) {
        uint64_t schedule_span;
        uint64_t payload_total;
        int valid;

        if (write_latencies(config, phase) ||
            bench_schedule_offset_ns((uint64_t)(config->records - 1U),
                                     config->offered_records_per_second,
                                     &schedule_span))
                return -1;
        payload_total = (uint64_t)phase->acknowledged * config->payload_bytes;
        valid = phase->accepted == config->records &&
                phase->acknowledged == config->records && phase->failed == 0;
        printf("{\"schema\":\"kafkars.producer-fixed-load.v1\","
               "\"adapter\":\"librdkafka-c\",\"adapter_version\":\"%s\","
               "\"run_id\":\"%s\",\"topic\":\"%s\","
               "\"offered_records\":%zu,\"accepted_records\":%zu,"
               "\"acknowledged_records\":%zu,\"failed_records\":%zu,"
               "\"payload_bytes\":%zu,\"acknowledged_payload_bytes\":%" PRIu64
               ",\"load\":{\"mode\":\"scheduled-open-loop-fixed-rate\","
               "\"callers_per_producer\":%zu,"
               "\"offered_records_per_second\":%" PRIu64 ","
               "\"schedule_span_ns\":%" PRIu64 ","
               "\"drain_duration_ns\":%" PRIu64 ","
               "\"admission_pressure_records\":0},"
               "\"acknowledged_records_per_second_including_drain\":%.6f,"
               "\"latency_ns\":{",
               adapter_version, config->run_id, config->topic, config->records,
               phase->accepted, phase->acknowledged, phase->failed,
               config->payload_bytes, payload_total, config->callers,
               config->offered_records_per_second, schedule_span,
               phase->duration_ns,
               phase->acknowledged /
                   ((double)phase->duration_ns / 1000000000.0));
        print_latency("uncorrected", phase->latency_ns, phase->acknowledged);
        putchar(',');
        print_latency("corrected", phase->corrected_latency_ns,
                      phase->acknowledged);
        putchar(',');
        print_latency("schedule_delay", phase->schedule_delay_ns,
                      phase->acknowledged);
        printf("},\"settings\":{\"acks\":\"all\",\"idempotence\":true,"
               "\"compression\":\"none\",\"linger_ms\":5,"
               "\"batch_records\":256,\"batch_bytes\":65536,"
               "\"request_bytes\":1048576,"
               "\"max_in_flight_requests_per_broker\":5,"
               "\"queue_bytes\":67108864,\"max_outstanding_records\":%zu,"
               "\"retry_max_replacements\":600,\"retry_backoff_ms\":100,"
               "\"explicit_balanced_partitioning\":true,"
               "\"admission_shape\":\"public-batch\","
               "\"completion_shape\":\"aggregate-batch-terminal\"},"
               "\"native_metrics\":{\"availability\":\"raw-jsonl\","
               "\"path\":\"client-metrics.jsonl\"},\"valid\":%s}\n",
               config->max_outstanding, valid ? "true" : "false");
        return valid ? 0 : -1;
}
