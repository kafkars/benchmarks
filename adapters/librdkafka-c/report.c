/* Stable JSON summary and raw per-record latency evidence. */
#include "benchmark.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>

static int compare_u64(const void *left, const void *right) {
        uint64_t lhs = *(const uint64_t *)left;
        uint64_t rhs = *(const uint64_t *)right;
        return (lhs > rhs) - (lhs < rhs);
}

static uint64_t percentile(const uint64_t *values,
                           size_t count,
                           size_t per_thousand) {
        size_t rank;

        if (count == 0)
                return 0;
        rank = ((count * per_thousand) + 999U) / 1000U;
        if (rank == 0)
                rank = 1;
        if (rank > count)
                rank = count;
        return values[rank - 1U];
}

static int write_latencies(const char *path, const bench_phase_t *phase) {
        FILE *output = fopen(path, "w");
        size_t sequence;

        if (!output) {
                perror("open latency output");
                return -1;
        }
        fprintf(output, "sequence,admitted_ns,completed_ns,latency_ns\n");
        for (sequence = 0; sequence < phase->accepted; ++sequence) {
                fprintf(output,
                        "%zu,%" PRIu64 ",%" PRIu64 ",%" PRIu64 "\n",
                        sequence, phase->admitted_ns[sequence],
                        phase->completed_ns[sequence],
                        phase->latency_ns[sequence]);
        }
        if (fclose(output) != 0) {
                perror("close latency output");
                return -1;
        }
        return 0;
}

int bench_write_report(const bench_config_t *config,
                       const bench_phase_t *phase,
                       const char *adapter_version) {
        uint64_t *sorted;
        uint64_t payload_total;
        double seconds;
        int valid;

        if (write_latencies(config->latency_path, phase) != 0)
                return -1;
        sorted = malloc(phase->acknowledged * sizeof(*sorted));
        if (!sorted) {
                perror("allocate latency sort buffer");
                return -1;
        }
        for (size_t index = 0; index < phase->acknowledged; ++index)
                sorted[index] = phase->latency_ns[index];
        qsort(sorted, phase->acknowledged, sizeof(*sorted), compare_u64);
        payload_total = (uint64_t)phase->acknowledged * config->payload_bytes;
        seconds       = (double)phase->duration_ns / 1000000000.0;
        valid         = phase->accepted == config->records &&
                phase->acknowledged == config->records && phase->failed == 0;
        printf("{\"schema\":\"kafkars.producer-benchmark.v1\","
               "\"adapter\":\"librdkafka-c\",\"adapter_version\":\"%s\","
               "\"run_id\":\"%s\",\"topic\":\"%s\","
               "\"offered_records\":%zu,\"accepted_records\":%zu,"
               "\"acknowledged_records\":%zu,\"failed_records\":%zu,"
               "\"payload_bytes\":%zu,\"acknowledged_payload_bytes\":%" PRIu64 ","
               "\"duration_ns\":%" PRIu64 ","
               "\"acknowledged_records_per_second\":%.6f,"
               "\"acknowledged_mib_per_second\":%.6f,"
               "\"latency_ns\":{\"p50\":%" PRIu64 ",\"p95\":%" PRIu64
               ",\"p99\":%" PRIu64 ",\"p999\":%" PRIu64 ",\"max\":%" PRIu64 "},"
               "\"settings\":{\"acks\":\"all\",\"idempotence\":true,"
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
               "\"path\":\"client-metrics.jsonl\"},"
               "\"valid\":%s}\n",
               adapter_version, config->run_id, config->topic, config->records,
               phase->accepted, phase->acknowledged, phase->failed,
               config->payload_bytes, payload_total, phase->duration_ns,
               phase->acknowledged / seconds,
               ((double)payload_total / seconds) / 1048576.0,
               percentile(sorted, phase->acknowledged, 500),
               percentile(sorted, phase->acknowledged, 950),
               percentile(sorted, phase->acknowledged, 990),
               percentile(sorted, phase->acknowledged, 999),
               percentile(sorted, phase->acknowledged, 1000),
               config->max_outstanding, valid ? "true" : "false");
        free(sorted);
        return valid ? 0 : -1;
}
