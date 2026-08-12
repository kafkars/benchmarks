/* Shared state for the raw librdkafka C producer benchmark adapter. */
#ifndef KAFKARS_BENCHMARK_H
#define KAFKARS_BENCHMARK_H

#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>

#include <rdkafka.h>

#define BENCH_RUN_ID_BYTES 16U
#define BENCH_MIN_PAYLOAD_BYTES 64U
#define BENCH_QUEUE_BYTES 67108864U
#define BENCH_CLIENT_RECORD_CAPACITY 100000U
#define BENCH_BATCH_RECORDS 256U
#define BENCH_BATCH_BYTES 65536U
#define BENCH_LINGER_MS 5U
#define BENCH_DELIVERY_TIMEOUT_MS 60000U
#define BENCH_METADATA_POLL_MS 5000
#define BENCH_MAX_RETRIES 600U
#define BENCH_RETRY_BACKOFF_MS 100U
#define BENCH_STATISTICS_INTERVAL_MS 100U
#define BENCH_STATISTICS_BUFFER_BYTES 33554432U

typedef struct bench_config_s {
        const char *bootstrap;
        const char *warmup_topic;
        const char *topic;
        const char *run_id;
        size_t warmup_records;
        size_t records;
        size_t payload_bytes;
        int32_t partitions;
        size_t max_outstanding;
        const char *latency_path;
        const char *statistics_path;
        uint64_t offered_records_per_second;
        size_t callers;
        int fixed_rate;
} bench_config_t;

typedef enum bench_statistics_phase_e {
        BENCH_STATISTICS_SETUP,
        BENCH_STATISTICS_WARMUP,
        BENCH_STATISTICS_BASELINE,
        BENCH_STATISTICS_MEASURED,
        BENCH_STATISTICS_FINAL
} bench_statistics_phase_t;

typedef struct bench_statistics_s {
        const char *path;
        char *buffer;
        size_t length;
        size_t capacity;
        size_t snapshots;
        bench_statistics_phase_t phase;
        int failed;
} bench_statistics_t;

typedef struct bench_phase_s bench_phase_t;
typedef struct bench_submission_s bench_submission_t;
typedef struct bench_fixed_state_s bench_fixed_state_t;

struct bench_submission_s {
        bench_phase_t *phase;
        size_t first_sequence;
        size_t count;
        _Atomic size_t remaining;
        uint64_t admitted_ns;
        size_t caller;
};

typedef struct bench_delivery_s {
        bench_submission_t *submission;
        size_t sequence;
} bench_delivery_t;

struct bench_phase_s {
        size_t offered;
        size_t accepted;
        size_t acknowledged;
        size_t failed;
        size_t outstanding;
        uint64_t started_ns;
        uint64_t duration_ns;
        uint64_t *admitted_ns;
        uint64_t *completed_ns;
        uint64_t *latency_ns;
        uint64_t *intended_ns;
        uint64_t *corrected_latency_ns;
        uint64_t *schedule_delay_ns;
        bench_delivery_t *deliveries;
        bench_submission_t *submissions;
        size_t submission_count;
        size_t *caller_outstanding;
        size_t callers;
        uint64_t offered_records_per_second;
        bench_fixed_state_t *fixed;
};

int bench_parse_config(int argc, char **argv, bench_config_t *config);
int bench_parse_fixed_config(int argc, char **argv, bench_config_t *config);
int bench_allocate_closed_phase(bench_phase_t *phase, size_t records);
int bench_run_closed_phase(rd_kafka_t *producer,
                           const bench_config_t *config,
                           const char *topic,
                           size_t records,
                           int prime_partitions,
                           bench_phase_t *phase);
uint64_t bench_now_ns(void);
int bench_await_topic_metadata(rd_kafka_t *producer,
                               const bench_config_t *config);
rd_kafka_t *bench_create_verifier(const char *bootstrap,
                                  const char *run_id);
int bench_assign_beginning(rd_kafka_t *consumer,
                           const char *topic,
                           int32_t partitions);
void bench_encode_key(uint64_t sequence, unsigned char key[8]);
void bench_payload(char *target,
                   size_t size,
                   const char *run_id,
                   uint64_t sequence);
int bench_schedule_offset_ns(uint64_t sequence,
                             uint64_t rate,
                             uint64_t *offset_ns);
int bench_write_report(const bench_config_t *config,
                       const bench_phase_t *phase,
                       const char *adapter_version);
int bench_write_fixed_report(const bench_config_t *config,
                             const bench_phase_t *phase,
                             const char *adapter_version);
int bench_allocate_fixed_phase(bench_phase_t *phase,
                               const bench_config_t *config);
int bench_fixed_await_due(bench_phase_t *phase, uint64_t due_ns);
int bench_fixed_await_budget(bench_phase_t *phase,
                             size_t caller,
                             size_t budget,
                             size_t count);
int bench_fixed_failed(bench_phase_t *phase);
void bench_fixed_fail(bench_phase_t *phase);
void bench_fixed_submitter_done(bench_phase_t *phase);
int bench_fixed_submitters_done(bench_phase_t *phase);
int bench_fixed_begin_submission(bench_phase_t *phase, size_t batch_index);
void bench_fixed_finish_submission(bench_phase_t *phase);
void bench_fixed_abort_submission(bench_phase_t *phase);
int bench_run_fixed_phase(rd_kafka_t *producer,
                          const bench_config_t *config,
                          bench_phase_t *phase);
void bench_destroy_phase(bench_phase_t *phase);
void bench_fixed_lock(bench_fixed_state_t *state);
void bench_fixed_unlock(bench_fixed_state_t *state);
void bench_fixed_changed(bench_fixed_state_t *state);
void bench_fixed_destroy(bench_fixed_state_t *state);
int bench_statistics_open(bench_statistics_t *statistics, const char *path);
void bench_statistics_set_phase(bench_statistics_t *statistics,
                                bench_statistics_phase_t phase);
int bench_statistics_capture(rd_kafka_t *producer,
                             bench_statistics_t *statistics,
                             bench_statistics_phase_t phase);
int bench_statistics_write(bench_statistics_t *statistics);
void bench_statistics_destroy(bench_statistics_t *statistics);
int bench_statistics_callback(rd_kafka_t *producer,
                              char *json,
                              size_t json_len,
                              void *opaque);

#endif
