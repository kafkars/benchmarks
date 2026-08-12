/* Shared state for the raw librdkafka C producer benchmark adapter. */
#ifndef KAFKARS_BENCHMARK_H
#define KAFKARS_BENCHMARK_H

#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>

#include <rdkafka.h>

#include "histogram.h"

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
#define BENCH_PATH_BYTES 4096U
#define BENCH_V2_RESULT_FILE "result.json"
#define BENCH_V2_STATISTICS_FILE "client-metrics.jsonl"
#define BENCH_V2_QUEUE_FULL_BACKOFF_NS 200000U

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
        /* Output directory of the v2 evidence contract; NULL on every legacy
           invocation, which is what keeps those byte-identical. */
        const char *v2_output;
        char v2_result_path[BENCH_PATH_BYTES];
        char v2_statistics_path[BENCH_PATH_BYTES];
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

/*
 * One offer of the v2 evidence contract: an immutable identity carrying four
 * timestamps, all nanoseconds since the measured phase began. `call_start_ns`
 * is written once, after the application's own outstanding-budget wait and
 * before anything that is part of admission, and is never rewritten by a
 * queue-full retry of the same offer — that reset is the defect the v2 path
 * exists to remove. `docs/EVIDENCE.md` states that bracket normatively, and
 * `await_budget_then_stamp` in `v2_phase.c` is the only place it is taken.
 */
typedef struct bench_offer_s {
        uint64_t intended_ns;
        uint64_t call_start_ns;
        uint64_t accepted_ns;
        size_t caller;
        int published;
        int active;
} bench_offer_t;

/*
 * Everything the v2 measured path accumulates. Its memory is bounded by the
 * outstanding-record ceiling and the histogram layout, never by run length:
 * the slab holds one entry per offer that can be in flight at once, and each
 * histogram is a fixed array of buckets.
 */
typedef struct bench_v2_phase_s {
        pthread_mutex_t lock;
        pthread_cond_t changed;
        int lock_ready;
        int condition_ready;
        int fatal;
        int closed;
        size_t active_submitters;
        size_t submission_count;

        bench_offer_t *slab;
        size_t slab_capacity;
        size_t *free_slots;
        size_t free_count;
        size_t *caller_outstanding;
        char *payload_pool;
        unsigned char *key_pool;
        rd_kafka_message_t *message_pool;
        uint64_t *intended_pool;

        uint64_t started_ns;
        uint64_t offered;
        uint64_t accepted;
        uint64_t acknowledged;
        uint64_t failed;
        uint64_t timed_out;
        uint64_t unknown;
        uint64_t outstanding;
        uint64_t max_outstanding_observed;
        /* The same two figures in payload bytes. Accumulated at the admit and
           terminal sites rather than multiplied out of the record count
           afterwards: the run's records are one fixed size today, so the two
           agree, and the moment a variable-size payload profile exists the
           multiplication would silently become wrong while this stays right. */
        uint64_t outstanding_bytes;
        uint64_t max_outstanding_bytes_observed;
        /* Schedule epoch to end of drain, on the phase's own monotonic clock:
           the interval the document's throughput is over. */
        uint64_t measured_duration_ns;

        bench_histogram_t intended_to_terminal;
        bench_histogram_t accepted_to_terminal;
        bench_histogram_t call_start_to_accepted;
        bench_histogram_t intended_to_call_start;

        const bench_config_t *config;
} bench_v2_phase_t;

int bench_parse_config(int argc, char **argv, bench_config_t *config);
int bench_parse_fixed_config(int argc, char **argv, bench_config_t *config);
int bench_parse_v2_config(int argc, char **argv, bench_config_t *config);
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
bench_v2_phase_t *bench_v2_create(const bench_config_t *config);
void bench_v2_destroy(bench_v2_phase_t *phase);
int bench_run_v2_phase(rd_kafka_t *producer,
                       const bench_config_t *config,
                       bench_v2_phase_t *phase);
void bench_v2_delivery(bench_v2_phase_t *phase,
                       const rd_kafka_message_t *message);
int bench_write_v2_report(const bench_config_t *config,
                          const bench_v2_phase_t *phase,
                          const char *adapter_version);

#endif
