/* Four bounded submitters over one immutable scheduled record stream. */
#include "benchmark.h"

#include <stdio.h>
#include <stdlib.h>

typedef struct fixed_caller_s {
        rd_kafka_t *producer;
        rd_kafka_topic_t *topic;
        const bench_config_t *config;
        bench_phase_t *phase;
        size_t caller;
        size_t budget;
} fixed_caller_t;

static int submit(fixed_caller_t *caller,
                  size_t batch_index,
                  size_t sequence,
                  size_t count,
                  uint64_t due_ns,
                  char *payload,
                  unsigned char *keys,
                  rd_kafka_message_t *messages) {
        bench_phase_t *phase = caller->phase;
        bench_submission_t *submission = &phase->submissions[batch_index];
        size_t index;

        if (bench_fixed_await_due(phase, due_ns) ||
            bench_fixed_await_budget(phase, caller->caller, caller->budget,
                                     count))
                return -1;
        submission->phase = phase;
        submission->first_sequence = sequence;
        submission->count = count;
        submission->caller = caller->caller;
        atomic_init(&submission->remaining, count);
        for (index = 0; index < count; ++index) {
                size_t current = sequence + index;
                char *record_payload =
                    payload + (index * caller->config->payload_bytes);
                unsigned char *key = keys + (index * 8U);
                bench_delivery_t *delivery = &phase->deliveries[current];

                bench_payload(record_payload, caller->config->payload_bytes,
                              caller->config->run_id, (uint64_t)current);
                bench_encode_key((uint64_t)current, key);
                delivery->submission = submission;
                delivery->sequence = current;
                messages[index].payload = record_payload;
                messages[index].len = caller->config->payload_bytes;
                messages[index].key = key;
                messages[index].key_len = 8U;
                messages[index].partition =
                    (int32_t)(current % (size_t)caller->config->partitions);
                messages[index]._private = delivery;
                messages[index].err = RD_KAFKA_RESP_ERR_NO_ERROR;
                if (bench_schedule_offset_ns(
                        (uint64_t)current,
                        caller->config->offered_records_per_second,
                        &phase->intended_ns[current]))
                        return -1;
        }
        if (bench_fixed_begin_submission(phase, batch_index))
                return -1;
        submission->admitted_ns = bench_now_ns();
        if (submission->admitted_ns < phase->started_ns) {
                bench_fixed_abort_submission(phase);
                return -1;
        }
        int produced = rd_kafka_produce_batch(
            caller->topic, RD_KAFKA_PARTITION_UA,
            RD_KAFKA_MSG_F_COPY | RD_KAFKA_MSG_F_PARTITION, messages,
            (int)count);
        if (produced != (int)count) {
                bench_fixed_abort_submission(phase);
                return -1;
        }
        for (index = 0; index < count; ++index) {
                size_t current = sequence + index;
                phase->admitted_ns[current] =
                    submission->admitted_ns - phase->started_ns;
                phase->schedule_delay_ns[current] =
                    phase->admitted_ns[current] - phase->intended_ns[current];
        }
        phase->accepted += count;
        phase->outstanding += count;
        phase->caller_outstanding[caller->caller] += count;
        bench_fixed_finish_submission(phase);
        return 0;
}

static void *caller_main(void *opaque) {
        fixed_caller_t *caller = opaque;
        const bench_config_t *config = caller->config;
        bench_phase_t *phase = caller->phase;
        char *payload = malloc(BENCH_BATCH_RECORDS * config->payload_bytes);
        unsigned char *keys = malloc(BENCH_BATCH_RECORDS * 8U);
        rd_kafka_message_t *messages =
            calloc(BENCH_BATCH_RECORDS, sizeof(*messages));
        size_t batch_count =
            (config->records + BENCH_BATCH_RECORDS - 1U) / BENCH_BATCH_RECORDS;
        size_t batch_index;

        if (!payload || !keys || !messages) {
                bench_fixed_fail(phase);
        }
        for (batch_index = caller->caller;
             !bench_fixed_failed(phase) && batch_index < batch_count;
             batch_index += config->callers) {
                size_t sequence = batch_index * BENCH_BATCH_RECORDS;
                size_t count = config->records - sequence;
                uint64_t due_ns;

                if (count > BENCH_BATCH_RECORDS)
                        count = BENCH_BATCH_RECORDS;
                if (bench_schedule_offset_ns((uint64_t)(sequence + count - 1U),
                                             config->offered_records_per_second,
                                             &due_ns) ||
                    submit(caller, batch_index, sequence, count, due_ns,
                           payload, keys, messages)) {
                        bench_fixed_fail(phase);
                }
        }
        free(payload);
        free(keys);
        free(messages);
        bench_fixed_submitter_done(phase);
        return NULL;
}

int bench_run_fixed_phase(rd_kafka_t *producer,
                          const bench_config_t *config,
                          bench_phase_t *phase) {
        rd_kafka_topic_t *topic;
        pthread_t threads[4];
        fixed_caller_t callers[4];
        size_t base_budget = config->max_outstanding / config->callers;
        size_t extra_budget = config->max_outstanding % config->callers;
        size_t started = 0;

        if (base_budget < BENCH_BATCH_RECORDS ||
            bench_allocate_fixed_phase(phase, config))
                return -1;
        topic = rd_kafka_topic_new(producer, config->topic, NULL);
        if (!topic)
                return -1;
        phase->started_ns = bench_now_ns();
        if (phase->started_ns == 0 ||
            phase->started_ns > UINT64_MAX - 100000000U) {
                rd_kafka_topic_destroy(topic);
                return -1;
        }
        phase->started_ns += 100000000U;
        for (size_t index = 0; index < config->callers; ++index) {
                callers[index] = (fixed_caller_t){
                    .producer = producer,
                    .topic = topic,
                    .config = config,
                    .phase = phase,
                    .caller = index,
                    .budget = base_budget + (index < extra_budget ? 1U : 0U),
                };
                if (pthread_create(&threads[index], NULL, caller_main,
                                   &callers[index]) != 0) {
                        bench_fixed_fail(phase);
                        for (size_t absent = index;
                             absent < config->callers; ++absent)
                                bench_fixed_submitter_done(phase);
                        break;
                }
                started++;
        }
        while (!bench_fixed_submitters_done(phase))
                (void)rd_kafka_poll(producer, 10);
        while (started > 0) {
                started--;
                if (pthread_join(threads[started], NULL) != 0)
                        bench_fixed_fail(phase);
        }
        if (rd_kafka_flush(producer, BENCH_DELIVERY_TIMEOUT_MS) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                bench_fixed_fail(phase);
        rd_kafka_topic_destroy(topic);
        if (phase->outstanding != 0 || rd_kafka_outq_len(producer) != 0 ||
            phase->accepted != config->records || bench_fixed_failed(phase)) {
                fprintf(stderr, "fixed-load phase failed admission or drain\n");
                return -1;
        }
        return 0;
}
