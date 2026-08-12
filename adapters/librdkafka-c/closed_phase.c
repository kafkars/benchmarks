/* Bounded closed-loop application batch admission and completion evidence. */
#include "benchmark.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int bench_allocate_closed_phase(bench_phase_t *phase, size_t records) {
        memset(phase, 0, sizeof(*phase));
        phase->offered      = records;
        phase->admitted_ns  = calloc(records, sizeof(*phase->admitted_ns));
        phase->completed_ns = calloc(records, sizeof(*phase->completed_ns));
        phase->latency_ns   = calloc(records, sizeof(*phase->latency_ns));
        phase->deliveries   = calloc(records, sizeof(*phase->deliveries));
        phase->submissions  = calloc(records, sizeof(*phase->submissions));
        if (!phase->admitted_ns || !phase->completed_ns ||
            !phase->latency_ns || !phase->deliveries || !phase->submissions) {
                perror("allocate phase evidence");
                return -1;
        }
        return 0;
}

int bench_run_closed_phase(rd_kafka_t *producer,
                           const bench_config_t *config,
                           const char *topic,
                           size_t records,
                           int prime_partitions,
                           bench_phase_t *phase) {
        char *payload;
        unsigned char *keys;
        rd_kafka_message_t *messages;
        rd_kafka_topic_t *topic_handle;
        size_t sequence;

        if (records == 0) {
                memset(phase, 0, sizeof(*phase));
                return 0;
        }
        if (bench_allocate_closed_phase(phase, records) != 0)
                return -1;
        topic_handle = rd_kafka_topic_new(producer, topic, NULL);
        if (!topic_handle) {
                fprintf(stderr, "create librdkafka topic handle: %s\n",
                        rd_kafka_err2str(rd_kafka_last_error()));
                return -1;
        }
        payload = malloc(BENCH_BATCH_RECORDS * config->payload_bytes);
        keys = malloc(BENCH_BATCH_RECORDS * 8U);
        messages = calloc(BENCH_BATCH_RECORDS, sizeof(*messages));
        if (!payload || !keys || !messages) {
                perror("allocate batch admission storage");
                free(payload);
                free(keys);
                free(messages);
                rd_kafka_topic_destroy(topic_handle);
                return -1;
        }
        phase->started_ns = bench_now_ns();
        if (phase->started_ns == 0) {
                fprintf(stderr, "monotonic clock failed\n");
                free(payload);
                free(keys);
                free(messages);
                rd_kafka_topic_destroy(topic_handle);
                return -1;
        }
        for (sequence = 0; sequence < records;) {
                bench_submission_t *submission;
                size_t available;
                size_t count;
                size_t index;
                size_t admission_limit =
                    prime_partitions && sequence < (size_t)config->partitions
                        ? 1U
                        : config->max_outstanding;

                while (phase->outstanding >= admission_limit)
                        (void)rd_kafka_poll(producer, 10);
                available = admission_limit - phase->outstanding;
                count = records - sequence;
                if (count > BENCH_BATCH_RECORDS)
                        count = BENCH_BATCH_RECORDS;
                if (count > available)
                        count = available;
                submission = &phase->submissions[phase->submission_count];
                submission->phase = phase;
                submission->first_sequence = sequence;
                submission->count = count;
                atomic_init(&submission->remaining, count);
                for (index = 0; index < count; ++index) {
                        size_t current = sequence + index;
                        char *record_payload =
                            payload + (index * config->payload_bytes);
                        unsigned char *key = keys + (index * 8U);
                        bench_delivery_t *delivery =
                            &phase->deliveries[current];

                        bench_payload(record_payload, config->payload_bytes,
                                      config->run_id, (uint64_t)current);
                        bench_encode_key((uint64_t)current, key);
                        delivery->submission = submission;
                        delivery->sequence = current;
                        messages[index].payload = record_payload;
                        messages[index].len = config->payload_bytes;
                        messages[index].key = key;
                        messages[index].key_len = 8U;
                        messages[index].partition =
                            (int32_t)(current % (size_t)config->partitions);
                        messages[index]._private = delivery;
                        messages[index].err = RD_KAFKA_RESP_ERR_NO_ERROR;
                }
                submission->admitted_ns = bench_now_ns();
                int produced = rd_kafka_produce_batch(
                    topic_handle, RD_KAFKA_PARTITION_UA,
                    RD_KAFKA_MSG_F_COPY | RD_KAFKA_MSG_F_PARTITION, messages,
                    (int)count);
                if (produced != (int)count) {
                        fprintf(stderr,
                                "batch admission %zu accepted %d of %zu records\n",
                                sequence, produced, count);
                        (void)rd_kafka_flush(producer,
                                             BENCH_DELIVERY_TIMEOUT_MS);
                        free(payload);
                        free(keys);
                        free(messages);
                        rd_kafka_topic_destroy(topic_handle);
                        return -1;
                }
                for (index = 0; index < count; ++index)
                        phase->admitted_ns[sequence + index] =
                            submission->admitted_ns - phase->started_ns;
                phase->accepted += count;
                phase->outstanding += count;
                phase->submission_count++;
                sequence += count;
                (void)rd_kafka_poll(producer, 0);
        }
        free(payload);
        free(keys);
        free(messages);
        if (rd_kafka_flush(producer, BENCH_DELIVERY_TIMEOUT_MS) !=
            RD_KAFKA_RESP_ERR_NO_ERROR) {
                fprintf(stderr, "librdkafka flush exceeded the delivery bound\n");
                rd_kafka_topic_destroy(topic_handle);
                return -1;
        }
        rd_kafka_topic_destroy(topic_handle);
        if (phase->outstanding != 0 || rd_kafka_outq_len(producer) != 0) {
                fprintf(stderr, "librdkafka did not drain every accepted record\n");
                return -1;
        }
        return 0;
}
