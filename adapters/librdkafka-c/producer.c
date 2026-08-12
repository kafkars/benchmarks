/* Raw librdkafka C producer adapter with bounded application ownership. */
#include "benchmark.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void delivery_report(rd_kafka_t *producer,
                            const rd_kafka_message_t *message,
                            void *opaque) {
        bench_delivery_t *delivery = (bench_delivery_t *)message->_private;
        bench_submission_t *submission;
        bench_phase_t *phase;
        uint64_t completed;
        size_t index;

        (void)producer;
        (void)opaque;
        if (!delivery || !delivery->submission)
                return;
        submission = delivery->submission;
        phase      = submission->phase;
        completed = bench_now_ns();
        if (phase->fixed)
                bench_fixed_lock(phase->fixed);
        if (message->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                phase->acknowledged++;
        } else {
                phase->failed++;
                fprintf(stderr, "delivery %zu failed: %s\n", delivery->sequence,
                        rd_kafka_message_errstr(message));
        }
        if (phase->outstanding > 0)
                phase->outstanding--;
        if (phase->caller_outstanding &&
            phase->caller_outstanding[submission->caller] > 0)
                phase->caller_outstanding[submission->caller]--;
        size_t remaining = atomic_fetch_sub(&submission->remaining, 1U) - 1U;
        if (remaining != 0) {
                if (phase->fixed) {
                        bench_fixed_changed(phase->fixed);
                        bench_fixed_unlock(phase->fixed);
                }
                return;
        }
        for (index = 0; index < submission->count; ++index) {
                size_t sequence = submission->first_sequence + index;

                phase->completed_ns[sequence] = completed - phase->started_ns;
                phase->latency_ns[sequence] =
                    completed - submission->admitted_ns;
                if (phase->corrected_latency_ns) {
                        phase->corrected_latency_ns[sequence] =
                            phase->completed_ns[sequence] -
                            phase->intended_ns[sequence];
                }
        }
        phase->duration_ns = completed - phase->started_ns;
        if (phase->fixed) {
                bench_fixed_changed(phase->fixed);
                bench_fixed_unlock(phase->fixed);
        }
}

static int set_property(rd_kafka_conf_t *configuration,
                        const char *name,
                        const char *value) {
        char error[512];

        if (rd_kafka_conf_set(configuration, name, value, error,
                              sizeof(error)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "librdkafka rejected %s=%s: %s\n", name, value,
                        error);
                return -1;
        }
        return 0;
}

static rd_kafka_t *create_producer(const bench_config_t *config,
                                   bench_statistics_t *statistics) {
        rd_kafka_conf_t *configuration = rd_kafka_conf_new();
        char error[512];
        rd_kafka_t *producer;

        if (!configuration)
                return NULL;
        if (set_property(configuration, "bootstrap.servers", config->bootstrap) ||
            set_property(configuration, "client.id",
                         "kafkars-raw-librdkafka-comparison") ||
            set_property(configuration, "enable.idempotence", "true") ||
            set_property(configuration, "acks", "all") ||
            set_property(configuration, "compression.type", "none") ||
            set_property(configuration, "linger.ms", "5") ||
            set_property(configuration, "batch.num.messages", "256") ||
            set_property(configuration, "batch.size", "65536") ||
            set_property(configuration, "queue.buffering.max.kbytes", "65536") ||
            set_property(configuration, "queue.buffering.max.messages",
                         "100000") ||
            set_property(configuration, "max.in.flight.requests.per.connection",
                         "5") ||
            set_property(configuration, "message.timeout.ms", "60000") ||
            set_property(configuration, "message.send.max.retries", "600") ||
            set_property(configuration, "retry.backoff.ms", "100") ||
            set_property(configuration, "retry.backoff.max.ms", "100") ||
            set_property(configuration, "statistics.interval.ms", "100") ||
            set_property(configuration, "allow.auto.create.topics", "false")) {
                rd_kafka_conf_destroy(configuration);
                return NULL;
        }
        rd_kafka_conf_set_opaque(configuration, statistics);
        rd_kafka_conf_set_dr_msg_cb(configuration, delivery_report);
        rd_kafka_conf_set_stats_cb(configuration, bench_statistics_callback);
        producer = rd_kafka_new(RD_KAFKA_PRODUCER, configuration, error,
                                sizeof(error));
        if (!producer)
                fprintf(stderr, "create librdkafka producer: %s\n", error);
        return producer;
}

int main(int argc, char **argv) {
        bench_config_t config;
        bench_phase_t warmup = {0};
        bench_phase_t measured = {0};
        bench_statistics_t statistics;
        rd_kafka_t *producer;
        int result;

        if ((argc > 1 && strcmp(argv[1], "--fixed-rate") == 0
                 ? bench_parse_fixed_config(argc, argv, &config)
                 : bench_parse_config(argc, argv, &config)) != 0)
                return EXIT_FAILURE;
        if (bench_statistics_open(&statistics, config.statistics_path) != 0)
                return EXIT_FAILURE;
        producer = create_producer(&config, &statistics);
        if (!producer) {
                bench_statistics_destroy(&statistics);
                return EXIT_FAILURE;
        }
        if (bench_await_topic_metadata(producer, &config) != 0) {
                rd_kafka_destroy(producer);
                bench_statistics_destroy(&statistics);
                return EXIT_FAILURE;
        }
        bench_statistics_set_phase(&statistics, BENCH_STATISTICS_WARMUP);
        if (bench_run_closed_phase(producer, &config, config.warmup_topic,
                                   config.warmup_records, 1, &warmup) != 0) {
                bench_destroy_phase(&warmup);
                rd_kafka_destroy(producer);
                bench_statistics_destroy(&statistics);
                return EXIT_FAILURE;
        }
        bench_destroy_phase(&warmup);
        if (bench_statistics_capture(producer, &statistics,
                                     BENCH_STATISTICS_BASELINE) != 0) {
                rd_kafka_destroy(producer);
                bench_statistics_destroy(&statistics);
                return EXIT_FAILURE;
        }
        bench_statistics_set_phase(&statistics, BENCH_STATISTICS_MEASURED);
        if ((config.fixed_rate
                 ? bench_run_fixed_phase(producer, &config, &measured)
                 : bench_run_closed_phase(producer, &config, config.topic,
                                          config.records, 0, &measured)) != 0) {
                bench_destroy_phase(&measured);
                rd_kafka_destroy(producer);
                bench_statistics_destroy(&statistics);
                return EXIT_FAILURE;
        }
        if (bench_statistics_capture(producer, &statistics,
                                     BENCH_STATISTICS_FINAL) != 0 ||
            bench_statistics_write(&statistics) != 0) {
                bench_destroy_phase(&measured);
                rd_kafka_destroy(producer);
                bench_statistics_destroy(&statistics);
                return EXIT_FAILURE;
        }
        result = config.fixed_rate
                     ? bench_write_fixed_report(&config, &measured,
                                                rd_kafka_version_str())
                     : bench_write_report(&config, &measured,
                                          rd_kafka_version_str());
        bench_destroy_phase(&measured);
        rd_kafka_destroy(producer);
        bench_statistics_destroy(&statistics);
        return result == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
