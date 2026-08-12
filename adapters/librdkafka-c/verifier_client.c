/* Isolated librdkafka consumer construction and direct assignment. */
#include "benchmark.h"

#include <stdio.h>

static int set_property(rd_kafka_conf_t *configuration,
                        const char *name,
                        const char *value) {
        char error[512];

        if (rd_kafka_conf_set(configuration, name, value, error,
                              sizeof(error)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "verifier rejected %s=%s: %s\n", name, value,
                        error);
                return -1;
        }
        return 0;
}

rd_kafka_t *bench_create_verifier(const char *bootstrap,
                                  const char *run_id) {
        rd_kafka_conf_t *configuration = rd_kafka_conf_new();
        char group_id[96];
        char error[512];
        rd_kafka_t *consumer;

        if (!configuration)
                return NULL;
        snprintf(group_id, sizeof(group_id), "kafkars-benchmark-verifier-%s",
                 run_id);
        if (set_property(configuration, "bootstrap.servers", bootstrap) ||
            set_property(configuration, "client.id",
                         "kafkars-independent-benchmark-verifier") ||
            set_property(configuration, "group.id", group_id) ||
            set_property(configuration, "enable.auto.commit", "false") ||
            set_property(configuration, "enable.partition.eof", "true") ||
            set_property(configuration, "allow.auto.create.topics", "false") ||
            set_property(configuration, "fetch.max.bytes", "67108864") ||
            set_property(configuration, "max.partition.fetch.bytes",
                         "67108864")) {
                rd_kafka_conf_destroy(configuration);
                return NULL;
        }
        consumer = rd_kafka_new(RD_KAFKA_CONSUMER, configuration, error,
                                sizeof(error));
        if (!consumer)
                fprintf(stderr, "create verifier consumer: %s\n", error);
        return consumer;
}

int bench_assign_beginning(rd_kafka_t *consumer,
                           const char *topic,
                           int32_t partitions) {
        rd_kafka_topic_partition_list_t *assignment =
            rd_kafka_topic_partition_list_new(partitions);
        rd_kafka_resp_err_t error;
        int32_t partition;

        if (!assignment)
                return -1;
        for (partition = 0; partition < partitions; ++partition) {
                rd_kafka_topic_partition_t *entry =
                    rd_kafka_topic_partition_list_add(assignment, topic,
                                                      partition);
                entry->offset = RD_KAFKA_OFFSET_BEGINNING;
        }
        error = rd_kafka_assign(consumer, assignment);
        rd_kafka_topic_partition_list_destroy(assignment);
        if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
                fprintf(stderr, "assign verifier partitions: %s\n",
                        rd_kafka_err2str(error));
                return -1;
        }
        return 0;
}
