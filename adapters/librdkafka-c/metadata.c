/* Full leader, replica, and ISR readiness for benchmark topics. */
#include "benchmark.h"

#include <stdio.h>
#include <string.h>

static int topic_is_ready(const struct rd_kafka_metadata *metadata,
                          const char *name,
                          int32_t partitions) {
        int topic_index;

        for (topic_index = 0; topic_index < metadata->topic_cnt; ++topic_index) {
                const struct rd_kafka_metadata_topic *topic =
                    &metadata->topics[topic_index];
                int partition_index;

                if (strcmp(topic->topic, name) != 0 ||
                    topic->err != RD_KAFKA_RESP_ERR_NO_ERROR ||
                    topic->partition_cnt != partitions)
                        continue;
                for (partition_index = 0;
                     partition_index < topic->partition_cnt;
                     ++partition_index) {
                        const struct rd_kafka_metadata_partition *partition =
                            &topic->partitions[partition_index];
                        if (partition->err != RD_KAFKA_RESP_ERR_NO_ERROR ||
                            partition->leader < 0 || partition->replica_cnt != 3 ||
                            partition->isr_cnt != 3)
                                break;
                }
                if (partition_index == topic->partition_cnt)
                        return 1;
        }
        return 0;
}

int bench_await_topic_metadata(rd_kafka_t *producer,
                               const bench_config_t *config) {
        uint64_t deadline = bench_now_ns() +
                            (uint64_t)BENCH_DELIVERY_TIMEOUT_MS * 1000000U;

        while (bench_now_ns() < deadline) {
                const struct rd_kafka_metadata *metadata = NULL;
                rd_kafka_resp_err_t error = rd_kafka_metadata(
                    producer, 1, NULL, &metadata, BENCH_METADATA_POLL_MS);
                int ready = error == RD_KAFKA_RESP_ERR_NO_ERROR && metadata &&
                            topic_is_ready(metadata, config->warmup_topic,
                                           config->partitions) &&
                            topic_is_ready(metadata, config->topic,
                                           config->partitions);
                if (metadata)
                        rd_kafka_metadata_destroy(metadata);
                if (ready)
                        return 0;
        }
        fprintf(stderr,
                "librdkafka topics did not reach full replicated leader readiness\n");
        return -1;
}
