// Fail-closed normalization of the pinned librdkafka statistics schema.

import { statSync } from "node:fs";

import {
  brokerCounterDelta,
  counterDelta,
  gauge,
  parseSnapshots,
  partitionDeltas,
  requestTypeDeltas,
  uniquePhaseIndex,
} from "./librdkafka-statistics/schema.mjs";
import {
  aggregateBrokerWindow,
  aggregateTopicWindow,
  sampledBrokerQueues,
  sampledProducerQueue,
} from "./librdkafka-statistics/windows.mjs";

export function summarizeLibrdkafkaStatistics(
  path,
  { topic, records, partitions, maxInFlightPerBroker },
) {
  const snapshots = parseSnapshots(path);
  const baselineIndex = uniquePhaseIndex(snapshots, "baseline");
  const finalIndex = uniquePhaseIndex(snapshots, "final");
  if (baselineIndex >= finalIndex || finalIndex !== snapshots.length - 1) {
    throw new Error("librdkafka statistics phase order is invalid");
  }
  for (let index = 1; index < snapshots.length; index += 1) {
    if (snapshots[index].captured_ns <= snapshots[index - 1].captured_ns) {
      throw new Error("librdkafka statistics captures are not monotonic");
    }
  }

  const baseline = snapshots[baselineIndex].statistics;
  const final = snapshots[finalIndex].statistics;
  const windows = snapshots.slice(baselineIndex + 1, finalIndex + 1);
  if (
    baseline.type !== "producer" ||
    final.type !== "producer" ||
    !windows.every((snapshot) =>
      ["measured", "final"].includes(snapshot.phase),
    )
  ) {
    throw new Error("librdkafka statistics do not isolate the measured phase");
  }

  const transmittedRecords = counterDelta(
    baseline,
    final,
    "txmsgs",
    "transmitted records",
  );
  if (transmittedRecords !== records) {
    throw new Error(
      `librdkafka transmitted ${transmittedRecords} measured records, expected ${records}`,
    );
  }
  const finalQueueRecords = gauge(final, "msg_cnt");
  const finalQueueBytes = gauge(final, "msg_size");
  if (finalQueueRecords !== 0 || finalQueueBytes !== 0) {
    throw new Error("librdkafka native queues did not drain");
  }

  const requestTypes = requestTypeDeltas(baseline, final);
  const produceRequests = requestTypes.Produce ?? 0;
  if (produceRequests <= 0) {
    throw new Error("librdkafka reported no measured Produce requests");
  }
  const batchRecords = aggregateTopicWindow(windows, topic, "batchcnt");
  const batchBytes = aggregateTopicWindow(windows, topic, "batchsize");
  if (
    batchRecords.samples !== batchBytes.samples ||
    batchRecords.sum !== records
  ) {
    throw new Error("librdkafka batch windows do not cover every measured record");
  }

  const partitionRecords = partitionDeltas(
    baseline,
    final,
    topic,
    partitions,
  );
  if (
    partitionRecords.reduce((total, entry) => total + entry.records, 0) !==
    records
  ) {
    throw new Error("librdkafka partition counters do not cover measured records");
  }

  const queues = sampledBrokerQueues(windows);
  const producerQueue = sampledProducerQueue(windows);
  const retries = brokerCounterDelta(baseline, final, "txretries");
  const timeouts = brokerCounterDelta(baseline, final, "req_timeouts");
  const transmittedMessageBytes = counterDelta(
    baseline,
    final,
    "txmsg_bytes",
    "transmitted message bytes",
  );
  const transmittedRequestBytes = counterDelta(
    baseline,
    final,
    "tx_bytes",
    "transmitted request bytes",
  );

  return {
    schema: "kafkars.librdkafka-native-metrics.v1",
    availability: "captured",
    snapshots: {
      total: snapshots.length,
      measured_windows: windows.length,
      interval_ms: 100,
      buffer_capacity_bytes: 33_554_432,
      captured_bytes: statSync(path).size,
    },
    producer: {
      transmitted_records: transmittedRecords,
      transmitted_message_bytes: transmittedMessageBytes,
      final_queue_records: finalQueueRecords,
      final_queue_bytes: finalQueueBytes,
    },
    requests: {
      produce: produceRequests,
      all: counterDelta(baseline, final, "tx", "requests"),
      transmitted_bytes: transmittedRequestBytes,
      retries,
      timeouts,
      by_type: requestTypes,
      records_per_produce_request: transmittedRecords / produceRequests,
      batches_per_produce_request: batchRecords.samples / produceRequests,
    },
    sampled_request_queues: {
      configured_max_in_flight_per_broker: maxInFlightPerBroker,
      ...queues,
    },
    sampled_producer_queue: producerQueue,
    batches: {
      count: batchRecords.samples,
      records: batchRecords.sum,
      transmitted_bytes: batchBytes.sum,
      records_per_batch: batchRecords.sum / batchRecords.samples,
      bytes_per_batch: batchBytes.sum / batchBytes.samples,
      record_count_min: batchRecords.minimum,
      record_count_max: batchRecords.maximum,
      byte_count_min: batchBytes.minimum,
      byte_count_max: batchBytes.maximum,
    },
    latency_us: {
      internal_queue: aggregateBrokerWindow(windows, "int_latency"),
      output_queue: aggregateBrokerWindow(windows, "outbuf_latency"),
      round_trip: aggregateBrokerWindow(windows, "rtt"),
    },
    partitions: partitionRecords,
  };
}
