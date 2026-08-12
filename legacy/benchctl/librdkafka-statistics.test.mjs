// Contract tests for raw librdkafka statistics normalization.

import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { summarizeLibrdkafkaStatistics } from "./librdkafka-statistics.mjs";

const topic = "measured-topic";

test("statistics isolate measured counters, windows, and broker queues", () => {
  withStatisticsFile(validSnapshots(), (path) => {
    const summary = summarizeLibrdkafkaStatistics(path, {
      topic,
      records: 100,
      partitions: 2,
      maxInFlightPerBroker: 5,
    });
    assert.equal(summary.producer.transmitted_records, 100);
    assert.equal(summary.snapshots.buffer_capacity_bytes, 33_554_432);
    assert.ok(summary.snapshots.captured_bytes > 0);
    assert.equal(summary.requests.produce, 10);
    assert.equal(summary.requests.by_type.Metadata, 2);
    assert.equal(summary.batches.count, 10);
    assert.equal(summary.batches.records_per_batch, 10);
    assert.equal(summary.sampled_request_queues.peak_waiting_response, 4);
    assert.deepEqual(summary.sampled_producer_queue, {
      samples: 1,
      span_ns: 0,
      first_records: 2,
      last_records: 2,
      peak_records: 2,
      linear_slope_records_per_second: null,
    });
    assert.deepEqual(summary.partitions, [
      { partition: 0, records: 50 },
      { partition: 1, records: 50 },
    ]);
  });
});

test("statistics expose the measured native producer queue trend", () => {
  const snapshots = validSnapshots();
  snapshots[1] = envelope(
    "measured",
    2,
    statistics(70, 7, 2, 16, 1, 0, 40, 4),
  );
  snapshots.splice(
    2,
    0,
    envelope("measured", 3, statistics(90, 10, 6, 18, 2, 0, 20, 2)),
  );
  snapshots.at(-1).captured_ns = 4;
  withStatisticsFile(snapshots, (path) => {
    const summary = summarizeLibrdkafkaStatistics(path, {
      topic,
      records: 100,
      partitions: 2,
      maxInFlightPerBroker: 5,
    });
    assert.deepEqual(summary.sampled_producer_queue, {
      samples: 2,
      span_ns: 1,
      first_records: 2,
      last_records: 6,
      peak_records: 6,
      linear_slope_records_per_second: 4_000_000_000,
    });
  });
});

test("statistics label a sub-interval measured phase with zero queue samples", () => {
  const snapshots = [
    envelope("baseline", 1, statistics(10, 1, 0, 10, 1, 0, 0, 0)),
    envelope("final", 3, statistics(110, 13, 0, 20, 3, 0, 100, 10)),
  ];
  withStatisticsFile(snapshots, (path) => {
    const summary = summarizeLibrdkafkaStatistics(path, {
      topic,
      records: 100,
      partitions: 2,
      maxInFlightPerBroker: 5,
    });
    assert.deepEqual(summary.sampled_producer_queue, {
      samples: 0,
      span_ns: 0,
      first_records: null,
      last_records: null,
      peak_records: 0,
      linear_slope_records_per_second: null,
    });
  });
});

test("statistics reject incomplete or inconsistent native evidence", () => {
  const snapshots = validSnapshots();
  snapshots.at(-1).statistics.txmsgs = 109;
  withStatisticsFile(snapshots, (path) => {
    assert.throws(() =>
      summarizeLibrdkafkaStatistics(path, {
        topic,
        records: 100,
        partitions: 2,
        maxInFlightPerBroker: 5,
      }),
    );
  });
  withStatisticsFile(validSnapshots().slice(0, -1), (path) => {
    assert.throws(() =>
      summarizeLibrdkafkaStatistics(path, {
        topic,
        records: 100,
        partitions: 2,
        maxInFlightPerBroker: 5,
      }),
    );
  });
});

function withStatisticsFile(snapshots, body) {
  const directory = mkdtempSync(join(tmpdir(), "kafkars-statistics-test-"));
  const path = join(directory, "client-metrics.jsonl");
  try {
    writeFileSync(path, `${snapshots.map(JSON.stringify).join("\n")}\n`);
    body(path);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

function validSnapshots() {
  return [
    envelope("baseline", 1, statistics(10, 1, 0, 10, 1, 0, 0, 0)),
    envelope("measured", 2, statistics(70, 7, 2, 16, 1, 0, 60, 6)),
    envelope("final", 3, statistics(110, 13, 0, 20, 3, 0, 40, 4)),
  ];
}

function envelope(phase, captured, value) {
  return {
    schema: "kafkars.librdkafka-statistics.v1",
    phase,
    captured_ns: captured,
    statistics: value,
  };
}

function statistics(
  txmsgs,
  tx,
  queued,
  produceRequests,
  metadataRequests,
  timeouts,
  batchRecords,
  batchCount,
) {
  const measured = txmsgs - 10;
  const partitionRecords = Math.max(0, measured / 2);
  return {
    type: "producer",
    txmsgs,
    txmsg_bytes: txmsgs * 1_100,
    tx,
    tx_bytes: tx * 10_000,
    msg_cnt: queued,
    msg_size: queued * 1_024,
    brokers: {
      broker: {
        nodeid: 1,
        req: { Produce: produceRequests, Metadata: metadataRequests },
        txretries: produceRequests >= 20 ? 2 : 1,
        req_timeouts: timeouts,
        waitresp_cnt: queued > 0 ? 4 : 0,
        outbuf_cnt: queued > 0 ? 2 : 0,
        int_latency: window(batchCount, batchCount * 10, 5, 15),
        outbuf_latency: window(batchCount, batchCount * 20, 10, 30),
        rtt: window(batchCount, batchCount * 30, 20, 40),
      },
    },
    topics: {
      [topic]: {
        batchcnt: window(batchCount, batchRecords, 8, 12),
        batchsize: window(batchCount, batchCount * 10_000, 8_000, 12_000),
        partitions: {
          0: { txmsgs: partitionRecords },
          1: { txmsgs: partitionRecords },
        },
      },
    },
  };
}

function window(count, sum, minimum, maximum) {
  return {
    cnt: count,
    sum,
    min: count === 0 ? 0 : minimum,
    max: count === 0 ? 0 : maximum,
  };
}
