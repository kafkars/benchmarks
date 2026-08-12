// Reset-on-emit native window aggregation and sampled queue peaks.

import {
  brokerObjects,
  counter,
  gauge,
  nonnegativeSafeInteger,
  plainObject,
  positiveSafeInteger,
} from "./schema.mjs";

export function aggregateTopicWindow(windows, topic, key) {
  const values = windows.flatMap((snapshot) => {
    const value = snapshot.statistics.topics?.[topic]?.[key];
    return value === undefined ? [] : [value];
  });
  return aggregateWindows(values, `topic ${topic} ${key}`);
}

export function aggregateBrokerWindow(windows, key) {
  const values = windows.flatMap((snapshot) =>
    brokerObjects(snapshot.statistics).flatMap((broker) =>
      broker[key] === undefined ? [] : [broker[key]],
    ),
  );
  return aggregateWindows(values, `broker ${key}`);
}

export function sampledBrokerQueues(windows) {
  let peakWaitingResponse = 0;
  let peakWaitingResponsePerBroker = 0;
  let peakOutput = 0;
  let peakOutputPerBroker = 0;
  for (const snapshot of windows) {
    const brokers = brokerObjects(snapshot.statistics);
    const waiting = brokers.map((broker) => gauge(broker, "waitresp_cnt"));
    const output = brokers.map((broker) => gauge(broker, "outbuf_cnt"));
    peakWaitingResponse = Math.max(
      peakWaitingResponse,
      waiting.reduce((total, value) => total + value, 0),
    );
    peakWaitingResponsePerBroker = Math.max(
      peakWaitingResponsePerBroker,
      ...waiting,
    );
    peakOutput = Math.max(
      peakOutput,
      output.reduce((total, value) => total + value, 0),
    );
    peakOutputPerBroker = Math.max(peakOutputPerBroker, ...output);
  }
  return {
    peak_waiting_response: peakWaitingResponse,
    peak_waiting_response_per_broker: peakWaitingResponsePerBroker,
    peak_output: peakOutput,
    peak_output_per_broker: peakOutputPerBroker,
  };
}

export function sampledProducerQueue(windows) {
  const samples = windows
    .filter((snapshot) => snapshot.phase === "measured")
    .map((snapshot) => ({
      captured_ns: snapshot.captured_ns,
      records: gauge(snapshot.statistics, "msg_cnt"),
    }));
  if (samples.length === 0) {
    return {
      samples: 0,
      span_ns: 0,
      first_records: null,
      last_records: null,
      peak_records: 0,
      linear_slope_records_per_second: null,
    };
  }
  const first = samples[0];
  const last = samples.at(-1);
  const spanNs = last.captured_ns - first.captured_ns;
  return {
    samples: samples.length,
    span_ns: spanNs,
    first_records: first.records,
    last_records: last.records,
    peak_records: Math.max(...samples.map((sample) => sample.records)),
    linear_slope_records_per_second:
      samples.length < 2 || spanNs === 0 ? null : linearSlope(samples),
  };
}

function aggregateWindows(values, name) {
  let samples = 0;
  let sum = 0;
  let minimum;
  let maximum = 0;
  for (const value of values) {
    if (!plainObject(value)) {
      throw new Error(`librdkafka ${name} window is invalid`);
    }
    const count = counter(value, "cnt");
    const windowSum = counter(value, "sum");
    const windowMin = counter(value, "min");
    const windowMax = counter(value, "max");
    if (count === 0) {
      continue;
    }
    samples += count;
    sum += windowSum;
    minimum = minimum === undefined ? windowMin : Math.min(minimum, windowMin);
    maximum = Math.max(maximum, windowMax);
  }
  if (!positiveSafeInteger(samples) || !nonnegativeSafeInteger(sum)) {
    throw new Error(`librdkafka ${name} window has no measured samples`);
  }
  return {
    samples,
    sum,
    average: sum / samples,
    minimum,
    maximum,
  };
}

function linearSlope(samples) {
  const origin = samples[0].captured_ns;
  const points = samples.map((sample) => ({
    seconds: (sample.captured_ns - origin) / 1_000_000_000,
    records: sample.records,
  }));
  const meanSeconds =
    points.reduce((total, point) => total + point.seconds, 0) / points.length;
  const meanRecords =
    points.reduce((total, point) => total + point.records, 0) / points.length;
  const numerator = points.reduce(
    (total, point) =>
      total + (point.seconds - meanSeconds) * (point.records - meanRecords),
    0,
  );
  const denominator = points.reduce(
    (total, point) => total + (point.seconds - meanSeconds) ** 2,
    0,
  );
  if (denominator === 0) {
    throw new Error("librdkafka producer queue samples have no time span");
  }
  return numerator / denominator;
}
