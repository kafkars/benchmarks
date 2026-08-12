// Fail-closed validation for normalized benchmark results and raw latency evidence.

import { readFileSync } from "node:fs";

export function positiveInteger(name, value, zeroAllowed = false) {
  if (!/^[0-9]+$/.test(value)) {
    throw new Error(`${name} must be an unsigned decimal integer`);
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isSafeInteger(parsed) || (!zeroAllowed && parsed === 0)) {
    throw new Error(`${name} is outside the supported positive range`);
  }
  return parsed;
}

export function validateExecutionOrder(value) {
  const order = value.split(",");
  if (
    order.length !== 2 ||
    new Set(order).size !== 2 ||
    !order.includes("kafkars") ||
    !order.includes("librdkafka-c")
  ) {
    throw new Error(`invalid adapter execution order: ${value}`);
  }
  return order;
}

export function validateProducerResult(
  result,
  adapter,
  topic,
  expectedRunId,
  expectedRecords,
  expectedPayloadBytes,
  settings,
) {
  const expectedBytes = expectedRecords * expectedPayloadBytes;
  const identityIsExact =
    result.schema === "kafkars.producer-benchmark.v1" &&
    result.adapter === adapter &&
    result.run_id === expectedRunId &&
    result.topic === topic;
  const countsAreExact =
    result.offered_records === expectedRecords &&
    result.accepted_records === expectedRecords &&
    result.acknowledged_records === expectedRecords &&
    result.failed_records === 0 &&
    result.payload_bytes === expectedPayloadBytes &&
    result.acknowledged_payload_bytes === expectedBytes;
  const measurementsAreFinite =
    positiveNumber(result.duration_ns) &&
    positiveNumber(result.acknowledged_records_per_second) &&
    positiveNumber(result.acknowledged_mib_per_second) &&
    ["p50", "p95", "p99", "p999", "max"].every((key) =>
      nonnegativeInteger(result.latency_ns?.[key]),
    );
  if (
    !identityIsExact ||
    !countsAreExact ||
    !measurementsAreFinite ||
    JSON.stringify(result.settings) !== JSON.stringify(settings) ||
    result.valid !== true
  ) {
    throw new Error(`${adapter} producer result violates the sealed contract`);
  }
}

export function validateVerification(
  result,
  topic,
  expectedRecords,
  partitions,
) {
  if (
    result.schema !== "kafkars.producer-verification.v1" ||
    result.topic !== topic ||
    result.expected_records !== expectedRecords ||
    result.verified_records !== expectedRecords ||
    result.duplicates !== 0 ||
    result.missing_records !== 0 ||
    result.corrupt !== 0 ||
    result.unexpected !== 0 ||
    result.eof_partitions !== partitions ||
    result.valid !== true
  ) {
    throw new Error(`verification for ${topic} violates the sealed contract`);
  }
}

export function validateKafkarsNativeMetrics(
  result,
  expectedRecords,
  maxInFlightPerBroker,
  brokers,
) {
  const requests = result.native_metrics?.producer_requests;
  if (
    !positiveSafeInteger(requests?.requests) ||
    !positiveSafeInteger(requests?.partition_batches) ||
    requests.records !== expectedRecords ||
    !positiveSafeInteger(requests?.encoded_record_bytes) ||
    !positiveSafeInteger(requests?.peak_in_flight_requests) ||
    !positiveSafeInteger(requests?.peak_in_flight_requests_per_broker) ||
    requests.peak_in_flight_requests_per_broker > maxInFlightPerBroker ||
    requests.peak_in_flight_requests > maxInFlightPerBroker * brokers
  ) {
    throw new Error("kafkars native Produce request evidence violates the configured gate");
  }
}

export function validateLatencyCsv(path, expectedRecords) {
  const lines = readFileSync(path, "utf8").trimEnd().split("\n");
  if (
    lines.shift() !== "sequence,admitted_ns,completed_ns,latency_ns" ||
    lines.length !== expectedRecords
  ) {
    throw new Error(`${path} has an invalid latency row count or header`);
  }
  const sequences = new Set();
  for (const line of lines) {
    const fields = line.split(",");
    if (fields.length !== 4 || !fields.every((field) => /^[0-9]+$/.test(field))) {
      throw new Error(`${path} contains a malformed latency row`);
    }
    const [sequence, admitted, completed, latency] = fields.map(BigInt);
    if (
      sequence >= BigInt(expectedRecords) ||
      completed < admitted ||
      completed - admitted !== latency
    ) {
      throw new Error(`${path} contains inconsistent latency evidence`);
    }
    sequences.add(sequence.toString());
  }
  if (sequences.size !== expectedRecords) {
    throw new Error(`${path} does not contain every sequence exactly once`);
  }
}

function positiveNumber(value) {
  return typeof value === "number" && Number.isFinite(value) && value > 0;
}

function positiveSafeInteger(value) {
  return Number.isSafeInteger(value) && value > 0;
}

function nonnegativeInteger(value) {
  return Number.isSafeInteger(value) && value >= 0;
}
