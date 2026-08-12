// Fail-closed validation of scheduled fixed-rate producer evidence.

import { readFileSync } from "node:fs";

export function validateFixedProducerResult(
  result,
  { adapter, topic, runId, records, payloadBytes, offeredRate, callers, settings },
) {
  const expectedBytes = records * payloadBytes;
  if (
    result.schema !== "kafkars.producer-fixed-load.v1" ||
    result.adapter !== adapter ||
    result.run_id !== runId ||
    result.topic !== topic ||
    result.offered_records !== records ||
    result.accepted_records !== records ||
    result.acknowledged_records !== records ||
    result.failed_records !== 0 ||
    result.payload_bytes !== payloadBytes ||
    result.acknowledged_payload_bytes !== expectedBytes ||
    result.load?.mode !== "scheduled-open-loop-fixed-rate" ||
    result.load.callers_per_producer !== callers ||
    result.load.offered_records_per_second !== offeredRate ||
    result.load.schedule_span_ns !== intendedOffset(records - 1, offeredRate) ||
    !positiveNumber(result.load.drain_duration_ns) ||
    result.load.admission_pressure_records !== 0 ||
    !positiveNumber(result.acknowledged_records_per_second_including_drain) ||
    JSON.stringify(result.settings) !== JSON.stringify(settings) ||
    result.valid !== true
  ) {
    throw new Error(`${adapter} fixed-load result violates the sealed contract`);
  }
  for (const surface of ["uncorrected", "corrected", "schedule_delay"]) {
    if (
      !["p50", "p95", "p99", "p999", "max"].every((key) =>
        nonnegativeInteger(result.latency_ns?.[surface]?.[key]),
      )
    ) {
      throw new Error(`${adapter} fixed-load latency summary is invalid`);
    }
  }
}

export function validateFixedLatencyCsv(path, records, offeredRate) {
  const lines = readFileSync(path, "utf8").trimEnd().split("\n");
  if (
    lines.shift() !==
      "sequence,intended_ns,admitted_ns,completed_ns,uncorrected_latency_ns,corrected_latency_ns" ||
    lines.length !== records
  ) {
    throw new Error(`${path} has an invalid fixed-load latency shape`);
  }
  const sequences = new Set();
  for (const line of lines) {
    const fields = line.split(",");
    if (fields.length !== 6 || !fields.every((field) => /^[0-9]+$/.test(field))) {
      throw new Error(`${path} contains a malformed fixed-load latency row`);
    }
    const [sequence, intended, admitted, completed, uncorrected, corrected] =
      fields.map(BigInt);
    if (
      sequence >= BigInt(records) ||
      intended !== BigInt(intendedOffset(Number(sequence), offeredRate)) ||
      admitted < intended ||
      completed < admitted ||
      completed - admitted !== uncorrected ||
      completed - intended !== corrected
    ) {
      throw new Error(`${path} contains inconsistent fixed-load latency evidence`);
    }
    sequences.add(sequence.toString());
  }
  if (sequences.size !== records) {
    throw new Error(`${path} does not contain every fixed-load sequence exactly once`);
  }
}

export function intendedOffset(sequence, rate) {
  if (
    !Number.isSafeInteger(sequence) ||
    sequence < 0 ||
    !Number.isSafeInteger(rate) ||
    rate <= 0 ||
    rate > 1_000_000_000
  ) {
    throw new Error("fixed-load schedule input is invalid");
  }
  const whole = Math.floor(sequence / rate) * 1_000_000_000;
  const remainder = sequence % rate;
  const result = whole + Math.floor((remainder * 1_000_000_000) / rate);
  if (!Number.isSafeInteger(result)) {
    throw new Error("fixed-load schedule offset is outside the safe range");
  }
  return result;
}

function positiveNumber(value) {
  return typeof value === "number" && Number.isFinite(value) && value > 0;
}

function nonnegativeInteger(value) {
  return Number.isSafeInteger(value) && value >= 0;
}
