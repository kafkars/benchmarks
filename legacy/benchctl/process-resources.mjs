// Fail-closed normalization of whole-adapter CPU and peak-memory evidence.

import { readFileSync } from "node:fs";

const SCOPE = "whole-adapter-process-including-warmup";

export function readProcessResources(path, acknowledgedRecords) {
  if (!Number.isSafeInteger(acknowledgedRecords) || acknowledgedRecords <= 0) {
    throw new Error("process resources require acknowledged records");
  }
  const raw = readFileSync(path, "utf8");
  const parsed = raw.includes("Maximum resident set size (kbytes)")
    ? parseGnuTime(raw)
    : parseDarwinTime(raw);
  const cpuCoreSeconds = parsed.user_cpu_seconds + parsed.system_cpu_seconds;
  if (
    !positive(cpuCoreSeconds) ||
    !positive(parsed.elapsed_wall_seconds) ||
    !positiveInteger(parsed.max_rss_bytes) ||
    !positiveInteger(parsed.peak_memory_bytes)
  ) {
    throw new Error("process resource evidence is incomplete or nonpositive");
  }
  return {
    schema: "kafkars.process-resources.v2",
    scope: SCOPE,
    ...parsed,
    cpu_core_seconds: cpuCoreSeconds,
    cpu_core_seconds_per_million_acknowledged_records:
      (cpuCoreSeconds * 1_000_000) / acknowledgedRecords,
    instructions_retired_per_million_acknowledged_records: normalizeOptional(
      parsed.instructions_retired,
      acknowledgedRecords,
    ),
    cycles_elapsed_per_million_acknowledged_records: normalizeOptional(
      parsed.cycles_elapsed,
      acknowledgedRecords,
    ),
  };
}

function parseDarwinTime(raw) {
  return {
    method: "darwin-bsd-time-lp",
    user_cpu_seconds: decimalLine(raw, "user"),
    system_cpu_seconds: decimalLine(raw, "sys"),
    elapsed_wall_seconds: decimalLine(raw, "real"),
    max_rss_bytes: integerMetric(raw, "maximum resident set size"),
    peak_memory_bytes: integerMetric(raw, "peak memory footprint"),
    peak_memory_method: "darwin-peak-physical-footprint",
    instructions_retired: optionalIntegerMetric(raw, "instructions retired"),
    cycles_elapsed: optionalIntegerMetric(raw, "cycles elapsed"),
    minor_page_faults: integerMetric(raw, "page reclaims"),
    major_page_faults: integerMetric(raw, "page faults"),
    voluntary_context_switches: integerMetric(
      raw,
      "voluntary context switches",
    ),
    involuntary_context_switches: integerMetric(
      raw,
      "involuntary context switches",
    ),
  };
}

function parseGnuTime(raw) {
  const maxRssBytes =
    keyedInteger(raw, "Maximum resident set size (kbytes)") * 1024;
  return {
    method: "linux-gnu-time-v",
    user_cpu_seconds: keyedDecimal(raw, "User time (seconds)"),
    system_cpu_seconds: keyedDecimal(raw, "System time (seconds)"),
    elapsed_wall_seconds: elapsedSeconds(
      keyedValue(raw, "Elapsed (wall clock) time (h:mm:ss or m:ss)"),
    ),
    max_rss_bytes: maxRssBytes,
    peak_memory_bytes: maxRssBytes,
    peak_memory_method: "linux-maximum-resident-set-size",
    instructions_retired: null,
    cycles_elapsed: null,
    minor_page_faults: keyedInteger(
      raw,
      "Minor (reclaiming a frame) page faults",
    ),
    major_page_faults: keyedInteger(
      raw,
      "Major (requiring I/O) page faults",
    ),
    voluntary_context_switches: keyedInteger(
      raw,
      "Voluntary context switches",
    ),
    involuntary_context_switches: keyedInteger(
      raw,
      "Involuntary context switches",
    ),
  };
}

function decimalLine(raw, label) {
  const match = raw.match(new RegExp(`^${label} ([0-9]+(?:\\.[0-9]+)?)$`, "m"));
  return decimal(match?.[1], label);
}

function integerMetric(raw, label) {
  const match = raw.match(new RegExp(`^\\s*([0-9]+)  ${label}$`, "m"));
  return integer(match?.[1], label);
}

function optionalIntegerMetric(raw, label) {
  const match = raw.match(new RegExp(`^\\s*([0-9]+)  ${label}$`, "m"));
  return match ? integer(match[1], label) : null;
}

function keyedValue(raw, label) {
  const line = raw.split("\n").find((entry) => entry.trimStart().startsWith(label));
  if (!line) {
    throw new Error(`missing process resource metric: ${label}`);
  }
  return line.slice(line.lastIndexOf(":") + 1).trim();
}

function keyedDecimal(raw, label) {
  return decimal(keyedValue(raw, label), label);
}

function keyedInteger(raw, label) {
  return integer(keyedValue(raw, label), label);
}

function elapsedSeconds(value) {
  const fields = value.split(":").map(Number);
  if (fields.some((field) => !Number.isFinite(field) || field < 0)) {
    throw new Error("invalid elapsed process resource metric");
  }
  return fields.reduce((total, field) => total * 60 + field, 0);
}

function decimal(value, label) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`invalid process resource metric: ${label}`);
  }
  return parsed;
}

function integer(value, label) {
  if (!/^[0-9]+$/.test(value ?? "")) {
    throw new Error(`invalid process resource metric: ${label}`);
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`out-of-range process resource metric: ${label}`);
  }
  return parsed;
}

function positive(value) {
  return Number.isFinite(value) && value > 0;
}

function positiveInteger(value) {
  return Number.isSafeInteger(value) && value > 0;
}

function normalizeOptional(value, acknowledgedRecords) {
  return value === null ? null : (value * 1_000_000) / acknowledgedRecords;
}
