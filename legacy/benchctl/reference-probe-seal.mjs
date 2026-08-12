// Fail-closed sealing of one raw-librdkafka sustainable-capacity probe.

import { createHash } from "node:crypto";
import { readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { relative, resolve } from "node:path";

import {
  CANONICAL_CAPACITY_SLO,
  evaluateCapacitySlo,
  isCanonicalCapacitySlo,
} from "./capacity-slo.mjs";
import {
  benchmarkEnvironmentIdentity,
  captureBenchmarkEnvironment,
} from "./environment.mjs";
import { compressEvidenceFile } from "./evidence-compression.mjs";
import {
  validateFixedLatencyCsv,
  validateFixedProducerResult,
} from "./fixed-validation.mjs";
import { summarizeLibrdkafkaStatistics } from "./librdkafka-statistics.mjs";
import { readProcessResources } from "./process-resources.mjs";
import { positiveInteger, validateVerification } from "./validation.mjs";

const args = process.argv.slice(2);
if (args.length !== 18) {
  throw new Error("reference-probe-seal.mjs received an incomplete identity");
}
const [
  bundle,
  runId,
  recordsText,
  warmupText,
  payloadText,
  partitionsText,
  outstandingText,
  offeredRateText,
  callersText,
  windowSecondsText,
  warmupSecondsText,
  correctedP99MsText,
  scheduleDelayP99MsText,
  drainTailMsText,
  queueSlopePercentText,
  minimumQueueSamplesText,
  bootstrap,
  brokerVersion,
] = args;
const records = positiveInteger("records", recordsText);
const warmup = positiveInteger("warmup", warmupText);
const payloadBytes = positiveInteger("payload bytes", payloadText);
const partitions = positiveInteger("partitions", partitionsText);
const maxOutstanding = positiveInteger("max outstanding", outstandingText);
const offeredRate = positiveInteger("offered rate", offeredRateText);
const callers = positiveInteger("callers", callersText);
const windowSeconds = positiveInteger("window seconds", windowSecondsText);
const warmupSeconds = positiveInteger("warmup seconds", warmupSecondsText);
const correctedP99Ms = positiveInteger("corrected p99 milliseconds", correctedP99MsText);
const scheduleDelayP99Ms = positiveInteger(
  "schedule-delay p99 milliseconds",
  scheduleDelayP99MsText,
);
const drainTailMs = positiveInteger("drain-tail milliseconds", drainTailMsText);
const queueSlopePercent = positiveInteger(
  "queue-slope percentage",
  queueSlopePercentText,
);
const minimumQueueSamples = positiveInteger(
  "minimum queue samples",
  minimumQueueSamplesText,
);
if (callers !== 4 || records !== offeredRate * windowSeconds) {
  throw new Error("reference capacity probes require four callers and a whole window");
}

const topicPrefix = `kafkars-reference-${runId}`;
const topics = {
  measured: `${topicPrefix}-measured`,
  warmup: `${topicPrefix}-warmup`,
};
const settings = {
  acks: "all",
  idempotence: true,
  compression: "none",
  linger_ms: 5,
  batch_records: 256,
  batch_bytes: 65_536,
  request_bytes: 1_048_576,
  max_in_flight_requests_per_broker: 5,
  queue_bytes: 67_108_864,
  max_outstanding_records: maxOutstanding,
  retry_max_replacements: 600,
  retry_backoff_ms: 100,
  explicit_balanced_partitioning: true,
  admission_shape: "public-batch",
  completion_shape: "aggregate-batch-terminal",
};
const parse = (path) => JSON.parse(readFileSync(path, "utf8"));
const result = parse(resolve(bundle, "result.json"));
const verification = {
  measured: parse(resolve(bundle, "verification.json")),
  warmup: parse(resolve(bundle, "warmup-verification.json")),
};
validateFixedProducerResult(result, {
  adapter: "librdkafka-c",
  topic: topics.measured,
  runId,
  records,
  payloadBytes,
  offeredRate,
  callers,
  settings,
});
validateFixedLatencyCsv(resolve(bundle, "latency.csv"), records, offeredRate);
validateVerification(verification.measured, topics.measured, records, partitions);
validateVerification(verification.warmup, topics.warmup, warmup, partitions);
const native = summarizeLibrdkafkaStatistics(
  resolve(bundle, "client-metrics.jsonl"),
  { topic: topics.measured, records, partitions, maxInFlightPerBroker: 5 },
);
const processResources = readProcessResources(
  resolve(bundle, "process-resources.txt"),
  result.acknowledged_records,
);
writeJson(resolve(bundle, "native-summary.json"), native);

const slo = {
  corrected_p99_ns: correctedP99Ms * 1_000_000,
  schedule_delay_p99_ns: scheduleDelayP99Ms * 1_000_000,
  drain_tail_ns: drainTailMs * 1_000_000,
  queue_slope_floor_records_per_second:
    CANONICAL_CAPACITY_SLO.queue_slope_floor_records_per_second,
  queue_slope_fraction_of_offered_rate: queueSlopePercent / 100,
  minimum_queue_samples: minimumQueueSamples,
};
const assessment = evaluateCapacitySlo(result, native, maxOutstanding, slo);
const repository = resolve(new URL("../..", import.meta.url).pathname);
const environment = captureBenchmarkEnvironment(repository, {
  adapterVersion: result.adapter_version,
  brokerVersion,
  bootstrap,
});
writeJson(resolve(bundle, "environment.json"), environment);
const summary = {
  schema: "kafkars.librdkafka-capacity-probe.v2",
  probe_id: runId,
  valid: true,
  sustainable: assessment.sustainable,
  claim_eligible: false,
  environment_identity_sha256: benchmarkEnvironmentIdentity(environment),
  reference_client: {
    adapter: "librdkafka-c",
    version: result.adapter_version,
    api: "rd_kafka_produce_batch",
  },
  workload: {
    mode: "scheduled-open-loop-fixed-rate",
    offered_records_per_second: offeredRate,
    window_seconds: windowSeconds,
    warmup_seconds: warmupSeconds,
    records,
    warmup_records: warmup,
    payload_bytes: payloadBytes,
    partitions,
    callers_per_producer: callers,
    max_outstanding_records: maxOutstanding,
  },
  slo: {
    canonical: isCanonicalCapacitySlo(slo),
    ...assessment,
  },
  exclusion_reasons: [
    "one reference-client window rather than a bracketed repeated capacity curve",
    "reference capacity alone does not compare kafkars with librdkafka",
    "process CPU and peak memory are diagnostic until broker and host classification is calibrated",
  ],
  result,
  process_resources: processResources,
  native_metrics: native,
  verification,
};
writeJson(resolve(bundle, "probe-summary.json"), summary);
await compressEvidenceFile(resolve(bundle, "latency.csv"));
await compressEvidenceFile(resolve(bundle, "client-metrics.jsonl"));
writeChecksums(bundle);
console.log(bundle);

function writeJson(path, value) {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
}

function writeChecksums(root) {
  const evidence = listFiles(root)
    .filter((path) => !path.endsWith("checksums.txt"))
    .sort();
  writeFileSync(
    resolve(root, "checksums.txt"),
    `${evidence
      .map((path) =>
        `${createHash("sha256").update(readFileSync(path)).digest("hex")}  ${relative(root, path)}`,
      )
      .join("\n")}\n`,
  );
}

function listFiles(root) {
  return readdirSync(root).flatMap((entry) => {
    const path = resolve(root, entry);
    return statSync(path).isDirectory() ? listFiles(path) : [path];
  });
}
