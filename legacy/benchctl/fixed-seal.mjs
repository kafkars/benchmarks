// Fail-closed sealing of one API-matched scheduled fixed-load pair.

import { createHash } from "node:crypto";
import { readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { relative, resolve } from "node:path";

import {
  validateFixedLatencyCsv,
  validateFixedProducerResult,
} from "./fixed-validation.mjs";
import {
  benchmarkEnvironmentIdentity,
  captureBenchmarkEnvironment,
} from "./environment.mjs";
import { compressEvidenceFile } from "./evidence-compression.mjs";
import { summarizeLibrdkafkaStatistics } from "./librdkafka-statistics.mjs";
import { readProcessResources } from "./process-resources.mjs";
import {
  positiveInteger,
  validateExecutionOrder,
  validateKafkarsNativeMetrics,
  validateVerification,
} from "./validation.mjs";

const [
  bundle,
  runId,
  order,
  recordsText,
  warmupText,
  payloadText,
  partitionsText,
  outstandingText,
  offeredRateText,
  callersText,
  bootstrap,
  brokerVersion,
] = process.argv.slice(2);
if (!brokerVersion) {
  throw new Error("fixed-seal.mjs received an incomplete result identity");
}
const records = positiveInteger("records", recordsText);
const warmup = positiveInteger("warmup", warmupText, true);
const payloadBytes = positiveInteger("payload bytes", payloadText);
const partitions = positiveInteger("partitions", partitionsText);
const maxOutstanding = positiveInteger("max outstanding", outstandingText);
const offeredRate = positiveInteger("offered rate", offeredRateText);
const callers = positiveInteger("callers", callersText);
const executionOrder = validateExecutionOrder(order);
if (callers !== 4) {
  throw new Error("fixed-load headline requires exactly four callers");
}

const parse = (path) => JSON.parse(readFileSync(path, "utf8"));
const adapters = ["kafkars", "librdkafka-c"];
const topicPrefix = `kafkars-fixed-${runId}`;
const topics = {
  kafkars: {
    measured: `${topicPrefix}-kafkars`,
    warmup: `${topicPrefix}-kafkars-warmup`,
  },
  "librdkafka-c": {
    measured: `${topicPrefix}-librdkafka`,
    warmup: `${topicPrefix}-librdkafka-warmup`,
  },
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
const results = {};
const verification = {};
const processResources = {};
for (const adapter of adapters) {
  const directory = resolve(bundle, "adapters", adapter);
  results[adapter] = parse(resolve(directory, "result.json"));
  verification[adapter] = {
    measured: parse(resolve(directory, "verification.json")),
    warmup: parse(resolve(directory, "warmup-verification.json")),
  };
  validateFixedProducerResult(results[adapter], {
    adapter,
    topic: topics[adapter].measured,
    runId,
    records,
    payloadBytes,
    offeredRate,
    callers,
    settings,
  });
  validateFixedLatencyCsv(resolve(directory, "latency.csv"), records, offeredRate);
  validateVerification(
    verification[adapter].measured,
    topics[adapter].measured,
    records,
    partitions,
  );
  validateVerification(
    verification[adapter].warmup,
    topics[adapter].warmup,
    warmup,
    partitions,
  );
  processResources[adapter] = readProcessResources(
    resolve(directory, "process-resources.txt"),
    results[adapter].acknowledged_records,
  );
}

validateKafkarsNativeMetrics(results.kafkars, records, 5, 3);
const librdkafkaNative = summarizeLibrdkafkaStatistics(
  resolve(bundle, "adapters", "librdkafka-c", "client-metrics.jsonl"),
  {
    topic: topics["librdkafka-c"].measured,
    records,
    partitions,
    maxInFlightPerBroker: 5,
  },
);
const repository = resolve(new URL("../..", import.meta.url).pathname);
const environment = captureBenchmarkEnvironment(repository, {
  adapterVersion: results["librdkafka-c"].adapter_version,
  brokerVersion,
  bootstrap,
});
writeJson(resolve(bundle, "environment.json"), environment);
writeJson(
  resolve(bundle, "adapters", "librdkafka-c", "native-summary.json"),
  librdkafkaNative,
);

const summary = {
  schema: "kafkars.producer-fixed-comparison.v2",
  run_id: runId,
  valid: true,
  claim_eligible: false,
  exclusion_reasons: [
    "one fixed-load point rather than the predeclared 25, 50, 75, and 90 percent matrix",
    "offered rate is diagnostic rather than derived from a valid librdkafka capacity curve",
    "one paired repetition rather than five statistically credible paired blocks",
    "process CPU and peak memory are diagnostic until broker and host classification is calibrated",
  ],
  execution_order: executionOrder,
  environment_identity_sha256: benchmarkEnvironmentIdentity(environment),
  workload: {
    mode: "scheduled-open-loop-fixed-rate",
    records,
    warmup_records: warmup,
    payload_bytes: payloadBytes,
    partitions,
    max_outstanding_records: maxOutstanding,
    offered_records_per_second: offeredRate,
    callers_per_producer: callers,
  },
  diagnostic_ratios: {
    uncorrected_p99_kafkars_over_librdkafka:
      results.kafkars.latency_ns.uncorrected.p99 /
      results["librdkafka-c"].latency_ns.uncorrected.p99,
    corrected_p99_kafkars_over_librdkafka:
      results.kafkars.latency_ns.corrected.p99 /
      results["librdkafka-c"].latency_ns.corrected.p99,
    schedule_delay_p99_kafkars_over_librdkafka:
      results.kafkars.latency_ns.schedule_delay.p99 /
      results["librdkafka-c"].latency_ns.schedule_delay.p99,
  },
  native_shape: {
    kafkars: results.kafkars.native_metrics.producer_requests,
    librdkafka_c: librdkafkaNative,
  },
  process_resources: {
    kafkars: processResources.kafkars,
    librdkafka_c: processResources["librdkafka-c"],
  },
  results,
  verification,
};
writeJson(resolve(bundle, "summary.json"), summary);
for (const adapter of adapters) {
  await compressEvidenceFile(
    resolve(bundle, "adapters", adapter, "latency.csv"),
  );
}
await compressEvidenceFile(
  resolve(bundle, "adapters", "librdkafka-c", "client-metrics.jsonl"),
);
const evidence = listFiles(bundle)
  .filter((path) => !path.endsWith("checksums.txt"))
  .sort();
writeFileSync(
  resolve(bundle, "checksums.txt"),
  `${evidence
    .map((path) =>
      `${createHash("sha256").update(readFileSync(path)).digest("hex")}  ${relative(bundle, path)}`,
    )
    .join("\n")}\n`,
);
console.log(bundle);

function writeJson(path, value) {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
}

function listFiles(root) {
  return readdirSync(root).flatMap((entry) => {
    const path = resolve(root, entry);
    return statSync(path).isDirectory() ? listFiles(path) : [path];
  });
}
