import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { dirname, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  positiveInteger,
  validateExecutionOrder,
  validateKafkarsNativeMetrics,
  validateLatencyCsv,
  validateProducerResult,
  validateVerification,
} from "./validation.mjs";
import { summarizeLibrdkafkaStatistics } from "./librdkafka-statistics.mjs";
import { renderResolvedWorkload } from "./workload.mjs";

const [
  bundle,
  runId,
  order,
  bootstrap,
  recordsText,
  warmupText,
  payloadText,
  partitionsText,
  outstandingText,
  brokerVersion,
] = process.argv.slice(2);
if (
  !bundle ||
  !runId ||
  !order ||
  !bootstrap ||
  !recordsText ||
  !warmupText ||
  !payloadText ||
  !partitionsText ||
  !outstandingText ||
  !brokerVersion
) {
  throw new Error("seal.mjs received an incomplete result identity");
}

const records = positiveInteger("records", recordsText);
const warmupRecords = positiveInteger(
  "warmup records",
  warmupText,
  true,
);
const payloadBytes = positiveInteger("payload bytes", payloadText);
const partitions = positiveInteger("partitions", partitionsText);
const maxOutstanding = positiveInteger("max outstanding", outstandingText);
const executionOrder = validateExecutionOrder(order);

const repo = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const parse = (path) => JSON.parse(readFileSync(path, "utf8"));
const adapters = ["kafkars", "librdkafka-c"];
const results = Object.fromEntries(
  adapters.map((adapter) => [
    adapter,
    parse(resolve(bundle, "adapters", adapter, "result.json")),
  ]),
);
const verification = Object.fromEntries(
  adapters.map((adapter) => [
    adapter,
    {
      measured: parse(
        resolve(bundle, "adapters", adapter, "verification.json"),
      ),
      warmup: parse(
        resolve(bundle, "adapters", adapter, "warmup-verification.json"),
      ),
    },
  ]),
);

const expectedSettings = {
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
const topicPrefix = `kafkars-bench-${runId}`;
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
for (const adapter of adapters) {
  validateProducerResult(
    results[adapter],
    adapter,
    topics[adapter].measured,
    runId,
    records,
    payloadBytes,
    expectedSettings,
  );
  validateVerification(
    verification[adapter].measured,
    topics[adapter].measured,
    records,
    partitions,
  );
  validateVerification(
    verification[adapter].warmup,
    topics[adapter].warmup,
    warmupRecords,
    partitions,
  );
  validateLatencyCsv(
    resolve(bundle, "adapters", adapter, "latency.csv"),
    records,
  );
}

const command = (program, args, cwd = repo) => {
  try {
    return execFileSync(program, args, {
      cwd,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    }).trim();
  } catch {
    return "unavailable";
  }
};
const gitState = (cwd) => ({
  commit: command("git", ["rev-parse", "HEAD"], cwd),
  dirty: command("git", ["status", "--porcelain"], cwd) !== "",
});
const sibling = (name) => resolve(repo, "..", name);
const clientRoot =
  process.env.KAFKA_BENCH_CLIENT_ROOT ?? resolve(repo, "..", "kafka-client");
const environment = {
  schema: "kafkars.benchmark-environment.v1",
  captured_at: new Date().toISOString(),
  source: {
    kafka_client: gitState(clientRoot),
    kafka_driver: gitState(sibling("kafka-driver")),
    kafka_protocol: gitState(sibling("kafka-protocol")),
    librdkafka: {
      version: results["librdkafka-c"].adapter_version,
      release: "v2.15.0",
      archive_sha256:
        "259015220cdca708afe838b5aa79ebf1a5fb710fb4179cf918d390aed85d5dbc",
      build_features: {
        ssl: false,
        gssapi: false,
        curl: false,
        external_zstd: false,
        external_lz4: false,
      },
    },
  },
  toolchain: {
    rustc: command("rustc", ["--version", "--verbose"]),
    cargo: command("cargo", ["--version"]),
    cc: command("cc", ["--version"]),
  },
  host: {
    uname: command("uname", ["-a"]),
    cpu: command("sysctl", ["-n", "machdep.cpu.brand_string"]),
    logical_cpus: command("sysctl", ["-n", "hw.logicalcpu"]),
    memory_bytes: command("sysctl", ["-n", "hw.memsize"]),
  },
  broker: {
    version: brokerVersion,
    bootstrap,
    lifecycle: "externally managed by the caller",
  },
};
writeJson(resolve(bundle, "environment.json"), environment);

const workload = {
  records,
  warmup_records: warmupRecords,
  payload_bytes: payloadBytes,
  partitions,
  replication_factor: 3,
  min_in_sync_replicas: 2,
  max_outstanding_records: maxOutstanding,
  acks: "all",
  idempotence: true,
  compression: "none",
  linger_ms: 5,
  batch_records: 256,
  batch_bytes: 65_536,
  request_bytes: 1_048_576,
  max_in_flight_requests_per_broker: 5,
  retry_max_replacements: 600,
  retry_backoff_ms: 100,
  partitioning: "explicit round-robin",
  warmup_serialized_partition_primer_records: partitions,
  native_request_concurrency: {
    status: "matched-configured",
    kafkars: "max_in_flight_requests_per_broker=5",
    librdkafka_c: "max.in.flight.requests.per.connection=5",
  },
};
writeFileSync(
  resolve(bundle, "workload.toml"),
  renderResolvedWorkload(workload),
);
writeJson(resolve(bundle, "adapter-config.json"), {
  schema: "kafkars.benchmark-adapter-config.v1",
  normalized_settings: expectedSettings,
  native_request_concurrency: workload.native_request_concurrency,
});

const kafkarsRate = results.kafkars.acknowledged_records_per_second;
const baselineRate =
  results["librdkafka-c"].acknowledged_records_per_second;
const kafkarsRequests = results.kafkars.native_metrics?.producer_requests;
validateKafkarsNativeMetrics(results.kafkars, records, 5, 3);
const librdkafkaNativeMetrics = summarizeLibrdkafkaStatistics(
  resolve(bundle, "adapters", "librdkafka-c", "client-metrics.jsonl"),
  {
    topic: topics["librdkafka-c"].measured,
    records,
    partitions,
    maxInFlightPerBroker: 5,
  },
);
writeJson(
  resolve(bundle, "adapters", "librdkafka-c", "native-summary.json"),
  librdkafkaNativeMetrics,
);
const summary = {
  schema: "kafkars.producer-comparison.v1",
  run_id: runId,
  valid: true,
  claim_eligible: false,
  exclusion_reasons: [
    "one diagnostic paired repetition rather than five predeclared repetitions",
    "one closed-loop capacity point rather than a sustainable capacity curve",
    "developer host without calibrated broker, CPU, RSS, thermal, or noise classification",
    "the full four-caller open-loop fixed-load matrix is not yet implemented",
  ],
  execution_order: executionOrder,
  workload,
  diagnostic_ratios: {
    acknowledged_goodput_kafkars_over_librdkafka: kafkarsRate / baselineRate,
    p99_latency_kafkars_over_librdkafka:
      results.kafkars.latency_ns.p99 /
      results["librdkafka-c"].latency_ns.p99,
  },
  diagnostics: {
    kafkars_records_per_produce_request:
      kafkarsRequests.records / kafkarsRequests.requests,
    kafkars_partition_batches_per_produce_request:
      kafkarsRequests.partition_batches / kafkarsRequests.requests,
    kafkars_records_per_partition_batch:
      kafkarsRequests.records / kafkarsRequests.partition_batches,
  },
  native_shape: {
    status: "captured",
    kafkars: {
      produce_requests: kafkarsRequests.requests,
      partition_batches: kafkarsRequests.partition_batches,
      records_per_produce_request:
        kafkarsRequests.records / kafkarsRequests.requests,
      partition_batches_per_produce_request:
        kafkarsRequests.partition_batches / kafkarsRequests.requests,
      records_per_partition_batch:
        kafkarsRequests.records / kafkarsRequests.partition_batches,
      peak_in_flight_requests: kafkarsRequests.peak_in_flight_requests,
      peak_in_flight_requests_per_broker:
        kafkarsRequests.peak_in_flight_requests_per_broker,
    },
    librdkafka_c: librdkafkaNativeMetrics,
  },
  results,
  verification,
};
writeJson(resolve(bundle, "summary.json"), summary);

const evidence = listFiles(bundle)
  .filter((path) => !path.endsWith("checksums.txt"))
  .sort();
const checksums = evidence
  .map((path) => {
    const bytes = readFileSync(path);
    const digest = createHash("sha256").update(bytes).digest("hex");
    return `${digest}  ${relative(bundle, path)}`;
  })
  .join("\n");
writeFileSync(resolve(bundle, "checksums.txt"), `${checksums}\n`);
console.log(bundle);

function writeJson(path, value) {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
}

function listFiles(root) {
  const files = [];
  for (const entry of readdirSync(root)) {
    const path = resolve(root, entry);
    if (statSync(path).isDirectory()) {
      files.push(...listFiles(path));
    } else {
      files.push(path);
    }
  }
  return files;
}
