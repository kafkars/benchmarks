// Fail-closed aggregate for balanced scheduled-load paired blocks.

import { createHash } from "node:crypto";
import {
  readdirSync,
  readFileSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { relative, resolve } from "node:path";

import {
  assessFixedLoadCredibility,
  assessResourceCredibility,
  summarizeRatios,
} from "./statistics.mjs";
import { positiveInteger } from "./validation.mjs";

const [suite, repetitionsText] = process.argv.slice(2);
if (!suite || !repetitionsText) {
  throw new Error("fixed-suite.mjs requires a result root and repetition count");
}
const repetitions = positiveInteger("repetitions", repetitionsText);
const pairs = [];
const runIds = new Set();
let workloadIdentity;
let environmentIdentity;

for (let index = 1; index <= repetitions; index += 1) {
  const directory = resolve(suite, `pair-${String(index).padStart(2, "0")}`);
  const summary = JSON.parse(
    readFileSync(resolve(directory, "summary.json"), "utf8"),
  );
  const expectedOrder =
    index % 2 === 1
      ? ["kafkars", "librdkafka-c"]
      : ["librdkafka-c", "kafkars"];
  const currentWorkloadIdentity = JSON.stringify(summary.workload);
  if (
    summary.schema !== "kafkars.producer-fixed-comparison.v2" ||
    summary.valid !== true ||
    summary.claim_eligible !== false ||
    !/^[0-9a-f]{64}$/.test(summary.environment_identity_sha256) ||
    JSON.stringify(summary.execution_order) !== JSON.stringify(expectedOrder) ||
    runIds.has(summary.run_id) ||
    (workloadIdentity !== undefined &&
      currentWorkloadIdentity !== workloadIdentity) ||
    (environmentIdentity !== undefined &&
      summary.environment_identity_sha256 !== environmentIdentity)
  ) {
    throw new Error(`pair ${index} violates the balanced fixed-load suite contract`);
  }
  workloadIdentity ??= currentWorkloadIdentity;
  environmentIdentity ??= summary.environment_identity_sha256;
  runIds.add(summary.run_id);
  const kafkars = summary.results.kafkars;
  const librdkafka = summary.results["librdkafka-c"];
  const kafkarsRequests = summary.native_shape.kafkars.requests;
  const librdkafkaRequests =
    summary.native_shape.librdkafka_c.requests.produce;
  const kafkarsResources = summary.process_resources?.kafkars;
  const librdkafkaResources = summary.process_resources?.librdkafka_c;
  requirePositiveInteger(kafkarsRequests, `pair ${index} kafkars request count`);
  requirePositiveInteger(
    librdkafkaRequests,
    `pair ${index} librdkafka request count`,
  );
  requirePositiveNumber(
    kafkarsResources?.cpu_core_seconds_per_million_acknowledged_records,
    `pair ${index} kafkars CPU`,
  );
  requirePositiveNumber(
    librdkafkaResources?.cpu_core_seconds_per_million_acknowledged_records,
    `pair ${index} librdkafka CPU`,
  );
  requirePositiveInteger(
    kafkarsResources?.max_rss_bytes,
    `pair ${index} kafkars RSS`,
  );
  requirePositiveInteger(
    librdkafkaResources?.max_rss_bytes,
    `pair ${index} librdkafka RSS`,
  );
  requirePositiveInteger(
    kafkarsResources?.peak_memory_bytes,
    `pair ${index} kafkars peak memory`,
  );
  requirePositiveInteger(
    librdkafkaResources?.peak_memory_bytes,
    `pair ${index} librdkafka peak memory`,
  );
  if (
    kafkarsResources.schema !== "kafkars.process-resources.v2" ||
    librdkafkaResources.schema !== "kafkars.process-resources.v2" ||
    kafkarsResources.peak_memory_method !==
      librdkafkaResources.peak_memory_method
  ) {
    throw new Error(`pair ${index} process memory methods do not match`);
  }
  pairs.push({
    index,
    directory: relative(suite, directory),
    run_id: summary.run_id,
    execution_order: summary.execution_order,
    uncorrected_p99_kafkars_over_librdkafka:
      kafkars.latency_ns.uncorrected.p99 /
      librdkafka.latency_ns.uncorrected.p99,
    corrected_p99_kafkars_over_librdkafka:
      kafkars.latency_ns.corrected.p99 /
      librdkafka.latency_ns.corrected.p99,
    schedule_delay_p99_kafkars_over_librdkafka:
      kafkars.latency_ns.schedule_delay.p99 /
      librdkafka.latency_ns.schedule_delay.p99,
    acknowledged_rate_kafkars_over_librdkafka:
      kafkars.acknowledged_records_per_second_including_drain /
      librdkafka.acknowledged_records_per_second_including_drain,
    produce_requests_librdkafka_over_kafkars:
      librdkafkaRequests / kafkarsRequests,
    cpu_core_seconds_kafkars_over_librdkafka:
      kafkarsResources.cpu_core_seconds_per_million_acknowledged_records /
      librdkafkaResources.cpu_core_seconds_per_million_acknowledged_records,
    peak_rss_kafkars_over_librdkafka:
      kafkarsResources.max_rss_bytes / librdkafkaResources.max_rss_bytes,
    peak_memory_kafkars_over_librdkafka:
      kafkarsResources.peak_memory_bytes /
      librdkafkaResources.peak_memory_bytes,
  });
}

const ratios = {
  uncorrected_p99_kafkars_over_librdkafka: summarize("uncorrected_p99"),
  corrected_p99_kafkars_over_librdkafka: summarize("corrected_p99"),
  schedule_delay_p99_kafkars_over_librdkafka: summarize("schedule_delay_p99"),
  acknowledged_rate_kafkars_over_librdkafka: summarize("acknowledged_rate"),
  produce_requests_librdkafka_over_kafkars: summarize("produce_requests"),
  cpu_core_seconds_kafkars_over_librdkafka: summarize("cpu_core_seconds"),
  peak_rss_kafkars_over_librdkafka: summarize("peak_rss"),
  peak_memory_kafkars_over_librdkafka: summarize("peak_memory"),
};
const resourceCredibility = assessResourceCredibility(
  ratios.cpu_core_seconds_kafkars_over_librdkafka,
  ratios.peak_memory_kafkars_over_librdkafka,
);
const aggregate = {
  schema: "kafkars.producer-fixed-comparison-suite.v2",
  valid: true,
  claim_eligible: false,
  repetitions,
  balance: {
    kafkars_first: Math.ceil(repetitions / 2),
    librdkafka_first: Math.floor(repetitions / 2),
  },
  workload: JSON.parse(workloadIdentity),
  environment_identity_sha256: environmentIdentity,
  exclusion_reasons: [
    "one diagnostic rate rather than the predeclared 25, 50, 75, and 90 percent matrix",
    "offered rate is not derived from a valid librdkafka sustainable-capacity curve",
    "process CPU and peak memory are diagnostic until broker and host classification is calibrated",
  ],
  statistical_credibility: assessFixedLoadCredibility(
    ratios.corrected_p99_kafkars_over_librdkafka,
    ratios.uncorrected_p99_kafkars_over_librdkafka,
  ),
  resource_credibility: resourceCredibility,
  paired_ratios: ratios,
  pairs,
};
writeJson(resolve(suite, "suite-summary.json"), aggregate);

const evidence = listFiles(suite)
  .filter((path) => !path.endsWith("suite-checksums.txt"))
  .sort();
writeFileSync(
  resolve(suite, "suite-checksums.txt"),
  `${evidence
    .map((path) =>
      `${createHash("sha256").update(readFileSync(path)).digest("hex")}  ${relative(suite, path)}`,
    )
    .join("\n")}\n`,
);
console.log(suite);

function summarize(prefix) {
  return summarizeRatios(
    pairs.map((pair) => pair[`${prefix}_kafkars_over_librdkafka`] ??
      pair[`${prefix}_librdkafka_over_kafkars`]),
  );
}

function requirePositiveNumber(value, label) {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    throw new Error(`${label} must be a positive number`);
  }
}

function requirePositiveInteger(value, label) {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${label} must be a positive integer`);
  }
}

function writeJson(path, value) {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
}

function listFiles(root) {
  return readdirSync(root).flatMap((entry) => {
    const path = resolve(root, entry);
    return statSync(path).isDirectory() ? listFiles(path) : [path];
  });
}
