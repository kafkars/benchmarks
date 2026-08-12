// Fail-closed aggregate for a balanced sequence of paired benchmark blocks.

import { createHash } from "node:crypto";
import {
  readdirSync,
  readFileSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { relative, resolve } from "node:path";

import {
  assessStatisticalCredibility,
  summarizeRatios,
} from "./statistics.mjs";
import { positiveInteger } from "./validation.mjs";

const [suite, repetitionsText] = process.argv.slice(2);
if (!suite || !repetitionsText) {
  throw new Error("suite.mjs requires a result root and repetition count");
}
const repetitions = positiveInteger("repetitions", repetitionsText);
const pairs = [];
const runIds = new Set();

for (let index = 1; index <= repetitions; index += 1) {
  const directory = resolve(suite, `pair-${String(index).padStart(2, "0")}`);
  const summary = JSON.parse(
    readFileSync(resolve(directory, "summary.json"), "utf8"),
  );
  const expectedOrder =
    index % 2 === 1
      ? ["kafkars", "librdkafka-c"]
      : ["librdkafka-c", "kafkars"];
  if (
    summary.valid !== true ||
    summary.claim_eligible !== false ||
    summary.native_shape?.status !== "captured" ||
    JSON.stringify(summary.execution_order) !== JSON.stringify(expectedOrder) ||
    runIds.has(summary.run_id)
  ) {
    throw new Error(`pair ${index} violates the balanced suite contract`);
  }
  runIds.add(summary.run_id);
  const kafkarsRequests = summary.native_shape.kafkars.produce_requests;
  const librdkafkaRequests =
    summary.native_shape.librdkafka_c.requests.produce;
  if (
    !Number.isSafeInteger(kafkarsRequests) ||
    kafkarsRequests <= 0 ||
    !Number.isSafeInteger(librdkafkaRequests) ||
    librdkafkaRequests <= 0
  ) {
    throw new Error(`pair ${index} has invalid native Produce request counts`);
  }
  pairs.push({
    index,
    directory: relative(suite, directory),
    run_id: summary.run_id,
    execution_order: summary.execution_order,
    acknowledged_goodput_kafkars_over_librdkafka:
      summary.diagnostic_ratios.acknowledged_goodput_kafkars_over_librdkafka,
    p99_latency_kafkars_over_librdkafka:
      summary.diagnostic_ratios.p99_latency_kafkars_over_librdkafka,
    produce_requests_librdkafka_over_kafkars:
      librdkafkaRequests / kafkarsRequests,
  });
}

const goodputRatios = summarizeRatios(
  pairs.map((pair) => pair.acknowledged_goodput_kafkars_over_librdkafka),
);
const p99Ratios = summarizeRatios(
  pairs.map((pair) => pair.p99_latency_kafkars_over_librdkafka),
);
const requestEfficiencyRatios = summarizeRatios(
  pairs.map((pair) => pair.produce_requests_librdkafka_over_kafkars),
);
const statisticalCredibility = assessStatisticalCredibility(
  goodputRatios,
  p99Ratios,
);

const aggregate = {
  schema: "kafkars.producer-comparison-suite.v1",
  valid: true,
  claim_eligible: false,
  repetitions,
  balance: {
    kafkars_first: Math.ceil(repetitions / 2),
    librdkafka_first: Math.floor(repetitions / 2),
  },
  exclusion_reasons: [
    "one closed-loop capacity point rather than a sustainable capacity curve",
    "developer host without calibrated broker, CPU, RSS, thermal, or noise classification",
    "the full four-caller open-loop fixed-load matrix is not yet implemented",
  ],
  statistical_credibility: statisticalCredibility,
  paired_ratios: {
    acknowledged_goodput_kafkars_over_librdkafka: goodputRatios,
    p99_latency_kafkars_over_librdkafka: p99Ratios,
    produce_requests_librdkafka_over_kafkars: requestEfficiencyRatios,
  },
  pairs,
};
writeJson(resolve(suite, "suite-summary.json"), aggregate);

const evidence = listFiles(suite)
  .filter((path) => !path.endsWith("suite-checksums.txt"))
  .sort();
const checksums = evidence
  .map((path) => {
    const digest = createHash("sha256")
      .update(readFileSync(path))
      .digest("hex");
    return `${digest}  ${relative(suite, path)}`;
  })
  .join("\n");
writeFileSync(resolve(suite, "suite-checksums.txt"), `${checksums}\n`);
console.log(suite);

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
