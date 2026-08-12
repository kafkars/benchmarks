// Execute and seal the four fixed-load suites derived from reference capacity.

import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { relative, resolve } from "node:path";

import { FIXED_LOAD_PERCENTAGES } from "./capacity-search.mjs";
import { assessFixedMatrix } from "./fixed-matrix.mjs";
import { archiveIncompleteDirectory } from "./resume.mjs";

const [repository, bootstrap, capacityPath, matrix] = process.argv.slice(2);
if (!matrix) {
  throw new Error("fixed-matrix-runner.mjs received an incomplete identity");
}
const capacityBytes = readFileSync(capacityPath);
const capacity = JSON.parse(capacityBytes);
if (
  capacity.schema !== "kafkars.librdkafka-capacity-curve.v2" ||
  capacity.valid !== true ||
  capacity.claim_eligible !== false ||
  !capacity.workload ||
  !capacity.capacity?.derived_fixed_rates
) {
  throw new Error("fixed-load matrix requires a valid sealed reference capacity");
}
mkdirSync(matrix, { recursive: true });
copyFileSync(capacityPath, resolve(matrix, "reference-capacity-summary.json"));
copyFileSync(
  resolve(capacityPath, "..", "environment.json"),
  resolve(matrix, "environment.json"),
);
const repetitions = capacity.search.repetitions_per_rate;
const workload = capacity.workload;
const points = [];
for (const percentage of FIXED_LOAD_PERCENTAGES) {
  const offeredRate = capacity.capacity.derived_fixed_rates[String(percentage)];
  const records = offeredRate * workload.window_seconds;
  const warmupRecords = offeredRate * workload.warmup_seconds;
  if (
    !Number.isSafeInteger(offeredRate) ||
    offeredRate <= 0 ||
    !Number.isSafeInteger(records) ||
    !Number.isSafeInteger(warmupRecords)
  ) {
    throw new Error(`derived ${percentage} percent fixed-load point is invalid`);
  }
  const directory = resolve(matrix, `load-${percentage}`);
  const summaryPath = resolve(directory, "suite-summary.json");
  const reused = existsSync(summaryPath);
  if (!reused) {
    if (existsSync(directory)) {
      archiveIncompleteDirectory(directory, "matrix point");
    }
    execFileSync(
      resolve(repository, "scripts/bench-producer-fixed-suite"),
      [bootstrap, directory],
      {
        cwd: repository,
        env: {
          ...process.env,
          KAFKARS_BENCH_BROKER_VERSION: String(
            capacity.environment.broker_version,
          ),
          KAFKARS_BENCH_REPETITIONS: String(repetitions),
          KAFKARS_BENCH_RECORDS: String(records),
          KAFKARS_BENCH_WARMUP_RECORDS: String(warmupRecords),
          KAFKARS_BENCH_PAYLOAD_BYTES: String(workload.payload_bytes),
          KAFKARS_BENCH_PARTITIONS: String(workload.partitions),
          KAFKARS_BENCH_MAX_OUTSTANDING: String(
            workload.max_outstanding_records,
          ),
          KAFKARS_BENCH_OFFERED_RATE: String(offeredRate),
          KAFKARS_BENCH_CALLERS: String(workload.callers_per_producer),
        },
        stdio: ["ignore", "pipe", "inherit"],
      },
    );
  }
  const suite = JSON.parse(
    readFileSync(summaryPath, "utf8"),
  );
  if (
    suite.schema !== "kafkars.producer-fixed-comparison-suite.v2" ||
    suite.valid !== true ||
    suite.repetitions !== repetitions ||
    suite.workload?.offered_records_per_second !== offeredRate ||
    suite.workload?.records !== records ||
    suite.environment_identity_sha256 !==
      capacity.environment_identity_sha256
  ) {
    throw new Error(`${percentage} percent fixed-load suite is invalid`);
  }
  points.push({
    percentage,
    offered_records_per_second: offeredRate,
    directory: relative(matrix, directory),
    statistical_credibility: suite.statistical_credibility.valid,
    corrected_p99:
      suite.paired_ratios.corrected_p99_kafkars_over_librdkafka,
    uncorrected_p99:
      suite.paired_ratios.uncorrected_p99_kafkars_over_librdkafka,
    request_efficiency:
      suite.paired_ratios.produce_requests_librdkafka_over_kafkars,
    resource_credibility: suite.resource_credibility.valid,
    cpu_core_seconds:
      suite.paired_ratios.cpu_core_seconds_kafkars_over_librdkafka,
    peak_memory:
      suite.paired_ratios.peak_memory_kafkars_over_librdkafka,
    peak_rss: suite.paired_ratios.peak_rss_kafkars_over_librdkafka,
  });
  process.stderr.write(
    `fixed matrix ${percentage}% at ${offeredRate} records/s: valid (${reused ? "reused" : "recorded"})\n`,
  );
}
const referenceCredible =
  capacity.evidence_credibility?.valid === true;
const assessment = assessFixedMatrix(points, referenceCredible);
const exclusionReasons = [
  "process CPU and peak memory are diagnostic until stable-runner thresholds are calibrated",
  "developer host without calibrated broker, thermal, or saturation classification",
  "one balanced producer workload rather than the complete primary matrix",
];
if (!capacity.canonical_slo) {
  exclusionReasons.unshift("reference capacity used a noncanonical SLO profile");
}
if (capacity.evidence_credibility?.canonical_search !== true) {
  exclusionReasons.unshift("reference capacity used a noncanonical search profile");
}
if (capacity.evidence_credibility?.canonical_window !== true) {
  exclusionReasons.unshift("reference capacity used a noncanonical stable window");
}
if (!capacity.statistical_credibility?.valid) {
  exclusionReasons.unshift("reference capacity lacks statistical credibility");
}
if (!points.every((point) => point.statistical_credibility)) {
  exclusionReasons.unshift("one or more fixed-load points lack statistical credibility");
}
if (!points.every((point) => point.resource_credibility)) {
  exclusionReasons.unshift("one or more fixed-load points lack resource credibility");
}
const summary = {
  schema: "kafkars.producer-fixed-matrix.v2",
  valid: true,
  claim_eligible: false,
  capacity_reference: {
    sha256: createHash("sha256").update(capacityBytes).digest("hex"),
    sustainable_records_per_second:
      capacity.capacity.sustainable_records_per_second,
    first_failing_records_per_second:
      capacity.capacity.first_failing_records_per_second,
    copied_summary: "reference-capacity-summary.json",
  },
  workload: {
    window_seconds: workload.window_seconds,
    warmup_seconds: workload.warmup_seconds,
    payload_bytes: workload.payload_bytes,
    partitions: workload.partitions,
    callers_per_producer: workload.callers_per_producer,
    max_outstanding_records: workload.max_outstanding_records,
    repetitions_per_point: repetitions,
  },
  assessment,
  exclusion_reasons: exclusionReasons,
  points,
};
writeJson(resolve(matrix, "matrix-summary.json"), summary);
writeChecksums(matrix);
console.log(matrix);

function writeJson(path, value) {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
}

function writeChecksums(root) {
  const evidence = listFiles(root)
    .filter((path) => !path.endsWith("matrix-checksums.txt"))
    .sort();
  writeFileSync(
    resolve(root, "matrix-checksums.txt"),
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
