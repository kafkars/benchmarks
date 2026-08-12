// Repeated raw-librdkafka capacity search and sealed aggregate evidence.

import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { relative, resolve } from "node:path";

import { searchSustainableCapacity } from "./capacity-search.mjs";
import { assessCapacityEvidenceContract } from "./capacity-slo.mjs";
import {
  COEFFICIENT_OF_VARIATION_BUDGET,
  MINIMUM_PAIRED_REPETITIONS,
  summarizePositiveValues,
} from "./statistics.mjs";
import { positiveInteger } from "./validation.mjs";
import { archiveIncompleteDirectory } from "./resume.mjs";

const [
  repository,
  bootstrap,
  suite,
  repetitionsText,
  initialRateText,
  minRateText,
  maxRateText,
  growthFactorText,
  resolutionPercentText,
] = process.argv.slice(2);
if (!resolutionPercentText) {
  throw new Error("reference-capacity-runner.mjs received an incomplete search");
}
const repetitions = positiveInteger("repetitions", repetitionsText);
const configuration = {
  initialRate: positiveInteger("initial rate", initialRateText),
  minRate: positiveInteger("minimum rate", minRateText),
  maxRate: positiveInteger("maximum rate", maxRateText),
  growthFactor: positiveInteger("growth factor", growthFactorText),
  resolutionFraction:
    positiveInteger("resolution percentage", resolutionPercentText) / 100,
};
const probeRoot = resolve(suite, "probes");
mkdirSync(probeRoot, { recursive: true });
const rateEvidence = new Map();
const environmentIdentities = new Set();
const search = searchSustainableCapacity(configuration, evaluateRate);
const candidate = rateEvidence.get(search.sustainable_records_per_second);
const correctedP99 = summarizePositiveValues(
  candidate.probes.map((probe) => probe.slo.observed.corrected_p99_ns),
);
const scheduleDelayP99 = summarizePositiveValues(
  candidate.probes.map((probe) =>
    Math.max(1, probe.slo.observed.schedule_delay_p99_ns),
  ),
);
const processCpu = summarizePositiveValues(
  candidate.probes.map(
    (probe) =>
      probe.process_resources
        .cpu_core_seconds_per_million_acknowledged_records,
  ),
);
const processPeakMemory = summarizePositiveValues(
  candidate.probes.map((probe) => probe.process_resources.peak_memory_bytes),
);
const processRss = summarizePositiveValues(
  candidate.probes.map((probe) => probe.process_resources.max_rss_bytes),
);
const statisticalCredibility = {
  minimum_repetitions: MINIMUM_PAIRED_REPETITIONS,
  coefficient_of_variation_budget: COEFFICIENT_OF_VARIATION_BUDGET,
  enough_repetitions: repetitions >= MINIMUM_PAIRED_REPETITIONS,
  corrected_p99_inside_noise_budget: insideNoiseBudget(correctedP99),
  schedule_delay_p99_inside_noise_budget: insideNoiseBudget(scheduleDelayP99),
  process_cpu_inside_noise_budget: insideNoiseBudget(processCpu),
  process_peak_memory_inside_noise_budget: insideNoiseBudget(processPeakMemory),
  process_rss_inside_noise_budget: insideNoiseBudget(processRss),
};
statisticalCredibility.valid =
  statisticalCredibility.enough_repetitions &&
  statisticalCredibility.corrected_p99_inside_noise_budget &&
  statisticalCredibility.schedule_delay_p99_inside_noise_budget &&
  statisticalCredibility.process_cpu_inside_noise_budget &&
  statisticalCredibility.process_peak_memory_inside_noise_budget;
const canonicalSlo = [...rateEvidence.values()].every((rate) =>
  rate.probes.every((probe) => probe.slo.canonical === true),
);
const evidenceCredibility = assessCapacityEvidenceContract({
  canonicalSlo,
  search: configuration,
  window: {
    windowSeconds: candidate.probes[0].workload.window_seconds,
    warmupSeconds: candidate.probes[0].workload.warmup_seconds,
  },
  bracketWidthFraction: search.bracket_width_fraction,
  statisticalCredibility: statisticalCredibility.valid,
});
const environment = JSON.parse(
  readFileSync(resolve(suite, candidate.directories[0], "environment.json"), "utf8"),
);
if (
  candidate.probes[0].environment_identity_sha256 !==
  [...environmentIdentities][0]
) {
  throw new Error("reference capacity aggregate selected the wrong environment");
}
writeJson(resolve(suite, "environment.json"), environment);
const summary = {
  schema: "kafkars.librdkafka-capacity-curve.v2",
  valid: true,
  claim_eligible: false,
  reference_client: "raw-librdkafka-c",
  workload: candidate.probes[0].workload,
  search: {
    method: "geometric-bracket-with-binary-refinement",
    repetitions_per_rate: repetitions,
    initial_records_per_second: configuration.initialRate,
    minimum_records_per_second: configuration.minRate,
    maximum_records_per_second: configuration.maxRate,
    growth_factor: configuration.growthFactor,
    resolution_fraction: configuration.resolutionFraction,
    execution_order: search.evaluations.map((entry) => entry.rate),
  },
  capacity: {
    sustainable_records_per_second: search.sustainable_records_per_second,
    first_failing_records_per_second:
      search.first_failing_records_per_second,
    bracket_width_fraction: search.bracket_width_fraction,
    derived_fixed_rates: search.derived_fixed_rates,
  },
  canonical_slo: canonicalSlo,
  evidence_credibility: evidenceCredibility,
  statistical_credibility: {
    ...statisticalCredibility,
    candidate_corrected_p99_ns: correctedP99,
    candidate_schedule_delay_p99_ns: scheduleDelayP99,
    candidate_cpu_core_seconds_per_million_acknowledged_records: processCpu,
    candidate_peak_memory_bytes: processPeakMemory,
    candidate_max_rss_bytes: processRss,
  },
  exclusion_reasons: [
    "reference capacity alone does not compare kafkars with librdkafka",
    "the predeclared 25, 50, 75, and 90 percent comparison matrix has not run",
    "process CPU and peak memory are diagnostic until broker and host classification is calibrated",
  ],
  curve: [...rateEvidence.values()]
    .toSorted((left, right) => left.rate - right.rate)
    .map((rate) => ({
      rate: rate.rate,
      sustainable: rate.sustainable,
      sustainable_repetitions: rate.probes.filter((probe) => probe.sustainable)
        .length,
      repetitions: rate.probes.length,
      directories: rate.directories,
    })),
  environment_identity_sha256: [...environmentIdentities][0],
  environment: {
    broker_version: environment.broker.version,
    bootstrap: environment.broker.bootstrap,
  },
};
writeJson(resolve(suite, "capacity-summary.json"), summary);
writeChecksums(suite);
console.log(suite);

function evaluateRate(rate) {
  const probes = [];
  const directories = [];
  for (let repetition = 1; repetition <= repetitions; repetition += 1) {
    const directory = resolve(
      probeRoot,
      `rate-${String(rate).padStart(9, "0")}`,
      `rep-${String(repetition).padStart(2, "0")}`,
    );
    const summaryPath = resolve(directory, "probe-summary.json");
    const reused = existsSync(summaryPath);
    if (!reused) {
      if (existsSync(directory)) {
        archiveIncompleteDirectory(directory, "probe");
      }
      mkdirSync(resolve(directory, ".."), { recursive: true });
      execFileSync(
        resolve(repository, "scripts/bench-producer-reference-probe"),
        [bootstrap, String(rate), directory],
        {
          cwd: repository,
          env: process.env,
          stdio: ["ignore", "pipe", "inherit"],
        },
      );
    }
    const probe = JSON.parse(
      readFileSync(summaryPath, "utf8"),
    );
    if (
      probe.schema !== "kafkars.librdkafka-capacity-probe.v2" ||
      probe.valid !== true ||
      probe.workload?.offered_records_per_second !== rate
    ) {
      throw new Error(`reference capacity probe at ${rate} is invalid`);
    }
    probes.push(probe);
    environmentIdentities.add(probe.environment_identity_sha256);
    if (environmentIdentities.size !== 1) {
      throw new Error("reference capacity environment changed between probes");
    }
    directories.push(relative(suite, directory));
    process.stderr.write(
      `reference capacity ${rate} records/s repetition ${repetition}/${repetitions}: ${probe.sustainable ? "sustainable" : "overloaded"} (${reused ? "reused" : "recorded"})\n`,
    );
  }
  const evidence = {
    rate,
    sustainable: probes.every((probe) => probe.sustainable === true),
    probes,
    directories,
  };
  rateEvidence.set(rate, evidence);
  return evidence.sustainable;
}

function insideNoiseBudget(summary) {
  return (
    summary.coefficient_of_variation !== null &&
    summary.coefficient_of_variation <= COEFFICIENT_OF_VARIATION_BUDGET
  );
}

function writeJson(path, value) {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
}

function writeChecksums(root) {
  const evidence = listFiles(root)
    .filter((path) => !path.endsWith("capacity-checksums.txt"))
    .sort();
  writeFileSync(
    resolve(root, "capacity-checksums.txt"),
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
