// Deterministic descriptive and bootstrap statistics for paired evidence.

const BOOTSTRAP_RESAMPLES = 50_000;
const BOOTSTRAP_SEED = 0x6b_61_66_6b;
export const MINIMUM_PAIRED_REPETITIONS = 5;
export const COEFFICIENT_OF_VARIATION_BUDGET = 0.05;

export function summarizeRatios(values) {
  return summarizePositiveValues(values, "paired-block-percentile-bootstrap");
}

export function summarizePositiveValues(
  values,
  bootstrapMethod = "repetition-percentile-bootstrap",
) {
  if (!Array.isArray(values) || values.length === 0) {
    throw new Error("at least one finite positive ratio is required");
  }
  if (!values.every((value) => Number.isFinite(value) && value > 0)) {
    throw new Error("ratios must be finite positive numbers");
  }
  const sorted = values.toSorted((left, right) => left - right);
  const arithmeticMean =
    values.reduce((total, value) => total + value, 0) / values.length;
  return {
    repetitions: values.length,
    geometric_mean: Math.exp(
      values.reduce((total, value) => total + Math.log(value), 0) /
        values.length,
    ),
    arithmetic_mean: arithmeticMean,
    median: median(sorted),
    minimum: sorted[0],
    maximum: sorted.at(-1),
    coefficient_of_variation:
      values.length < 2
        ? null
        : sampleStandardDeviation(values, arithmeticMean) / arithmeticMean,
    confidence_interval_95: bootstrapGeometricMean(values, bootstrapMethod),
  };
}

export function median(values) {
  if (!Array.isArray(values) || values.length === 0) {
    throw new Error("at least one value is required");
  }
  const sorted = values.toSorted((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 1
    ? sorted[middle]
    : (sorted[middle - 1] + sorted[middle]) / 2;
}

export function assessStatisticalCredibility(goodput, p99) {
  const enoughRepetitions =
    goodput.repetitions >= MINIMUM_PAIRED_REPETITIONS &&
    p99.repetitions >= MINIMUM_PAIRED_REPETITIONS;
  const goodputInsideNoiseBudget = insideNoiseBudget(goodput);
  const p99InsideNoiseBudget = insideNoiseBudget(p99);
  return {
    minimum_paired_repetitions: MINIMUM_PAIRED_REPETITIONS,
    coefficient_of_variation_budget: COEFFICIENT_OF_VARIATION_BUDGET,
    enough_repetitions: enoughRepetitions,
    goodput_inside_noise_budget: goodputInsideNoiseBudget,
    p99_inside_noise_budget: p99InsideNoiseBudget,
    valid:
      enoughRepetitions &&
      goodputInsideNoiseBudget &&
      p99InsideNoiseBudget,
  };
}

export function assessFixedLoadCredibility(correctedP99, uncorrectedP99) {
  const enoughRepetitions =
    correctedP99.repetitions >= MINIMUM_PAIRED_REPETITIONS &&
    uncorrectedP99.repetitions >= MINIMUM_PAIRED_REPETITIONS;
  const correctedInsideNoiseBudget = insideNoiseBudget(correctedP99);
  const uncorrectedInsideNoiseBudget = insideNoiseBudget(uncorrectedP99);
  return {
    minimum_paired_repetitions: MINIMUM_PAIRED_REPETITIONS,
    coefficient_of_variation_budget: COEFFICIENT_OF_VARIATION_BUDGET,
    enough_repetitions: enoughRepetitions,
    corrected_p99_inside_noise_budget: correctedInsideNoiseBudget,
    uncorrected_p99_inside_noise_budget: uncorrectedInsideNoiseBudget,
    valid:
      enoughRepetitions &&
      correctedInsideNoiseBudget &&
      uncorrectedInsideNoiseBudget,
  };
}

export function assessResourceCredibility(cpu, peakMemory) {
  const enoughRepetitions =
    cpu.repetitions >= MINIMUM_PAIRED_REPETITIONS &&
    peakMemory.repetitions >= MINIMUM_PAIRED_REPETITIONS;
  const cpuInsideNoiseBudget = insideNoiseBudget(cpu);
  const peakMemoryInsideNoiseBudget = insideNoiseBudget(peakMemory);
  return {
    minimum_paired_repetitions: MINIMUM_PAIRED_REPETITIONS,
    coefficient_of_variation_budget: COEFFICIENT_OF_VARIATION_BUDGET,
    enough_repetitions: enoughRepetitions,
    cpu_inside_noise_budget: cpuInsideNoiseBudget,
    peak_memory_inside_noise_budget: peakMemoryInsideNoiseBudget,
    valid:
      enoughRepetitions &&
      cpuInsideNoiseBudget &&
      peakMemoryInsideNoiseBudget,
  };
}

function sampleStandardDeviation(values, mean) {
  const squaredDifference = values.reduce(
    (total, value) => total + (value - mean) ** 2,
    0,
  );
  return Math.sqrt(squaredDifference / (values.length - 1));
}

function insideNoiseBudget(summary) {
  return (
    summary.coefficient_of_variation !== null &&
    summary.coefficient_of_variation <= COEFFICIENT_OF_VARIATION_BUDGET
  );
}

function bootstrapGeometricMean(values, method) {
  const distribution = new Array(BOOTSTRAP_RESAMPLES);
  let state = BOOTSTRAP_SEED;
  for (let sample = 0; sample < BOOTSTRAP_RESAMPLES; sample += 1) {
    let logTotal = 0;
    for (let draw = 0; draw < values.length; draw += 1) {
      state = xorshift32(state);
      logTotal += Math.log(values[state % values.length]);
    }
    distribution[sample] = Math.exp(logTotal / values.length);
  }
  distribution.sort((left, right) => left - right);
  return {
    method,
    resamples: BOOTSTRAP_RESAMPLES,
    seed: BOOTSTRAP_SEED,
    lower: quantile(distribution, 0.025),
    upper: quantile(distribution, 0.975),
  };
}

function xorshift32(value) {
  let state = value >>> 0;
  state ^= state << 13;
  state ^= state >>> 17;
  state ^= state << 5;
  return state >>> 0;
}

function quantile(sorted, probability) {
  const index = Math.floor(probability * (sorted.length - 1));
  return sorted[index];
}
