// Deterministic bracket expansion, refinement, and fixed-rate derivation.

export const FIXED_LOAD_PERCENTAGES = Object.freeze([25, 50, 75, 90]);

export function searchSustainableCapacity(configuration, evaluate) {
  validateConfiguration(configuration);
  if (typeof evaluate !== "function") {
    throw new Error("capacity search requires an evaluator");
  }
  const evaluated = new Map();
  const assess = (rate) => {
    if (!evaluated.has(rate)) {
      evaluated.set(rate, Boolean(evaluate(rate)));
    }
    return evaluated.get(rate);
  };

  let lower;
  let upper;
  if (assess(configuration.initialRate)) {
    lower = configuration.initialRate;
    for (;;) {
      const next = Math.min(
        configuration.maxRate,
        lower * configuration.growthFactor,
      );
      if (!Number.isSafeInteger(next) || next <= lower) {
        throw new Error("capacity expansion cannot make progress");
      }
      if (!assess(next)) {
        upper = next;
        break;
      }
      if (next === configuration.maxRate) {
        throw new Error("capacity exceeds the configured search ceiling");
      }
      lower = next;
    }
  } else {
    upper = configuration.initialRate;
    for (;;) {
      const next = Math.max(
        configuration.minRate,
        Math.floor(upper / configuration.growthFactor),
      );
      if (next >= upper) {
        throw new Error("capacity contraction cannot make progress");
      }
      if (assess(next)) {
        lower = next;
        break;
      }
      if (next === configuration.minRate) {
        throw new Error("no sustainable rate found above the search floor");
      }
      upper = next;
    }
  }

  while ((upper - lower) / lower > configuration.resolutionFraction) {
    const midpoint = Math.floor((lower + upper) / 2);
    if (midpoint <= lower || midpoint >= upper) {
      break;
    }
    if (assess(midpoint)) {
      lower = midpoint;
    } else {
      upper = midpoint;
    }
  }
  return {
    sustainable_records_per_second: lower,
    first_failing_records_per_second: upper,
    bracket_width_fraction: (upper - lower) / lower,
    evaluations: [...evaluated.entries()].map(([rate, sustainable]) => ({
      rate,
      sustainable,
    })),
    derived_fixed_rates: deriveFixedRates(lower),
  };
}

export function deriveFixedRates(capacity) {
  if (!Number.isSafeInteger(capacity) || capacity <= 0) {
    throw new Error("reference capacity must be a positive integer");
  }
  return Object.fromEntries(
    FIXED_LOAD_PERCENTAGES.map((percentage) => [
      String(percentage),
      Math.floor((capacity * percentage) / 100),
    ]),
  );
}

function validateConfiguration(configuration) {
  const integerKeys = ["initialRate", "minRate", "maxRate", "growthFactor"];
  if (
    !integerKeys.every(
      (key) => Number.isSafeInteger(configuration?.[key]) && configuration[key] > 0,
    ) ||
    configuration.growthFactor < 2 ||
    configuration.minRate >= configuration.initialRate ||
    configuration.initialRate >= configuration.maxRate ||
    typeof configuration.resolutionFraction !== "number" ||
    !Number.isFinite(configuration.resolutionFraction) ||
    configuration.resolutionFraction <= 0 ||
    configuration.resolutionFraction >= 1
  ) {
    throw new Error("capacity search configuration is invalid");
  }
}
