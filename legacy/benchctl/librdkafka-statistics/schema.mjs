// Envelope, cumulative-counter, broker, and partition schema validation.

import { readFileSync } from "node:fs";

export function parseSnapshots(path) {
  const lines = readFileSync(path, "utf8").trimEnd().split("\n");
  if (lines.length < 2 || lines.some((line) => line.length === 0)) {
    throw new Error("librdkafka statistics require multiple complete snapshots");
  }
  return lines.map((line, index) => parseSnapshot(line, index));
}

export function uniquePhaseIndex(snapshots, phase) {
  const indices = snapshots.flatMap((snapshot, index) =>
    snapshot.phase === phase ? [index] : [],
  );
  if (indices.length !== 1) {
    throw new Error(`librdkafka statistics require exactly one ${phase} snapshot`);
  }
  return indices[0];
}

export function counterDelta(before, after, key, name) {
  const start = counter(before, key);
  const finish = counter(after, key);
  if (finish < start) {
    throw new Error(`librdkafka ${name} counter moved backwards`);
  }
  return finish - start;
}

export function counter(value, key) {
  const found = value?.[key];
  if (!nonnegativeSafeInteger(found)) {
    throw new Error(`librdkafka counter ${key} is invalid`);
  }
  return found;
}

export function gauge(value, key) {
  return counter(value, key);
}

export function brokerObjects(snapshot) {
  if (!plainObject(snapshot.brokers)) {
    throw new Error("librdkafka broker statistics are missing");
  }
  return Object.values(snapshot.brokers).filter(
    (broker) =>
      plainObject(broker) &&
      Number.isSafeInteger(broker.nodeid) &&
      broker.nodeid >= 0,
  );
}

export function requestTypeDeltas(before, after) {
  const starts = brokerRequestTypes(before);
  const finishes = brokerRequestTypes(after);
  const result = {};
  for (const key of new Set([...Object.keys(starts), ...Object.keys(finishes)])) {
    const start = starts[key] ?? 0;
    const finish = finishes[key] ?? 0;
    if (finish < start) {
      throw new Error(`librdkafka request counter ${key} moved backwards`);
    }
    const difference = finish - start;
    if (difference > 0) {
      result[key] = difference;
    }
  }
  return Object.fromEntries(
    Object.entries(result).toSorted(([left], [right]) =>
      left.localeCompare(right),
    ),
  );
}

export function brokerCounterDelta(before, after, key) {
  const start = brokerObjects(before).reduce(
    (total, broker) => total + counter(broker, key),
    0,
  );
  const finish = brokerObjects(after).reduce(
    (total, broker) => total + counter(broker, key),
    0,
  );
  if (finish < start) {
    throw new Error(`librdkafka broker counter ${key} moved backwards`);
  }
  return finish - start;
}

export function partitionDeltas(before, after, topic, partitions) {
  const beforePartitions = before.topics?.[topic]?.partitions ?? {};
  const afterPartitions = after.topics?.[topic]?.partitions;
  if (!plainObject(afterPartitions)) {
    throw new Error(`librdkafka topic statistics are missing for ${topic}`);
  }
  return Array.from({ length: partitions }, (_, partition) => {
    const key = String(partition);
    const start = beforePartitions[key]?.txmsgs ?? 0;
    const finish = afterPartitions[key]?.txmsgs;
    if (
      !nonnegativeSafeInteger(start) ||
      !nonnegativeSafeInteger(finish) ||
      finish < start
    ) {
      throw new Error(`librdkafka partition ${partition} counter is invalid`);
    }
    return { partition, records: finish - start };
  });
}

export function plainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

export function positiveSafeInteger(value) {
  return Number.isSafeInteger(value) && value > 0;
}

export function nonnegativeSafeInteger(value) {
  return Number.isSafeInteger(value) && value >= 0;
}

function parseSnapshot(line, index) {
  let snapshot;
  try {
    snapshot = JSON.parse(line);
  } catch (error) {
    throw new Error(`invalid librdkafka statistics JSON at line ${index + 1}`, {
      cause: error,
    });
  }
  if (
    snapshot.schema !== "kafkars.librdkafka-statistics.v1" ||
    !["setup", "warmup", "baseline", "measured", "final"].includes(
      snapshot.phase,
    ) ||
    !positiveSafeInteger(snapshot.captured_ns) ||
    !plainObject(snapshot.statistics)
  ) {
    throw new Error(`invalid librdkafka statistics envelope at line ${index + 1}`);
  }
  return snapshot;
}

function brokerRequestTypes(snapshot) {
  const totals = {};
  for (const broker of brokerObjects(snapshot)) {
    if (!plainObject(broker.req)) {
      throw new Error("librdkafka broker request counters are missing");
    }
    for (const [key, value] of Object.entries(broker.req)) {
      if (!nonnegativeSafeInteger(value)) {
        throw new Error(`librdkafka request counter ${key} is invalid`);
      }
      totals[key] = (totals[key] ?? 0) + value;
    }
  }
  return totals;
}
