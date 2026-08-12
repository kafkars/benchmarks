// Contract tests for cross-platform process resource normalization.

import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import test from "node:test";

import { readProcessResources } from "./process-resources.mjs";

test("normalizes Darwin physical footprint separately from byte RSS", () => {
  const result = parse(`real 12.50
user 3.25
sys 1.75
             33554432  maximum resident set size
                 120  page reclaims
                   2  page faults
                  40  voluntary context switches
                   8  involuntary context switches
          1250000000  instructions retired
           750000000  cycles elapsed
            25165824  peak memory footprint
`);
  assert.equal(result.method, "darwin-bsd-time-lp");
  assert.equal(result.schema, "kafkars.process-resources.v2");
  assert.equal(result.cpu_core_seconds, 5);
  assert.equal(result.max_rss_bytes, 33_554_432);
  assert.equal(result.peak_memory_bytes, 25_165_824);
  assert.equal(result.peak_memory_method, "darwin-peak-physical-footprint");
  assert.equal(result.cpu_core_seconds_per_million_acknowledged_records, 20);
  assert.equal(result.instructions_retired_per_million_acknowledged_records, 5_000_000_000);
  assert.equal(result.cycles_elapsed_per_million_acknowledged_records, 3_000_000_000);
  assert.equal(result.involuntary_context_switches, 8);
});

test("normalizes GNU time output with KiB RSS", () => {
  const result = parse(`Command being timed: "adapter"
User time (seconds): 4.50
System time (seconds): 0.50
Elapsed (wall clock) time (h:mm:ss or m:ss): 0:12.25
Maximum resident set size (kbytes): 32768
Minor (reclaiming a frame) page faults: 200
Major (requiring I/O) page faults: 1
Voluntary context switches: 60
Involuntary context switches: 9
`);
  assert.equal(result.method, "linux-gnu-time-v");
  assert.equal(result.elapsed_wall_seconds, 12.25);
  assert.equal(result.max_rss_bytes, 33_554_432);
  assert.equal(result.peak_memory_bytes, 33_554_432);
  assert.equal(result.peak_memory_method, "linux-maximum-resident-set-size");
  assert.equal(result.instructions_retired, null);
  assert.equal(result.cycles_elapsed, null);
  assert.equal(result.cpu_core_seconds, 5);
});

test("fails closed on missing or zero resource evidence", () => {
  assert.throws(() => parse("real 1\nuser 0\nsys 0\n"));
  assert.throws(() =>
    parse(`real 1
user 1
sys 0
  1024  maximum resident set size
  1  page reclaims
  0  page faults
  1  voluntary context switches
  1  involuntary context switches
`),
  );
  assert.throws(() => parse("", 0));
});

function parse(raw, records = 250_000) {
  const directory = mkdtempSync(resolve(tmpdir(), "kafkars-resources-"));
  const path = resolve(directory, "resources.txt");
  writeFileSync(path, raw);
  return readProcessResources(path, records);
}
