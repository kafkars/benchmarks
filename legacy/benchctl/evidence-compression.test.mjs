// Contract tests for post-validation raw-evidence compression.

import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { gunzipSync } from "node:zlib";

import { compressEvidenceFile } from "./evidence-compression.mjs";

test("compression preserves bytes and removes only the validated source", async () => {
  const directory = mkdtempSync(join(tmpdir(), "kafkars-evidence-test-"));
  const path = join(directory, "latency.csv");
  const bytes = Buffer.from("header\n1,2,3\n".repeat(100));
  try {
    writeFileSync(path, bytes);
    const compressed = await compressEvidenceFile(path);
    assert.deepEqual(gunzipSync(readFileSync(compressed)), bytes);
    assert.throws(() => readFileSync(path));
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
