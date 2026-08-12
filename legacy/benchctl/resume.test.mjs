// Contract tests for lossless interrupted-attempt archival.

import assert from "node:assert/strict";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { archiveIncompleteDirectory } from "./resume.mjs";

test("resume archives every interrupted attempt without overwriting", () => {
  const root = mkdtempSync(join(tmpdir(), "kafkars-resume-test-"));
  const directory = join(root, "rep-03");
  try {
    writeFileSync(directory, "first");
    const first = archiveIncompleteDirectory(directory, "probe");
    assert.equal(first.endsWith("rep-03-aborted-01"), true);
    writeFileSync(directory, "second");
    const second = archiveIncompleteDirectory(directory, "probe");
    assert.equal(second.endsWith("rep-03-aborted-02"), true);
    assert.equal(existsSync(first), true);
    assert.equal(existsSync(second), true);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
