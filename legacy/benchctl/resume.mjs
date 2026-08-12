// Lossless archival of interrupted benchmark attempts before deterministic retry.

import { existsSync, renameSync } from "node:fs";

export function archiveIncompleteDirectory(directory, label) {
  if (!existsSync(directory)) {
    throw new Error(`missing incomplete ${label} directory: ${directory}`);
  }
  for (let attempt = 1; attempt <= 99; attempt += 1) {
    const archived = `${directory}-aborted-${String(attempt).padStart(2, "0")}`;
    if (!existsSync(archived)) {
      renameSync(directory, archived);
      return archived;
    }
  }
  throw new Error(`too many aborted ${label} attempts for ${directory}`);
}
