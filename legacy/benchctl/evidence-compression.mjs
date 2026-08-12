// Post-validation streaming compression of immutable raw benchmark evidence.

import { createReadStream, createWriteStream, existsSync, unlinkSync } from "node:fs";
import { pipeline } from "node:stream/promises";
import { createGzip } from "node:zlib";

export async function compressEvidenceFile(path) {
  const compressed = `${path}.gz`;
  if (!existsSync(path) || existsSync(compressed)) {
    throw new Error(`evidence compression target is invalid: ${path}`);
  }
  try {
    await pipeline(
      createReadStream(path),
      createGzip({ level: 9 }),
      createWriteStream(compressed, { flags: "wx" }),
    );
  } catch (error) {
    if (existsSync(compressed)) {
      unlinkSync(compressed);
    }
    throw error;
  }
  unlinkSync(path);
  return compressed;
}
