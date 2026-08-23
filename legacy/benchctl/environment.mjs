// Stable source, toolchain, host, broker, and raw-librdkafka identity capture.

import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { arch, cpus, platform, release, totalmem } from "node:os";
import { resolve } from "node:path";

export function captureBenchmarkEnvironment(
  repository,
  { adapterVersion, brokerVersion, bootstrap },
) {
  const sibling = (name) => resolve(repository, "..", name);
  const clientRoot =
    process.env.KAFKA_BENCH_CLIENT_ROOT ??
    resolve(repository, "..", "kafkars");
  return {
    schema: "kafkars.benchmark-environment.v1",
    captured_at: new Date().toISOString(),
    source: {
      kafka_client: gitState(clientRoot),
      kafka_driver: gitState(sibling("kafka-driver")),
      kafka_protocol: gitState(sibling("kafka-protocol")),
      librdkafka: {
        version: adapterVersion,
        release: "v2.15.0",
        archive_sha256:
          "259015220cdca708afe838b5aa79ebf1a5fb710fb4179cf918d390aed85d5dbc",
        build_features: {
          ssl: false,
          gssapi: false,
          curl: false,
          external_zstd: false,
          external_lz4: false,
        },
      },
    },
    toolchain: {
      rustc: command("rustc", ["--version", "--verbose"]),
      cargo: command("cargo", ["--version"]),
      cc: command("cc", ["--version"]),
      node: process.version,
    },
    host: {
      platform: platform(),
      release: release(),
      architecture: arch(),
      cpu: cpus()[0]?.model ?? "unavailable",
      logical_cpus: cpus().length,
      memory_bytes: totalmem(),
      uname: command("uname", ["-a"]),
    },
    broker: {
      version: brokerVersion,
      bootstrap,
      lifecycle: "externally managed by the caller",
    },
  };
}

export function benchmarkEnvironmentIdentity(environment) {
  if (environment?.schema !== "kafkars.benchmark-environment.v1") {
    throw new Error("benchmark environment violates the sealed contract");
  }
  const identity = { ...environment };
  delete identity.captured_at;
  return createHash("sha256")
    .update(JSON.stringify(identity))
    .digest("hex");
}

function gitState(repository) {
  return {
    commit: command("git", ["rev-parse", "HEAD"], repository),
    dirty: command("git", ["status", "--porcelain"], repository) !== "",
  };
}

function command(program, args, cwd) {
  try {
    return execFileSync(program, args, {
      cwd,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    }).trim();
  } catch {
    return "unavailable";
  }
}
