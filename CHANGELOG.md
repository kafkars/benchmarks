# Changelog

All notable changes to kafka-benchmarks are documented here. The repository
publishes no packages, so entries describe the harness and the evidence
documents it produces rather than a released version cohort.

Evidence schemas are append-only. An entry that adds a field to an existing
schema id is a compatible change; an entry that changes what a field means is
not permitted and appears instead as a new schema id.

## [0.1.0] - Unreleased

### Added

- extracted the benchmark harness out of the private `zsumz/kafka-client-private`
  repository into a standalone lab, so that measuring a client is no longer a
  subdirectory of the client being measured;
- the Rust workspace: `bench-schema`, `benchctl`, `bench-adapter-librdkafka`,
  `bench-verifier`, and `bench-report`, on Rust 1.88 with warnings denied in
  clippy and rustdoc and a committed lockfile;
- the repository contract in `AGENTS.md`: no async runtime, no benchmarking
  framework, subjects as processes, public client surfaces only, append-only
  evidence schemas, and immutable sealed bundles;
- typed views over `kafkars.producer-verification.v1`, mirroring the document
  the C verifier prints;
- descriptive statistics over repeated positive measurements, carrying the
  repetition count and coefficient of variation alongside the mean, plus the
  minimum-repetition and noise-budget thresholds inherited from the legacy
  control plane;
- `scripts/check` as the single gate, composing the workspace, schema,
  control-plane, and dependency-provenance lanes.
