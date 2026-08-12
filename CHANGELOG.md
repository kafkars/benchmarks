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
  control-plane, and dependency-provenance lanes;
- `kafkars.producer-benchmark.v2`, the measurement document of the
  four-timestamp offer model: every offer carries one immutable identity and its
  intended, call-start, accepted, and terminal instants, none of which is reset
  when a queue is full, so admission backpressure is inside every reported
  latency and an offer that never crossed the client API is counted as
  offered-but-not-accepted rather than dropped. A new schema id rather than a
  change to v1: the legacy stdout verbs keep emitting
  `kafkars.producer-benchmark.v1` unchanged;
- the `kafkars.log-linear.v1` histogram, a bounded log-linear encoding with 128
  linear sub-buckets per power of two, carried as a sparse ascending
  index/count list. Evidence memory stops scaling with run length, percentiles
  are derived by the reader from a bucket's inclusive upper bound rather than
  chosen by the writer, and the encoding is specified byte-for-byte so the Rust
  and C adapters can be asserted equal instead of close;
- `benchctl suite`, which runs paired repetitions of one scenario with
  alternating subject order and seals each attempt independently, and
  `benchctl capacity`, which searches for the highest offered rate that still
  meets every declared objective and reports an unbracketed search as
  inconclusive rather than naming an unobserved capacity;
- `benchctl report` over a sealed bundle and `benchctl packet` over a suite
  summary, the latter emitting `kafkars.analysis-packet.v1` — the
  numbered-metric, referenced-finding boundary between measurement and prose,
  whose verdict downstream summaries may narrow but never contradict;
- the headline producer set under `scenarios/producer/headline/`: a 128-byte
  latency floor, the balanced 1 KiB fixed-rate default, a 96-partition fanout
  point, a 16 KiB payload point, a deliberate overload with a declared SLO, and
  a balanced capacity search — each stating the design question it answers, and
  each sized to complete on a developer host;
- the v2 measured path in both adapters, which is what makes
  `kafkars.producer-benchmark.v2` an observation rather than a schema. The
  librdkafka adapter grew a byte-exact C implementation of the log-linear
  histogram; the kafkars adapter grew pooled payloads and bounded slabs so that
  the offer path does not allocate per record after warmup and the harness
  measures the client rather than its own allocator;
- the histogram conformance vector under `conformance/histogram/`, the third
  cross-adapter byte contract alongside the payload and the schedule. Its input
  is a table of edge cases rather than a sample — the exact-bucket region, the
  first scale change, `2^40` and `2^63`, and `u64::MAX` twice so that `sum` must
  saturate rather than wrap, which is the case a straight `+=` in C gets
  silently wrong;
- paired bootstrap confidence intervals over repeated blocks, computed from a
  hand-rolled seeded `xoshiro256**` generator so that a resampling result is
  reconstructible from a seed in the sealed output rather than dependent on a
  dependency version;
- request economics: produce requests, wire bytes, batches, retries, and
  timeouts spent per subject, read from the librdkafka statistics stream. Two
  clients can post the same goodput and the same p99 while spending very
  different amounts of broker traffic to do it, which a latency histogram does
  not show;
- markdown and HTML renderers over a suite summary, with committed goldens;
- `scenarios/packs/pr.toml` and `scenarios/packs/nightly.toml`, declaring which
  scenarios belong to which cadence and at how many repetitions, plus
  `scenarios/DEFERRED.md` naming every matrix row that cannot run yet and what
  refuses it;
- `benchctl pack`, which runs every entry of one reviewed manifest in the order
  it states, dispatching each to the verb its repetition count implies — two or
  more is a suite, one is a run, and one over a scenario carrying a `[search]`
  section is a capacity ladder. It contributes no measurement and no statistic
  of its own, so a pack leaves exactly the evidence those verbs would have left
  if the commands had been typed one at a time; it exits 0 only when every
  entry did, and 20 otherwise;
- `scripts/benchmark-openai-summary`, the narration step: it reads one analysis
  packet — never a bundle, never a result document — sends it under the
  versioned `analysis/prompts/producer-comparison.v1.md` prompt with a strict
  structured-output schema mirroring `kafkars.llm-summary.v1`, records the exact
  request before the call, and then runs `benchctl packet` over the reply. The
  markdown is rendered only for a summary the guardrail accepted, and
  `llm-provenance.json` carries the sha256 of both the packet in and the summary
  out. `scripts/benchmark-openai-summary-test` proves that request contract
  offline against a committed fixture, with no network, no API key, and no
  build;
- `.github/workflows/nightly.yml`, which runs the nightly pack against the dev
  compose cluster on a schedule, uploads the sealed evidence and reports for 30
  days, and narrates each suite's packet in the run summary. Its numbers are
  shared-runner diagnostics and never a comparison between clients. A rejected
  model summary is reported and does not fail the workflow: narration is
  commentary on evidence and may never be the reason evidence is discarded;
- `scripts/generate-subject-config`, the one place the per-machine subject list
  and cluster profile are written, shared by `scripts/bench-suite-acceptance`
  and the nightly workflow so the two cannot drift into measuring different
  subjects;
- `scripts/check-librdkafka-pin`, a gate lane asserting that the reviewed
  librdkafka version and archive checksum read the same in all seven places
  they are written down — the bootstrap script, the adapter's describe
  constant, both workflow cache keys, the environment schema's consts, and the
  two legacy capture sites. A bump that missed one produced a bundle naming one
  version while another ran, which is evidence that is internally consistent and
  wrong;
- a tripwire in `scripts/check-control-plane` over the one deliberate deviation
  in `legacy/`: both environment capture sites must still resolve the client
  through `KAFKA_BENCH_CLIENT_ROOT`. Restoring the upstream shape in the name of
  fidelity would break no test and would attribute this repository's git state
  to the client under measurement in every bundle sealed afterwards.

### Changed

- `analysis/prompts/producer-comparison.v1.md` no longer says that nothing in
  this repository invokes a language model. Nothing on a *measurement's path*
  does, which is the claim that mattered and is still true; the prompt now names
  the script that calls a model after sealing, and the guardrail that decides
  whether the reply is evidence. The prompt's own rules are unchanged, so the
  version is unchanged;
- the declared payload-construction vocabulary is unified across the scenario
  TOMLs, the resolved experiment, and both adapters, so that one workload shape
  has one name everywhere it is written down;
- `scripts/check-dependency-provenance` treats an **absent** sibling checkout as
  an advisory rather than a hard failure when it is not in strict mode. No crate
  in this workspace depends on the siblings — only the out-of-workspace adapter
  does — so `scripts/check` now runs green on a clean clone with nothing beside
  it, which is what the quickstart claims. Strict mode (`CI=true` or
  `KAFKA_BENCH_PROVENANCE=strict`) still refuses, and `RELEASING.md` now names
  the strict invocation explicitly because the plain gate does not perform it;
- the two large-record headline scenarios moved to
  `scenarios/producer/deferred/`. They are authored and validate cleanly, and
  they cannot yet produce a valid kafkars measurement; `scenarios/DEFERRED.md`
  records both sealed findings.

### Fixed

- the kafkars adapter reported a failed measurement phase as the session-close
  error it caused, so the diagnosis a reader saw first was a symptom. The
  measurement's own failure now leads, and producer-admission fencing after a
  failure terminal is reported accurately instead of being masked;
- the documentation mirrors under `schemas/` for `run-status.v1`,
  `execution-order.v1`, `comparison.v1`, `classification.v1`, `experiment.v1`,
  and `subjects-lock.v1` described fields the serde types do not have and
  required fields the types omit — three of them rejected documents the harness
  really writes. The wire format did not move; only the description of it was
  wrong. Corrected against real sealed bundles, and `AGENTS.md` now records that
  fixing a mirror of an unchanged format is a documentation change rather than a
  schema-affecting one.
