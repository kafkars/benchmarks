# Changelog

All notable changes to kafka-benchmarks are documented here. The repository
publishes no packages, so entries describe the harness and the evidence
documents it produces rather than a released version cohort.

Evidence schemas are append-only. An entry that adds a field to an existing
schema id is a compatible change; an entry that changes what a field means is
not permitted and appears instead as a new schema id.

## [0.1.0] - Unreleased

### Added

- `kafkars.kafkars-native-metrics.v1`, a versioned v2 sidecar containing the
  public Kafkars producer snapshots immediately before and after measurement
  plus validated deltas for Produce requests, partition batches, records, and
  encoded record bytes. Request economics consumes only those exact counters;
  wire bytes, payload bytes, retries, and timeouts remain absent because the
  public surface does not report them;
- public sibling provenance for `kafkars/kafkars`,
  `kafkars/kafka-driver`, and `kafkars/kafka-wire`, including the Kafkars
  public-pair attestation and secret-free CI checkouts;

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
- `scripts/check` as the single gate, composing the workspace, guardrails,
  schema, control-plane, and dependency-provenance lanes;
- zrail 0.0.1 as the content-bound architecture authority for the harness
  workspace. `zrail.toml` and `zrail.lock` replace the custom
  `bench-guardrails` crate with package layers, locked dependency declarations,
  module contracts, declarative facades, sibling-test reachability, source
  hygiene, and tightening per-file size ratchets. A narrow companion check
  retains the two repository-specific policies outside zrail's declared-edge
  model: the transitive async-runtime ban over the root `Cargo.lock`, and source
  shape for the detached Kafkars adapter whose reviewed path dependencies live
  in sibling repositories. The adapter's own lock remains exempt because the
  client under test owns that graph;
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
- the `matched-execution-surface` gate in `kafkars.suite-summary.v1`. A ratio
  between two measurements is only a comparison when both sides measured the
  same thing, and `docs/EVIDENCE.md` has always said a reader "should require
  equal `payload_construction`" — this is that obligation as a gate rather than
  as advice. It fails when the two sides of a compared pair declare a different
  `payload_construction` or `serialization`, naming both values, and when a pair
  carries no declaration to check at all. Unequal `ownership` and
  `completion_mode` become named notes instead: those are real product-surface
  differences between the two clients' public APIs, and refusing to compare them
  would refuse to compare the clients as they exist. A failing gate also becomes
  a deterministic finding in the analysis packet, because it changes what every
  other finding there means;
- three attribution fields on every suite observation and median, appended to
  `kafkars.suite-summary.v1`: `declared` (what the adapter said was in the
  measured path), `p99_intended_to_call_start_ns` (scheduler lateness, absent
  rather than zero for a closed-loop run), and `p99_accepted_to_terminal_ns`
  (the client-internal portion). All three locate a difference and none may
  establish one, so they are paired, rendered, and citable from the packet, and
  no gate is written over any of them and none feeds the packet verdict — a
  client that refuses admission for longer looks better on the last of them by
  construction, which is the exact substitution v2 was minted to prevent;
- `cpu_core_seconds_per_million_acknowledged` beside `cpu_core_seconds`, in the
  same schema and in the scorecard. Total CPU is not comparable between subjects
  that moved different amounts of traffic; absent, never zero, when the platform
  reported no resources or nothing was acknowledged to divide by;
- `queue.max_outstanding_bytes_observed` in `kafkars.producer-benchmark.v2`,
  stamped by both adapters at the same admit and terminal sites the record
  high-water is updated at. Accumulated rather than multiplied out of the record
  count afterwards: today's fixed payload size makes the two agree, and a
  variable-size payload profile would silently make the multiplication wrong;
- an optional `provenance` block in `kafkars.llm-summary.v1` — model, prompt
  version, reasoning effort, response id, the digests of the packet in and the
  summary out, and when it was written.
  `scripts/benchmark-openai-summary` now embeds it *and* keeps writing the
  `llm-provenance.json` sidecar: the sidecar because the surrounding tooling
  reads it and a rejected summary still leaves one, the embed because a summary
  that has been moved arrives without its sidecar. `output_sha256` covers the
  model's own bytes with `provenance` removed, since a digest cannot cover the
  field carrying it, and the strict output schema does not offer the model a
  `provenance` field;
- `--offline-reply` on `scripts/benchmark-openai-summary`, which replays a
  captured provider response instead of calling the API. The guardrail still
  runs, so `scripts/benchmark-openai-summary-test` now exercises the whole reply
  path — the embed, the sidecar, and the invariant that no markdown is rendered
  for a summary nothing accepted — with no network, no key, and no toolchain;
- build identity in `kafkars.benchmark-environment.v2`'s toolchain map:
  `rustflags`, `build_profile` read off the running `benchctl`, and a documented
  constant `cargo_locked`. A compiler version alone does not identify a binary;
- `slo-drain-tail` and `slo-queue-growth-slope` in every classification's
  `deferred_checks`. `SloSpec` declares them and nothing evaluates them, and
  until now that was written down only in a module contract a bundle reader
  never opens;
- a "First run, no cluster" section at the top of the README. `fake-adapter`
  speaks the whole adapter protocol and plays all three cluster tools, so a
  complete sealed bundle takes under a second against no broker — and until now
  the word "fake" appeared in no markdown file in the repository;
- `benchctl run` prints the bundle it sealed as its last line of stdout, on
  every exit code, and `benchctl resolve` prints the `experiment_id` on stderr
  so stdout stays exactly one JSON document. The pack table gained an evidence
  column naming each single-attempt entry's bundle;
- `--bootstrap` endpoints are checked as `host:port` at parse time. A typo used
  to seal silently into the runtime binding and the environment document of an
  immutable bundle;
- the deferred subject-revision model, the two unwritten cadences, the parked
  cross-repository trigger, and the three unbuilt LLM summary flavors are now
  named in `docs/ROADMAP.md`. The first states plainly that `base` and `head`
  mean two externally built binaries against one shared sibling pin today, so
  two client *revisions* cannot be compared in one attempt;

### Changed

- `zcheck.toml` is now the canonical local and CI qualification graph. zcheck
  0.0.2 preserves the seven existing bare-clone tasks, records receipts and
  complete logs, retains CI qualification evidence for 30 days, and replaces
  only the deleted `scripts/check` dispatcher;
  adapter conformance remains a separate task requiring reviewed sibling
  checkouts. The zrail 0.0.2 epoch migration is deliberately deferred because
  that release reports 62 unresolved ordinary Rust bindings under the current
  strict policy, so zrail remains pinned to 0.0.1 without weakening authority;
- the README now uses the Kafkars organization benchmarks mark and keeps the
  front door to purpose, verification, one real run, result reading, and links
  to the detailed contracts;
- suite reports now lead with the comparison result, use plain-language
  checks and run-stability labels, show the practical threshold as a
  percentage, and replace raw metric field names with reader-facing names. The
  README keeps the same quickstarts and boundaries in a shorter front door;
- **the `bench-report` render goldens were regenerated.** `crates/bench-report/golden/suite.md`
  and `suite.html` gained three scorecard columns (lateness p99,
  accepted-to-terminal p99, CPU per million acknowledged), one gate row
  (`matched-execution-surface`), one paired-comparison row, and three dispersion
  rows. This is a deliberate regeneration under the carve-out in `AGENTS.md`:
  the goldens pin what a reader is shown, the change adds columns and rows
  without altering any previously reported number, and no schema id moved. Every
  other golden and every conformance vector is byte-identical;
- the attempt-level `comparison.json` divides by the subject the experiment
  labelled `base`, falling back to execution order only when no role is
  declared. `benchctl suite` alternates which subject runs first, so under the
  old rule one suite sealed `head/base` in one repetition and `base/head` in the
  next — reproduced live before this changed — and a reader comparing two
  comparison documents from one suite saw the ratio invert for no reason the
  documents explained. The anchor can no longer become a denominator when a base
  exists;
- the packet verdict is `inconclusive` when fewer than the five paired
  repetitions a comparison needs were valid, however far past the threshold the
  intervals sit. A bootstrap over two paired blocks is an interval over two
  numbers. The directional read is not discarded: it moves into a deterministic
  finding that names the direction and the repetition count in one sentence;
- a finding in `kafkars.llm-summary.v1` must cite at least one metric.
  `findings` entries are asserted as fact and an uncited one reads exactly like
  a cited one while resting on nothing; a statement the model cannot attach to a
  number is a hypothesis, and `hypotheses` is deliberately not required to cite.
  Stating no findings at all is still fine;
- `ci.yml`'s `diagnostic` job runs on pull requests as well as
  `workflow_dispatch`, under `continue-on-error: true` and still absent from
  `quality-gate.needs`, and it now runs `benchctl pack --manifest
  scenarios/packs/pr.toml` through `scripts/generate-subject-config` rather than
  the legacy `scripts/bench-producer-compare`. Its numbers are shared-runner
  numbers and it must be able to fail without blocking a merge; a fork has no
  sibling-checkout token, so the job fails there harmlessly by design;
- an unknown option is reported as one before anything looks for its value.
  `--turbo` with nothing after it used to say it needed a value, which tells a
  reader the option exists and they got the syntax wrong;
- a missing `--llm-summary` file, a `--bundle` that is not a sealed bundle, and
  a rejected summary are reported as invalid *input* naming the flag, rather
  than as an invalid experiment — the one input that was fine. Exit code 65 is
  unchanged;
- `scripts/check-dependency-provenance` reports a cross-check whose input could
  not be read as uncheckable rather than as a mismatch. On a bare clone the two
  derived checks have nothing to compare, and calling that a mismatch told a
  reader two revisions disagreed when nothing had been compared;
- `results/pending/` is removed when the last workspace has moved out of it.
  An empty directory left in the evidence tree reads as an attempt somebody
  lost; the removal is best-effort and never fails a run whose bundle is already
  sealed;
- `scenarios/producer/producer-baseline.toml` opens with a header saying it is
  the design document's non-executable reference matrix and that the resolver
  refuses it on purpose. The explanation existed only in a Rust module contract;
- `scenarios/DEFERRED.md` points at the decomposed
  `adapters/kafkars/src/protocol/{verdict,describe,settings}.rs` rather than the
  `protocol.rs` that no longer holds those decisions, and says plainly that the
  headline set is 6 scenarios against the design's 12 to 18, with every missing
  row's axis listed;

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
