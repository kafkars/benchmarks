<p align="center">
  <img src="./kafka-benchmarks-logo.svg" alt="kafka-benchmarks" width="720">
</p>

<p align="center">
  <strong>Reproducible performance evidence for kafkars and other Apache Kafka clients.</strong>
</p>

<p align="center">
  A benchmark run here is not a number. It is a sealed, checksummed bundle that
  records the exact experiment, the exact environment, what every client
  actually did, and whether an independent verifier agrees the data survived.
</p>

<p align="center">
  <a href="https://github.com/zsumz/kafka-benchmarks/actions/workflows/ci.yml"><img src="https://github.com/zsumz/kafka-benchmarks/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
</p>

<p align="center">
  <a href="#what-it-is">What it is</a>
  <span> · </span>
  <a href="#layout">Layout</a>
  <span> · </span>
  <a href="#first-run-no-cluster">First run</a>
  <span> · </span>
  <a href="#the-engine">Engine</a>
  <span> · </span>
  <a href="#ci-and-the-nightly">CI</a>
  <span> · </span>
  <a href="#evidence">Evidence</a>
  <span> · </span>
  <a href="#scenarios-and-packs">Scenarios</a>
  <span> · </span>
  <a href="#quickstart">Quickstart</a>
  <span> · </span>
  <a href="#boundaries">Boundaries</a>
  <span> · </span>
  <a href="#status">Status</a>
</p>

<br />

## What it is

kafka-benchmarks answers one question for every change worth arguing about: did
it improve the client, against which baseline, and how do we know?

The rules are simple:

- **Subjects are processes.** The control plane never links a Kafka client.
- **Verification is independent.** An adapter cannot decide its own validity.
- **Every attempt seals.** Crashes, timeouts, and interrupts remain evidence.
- **Evidence is immutable.** Schemas are versioned and sealed bundles are never rewritten.
- **Success is not validity.** A clean run may still be too noisy or incomplete to support a claim.

## Layout

```txt
crates/bench-schema      evidence documents, canonical JSON, identities
crates/benchctl          control plane: resolve/run/suite/capacity/report/packet
crates/bench-adapter-librdkafka   adapter-protocol shim for the C reference
crates/bench-verifier    typed views over independent read-back evidence
crates/bench-report      descriptive statistics over sealed bundles

adapters/                subject adapters; standalone workspaces, not members
legacy/benchctl/         the Node harness this repository was extracted from
scenarios/producer/headline/   the headline producer set (TOML)
scenarios/packs/         which scenarios belong to which cadence
schemas/                 one JSON Schema document per schema id
conformance/             committed payload, schedule, and histogram vectors
clusters/                local broker topologies for development runs
scripts/                 the gate and the harness entry points
docs/                    the performance contract, harness design, and roadmap
```

Adapters are separate workspaces, so each subject keeps its pinned dependency graph.

## First run, no cluster

The full pipeline can run without Kafka. `fake-adapter` invents measurements so
you can inspect a real sealed bundle in under a second. Its numbers are not
Kafka performance evidence.

```sh
git clone https://github.com/zsumz/kafka-benchmarks && cd kafka-benchmarks
cargo build --release --locked -p benchctl   # builds fake-adapter too
```

Create two local input files:

```toml
# subjects.toml — both subjects are the fake adapter.
# The roles say which side of the comparison each subject is; they participate
# in the experiment id, so swapping them is a different experiment.
[[subjects]]
name = "base-subject"
role = "base"
command = ["target/release/fake-adapter"]

[[subjects]]
name = "head-subject"
role = "head"
command = ["target/release/fake-adapter"]
```

```toml
# cluster.toml — no broker is contacted.
# Topic management and read-back verification are configured tools rather than
# adapter verbs, so they are named here; offline they are the same binary.
name = "offline"
bootstrap = "127.0.0.1:39092,127.0.0.1:39093,127.0.0.1:39094"
broker_version = "none — nothing is contacted"
security = "plaintext"
brokers = 3
lifecycle = "externally managed by the caller"

[tools]
topic_create = ["target/release/fake-adapter", "topics-create"]
topic_delete = ["target/release/fake-adapter", "topics-delete"]
verify = ["target/release/fake-adapter", "verify"]
```

Then one attempt. `run` prints the bundle it sealed as its last line:

```sh
bundle=$(target/release/benchctl run \
  --experiment scenarios/producer/legacy-balanced-1k.toml \
  --subjects subjects.toml \
  --cluster cluster.toml \
  --bootstrap 127.0.0.1:39092,127.0.0.1:39093,127.0.0.1:39094 \
  --results results | tail -n 1)

target/release/benchctl report --bundle "$bundle" --out reports/attempt.md
```

Start with the bundle:

- `status.json`: how far the attempt got.
- `classification.json`: whether it is valid and which checks were deferred.
- `comparison.json`: ratios against the declared base subject.
- `adapters/<subject>/result.json`: each subject's measurement.
- `bundle.json` and `checksums.txt`: the sealed identity and byte checks.

[`docs/EVIDENCE.md`](./docs/EVIDENCE.md) defines every file and its owner.

## The engine

`benchctl` resolves experiments, runs subjects, invokes verification, and seals
evidence. It uses three inputs:

| Input | Purpose |
| --- | --- |
| scenario | reviewed workload and validity rules |
| subject list | binaries available on this machine |
| cluster profile | brokers and external tools |

Generate real-client inputs after checking out the three sibling repositories:

```sh
cargo build --release --locked -p benchctl
export PATH="$PWD/target/release:$PATH"

bootstrap=127.0.0.1:39092,127.0.0.1:39093,127.0.0.1:39094
inputs=target/quickstart
scripts/generate-subject-config "$inputs" "$bootstrap"
```

| Command | Purpose |
| --- | --- |
| `resolve` | show the exact experiment without touching Kafka |
| `run` | execute one attempt and print its sealed bundle path |
| `suite` | run paired, alternating repetitions and write a comparison report |
| `capacity` | search an open-loop rate against declared objectives |
| `pack` | run a reviewed scenario manifest |
| `report` | render one sealed attempt without rewriting it |
| `packet` | verify that generated prose matches a suite's analysis packet |

Run a paired suite:

```sh
benchctl suite \
  --experiment scenarios/producer/headline/balanced-1k-12p.toml \
  --subjects "$inputs/subjects.toml" \
  --cluster "$inputs/cluster.toml" \
  --bootstrap "$bootstrap" \
  --repetitions 5
```

Render a single attempt:

```sh
benchctl report --bundle "$bundle" --out reports/attempt.md
```

Use `benchctl <command> --help` for the full option list. The offline first run
above works on a bare clone; real subjects need the sibling checkouts.

## CI and the nightly

- **CI** runs the quality gate. It tests the harness; it does not measure client performance.
- **Nightly** runs the full pack on a shared runner and uploads evidence for 30 days.
- **Nightly numbers are diagnostic only.** Shared-runner output cannot support a client comparison.
- **Generated prose is checked.** `benchctl packet` rejects summaries that do not match the analysis packet.

## Evidence

The main measurement document is `kafkars.producer-benchmark.v2`.

- Each offer keeps one identity and four timestamps: intended, call start, accepted, terminal.
- Queue pressure remains part of end-to-end latency.
- Offered and accepted counts stay separate, so overload cannot look like throughput.
- Bounded histograms keep evidence size independent of run length.
- Rust and C adapters must encode those histograms byte for byte.

[`docs/EVIDENCE.md`](./docs/EVIDENCE.md) defines the documents, accounting rules,
and allowed conclusions.

## Scenarios and packs

- `scenarios/producer/headline/`: the runnable producer set.
- `scenarios/producer/producer-baseline.toml`: the parameter matrix, not a runnable scenario.
- `scenarios/producer/deferred/`: workloads blocked by a known client limit.
- `scenarios/packs/pr.toml`: one balanced attempt.
- `scenarios/packs/nightly.toml`: three repetitions of the full set plus capacity search.

[`scenarios/DEFERRED.md`](./scenarios/DEFERRED.md) names every blocked workload
and the specific limitation.

## Quickstart

```sh
git clone https://github.com/zsumz/kafka-benchmarks && cd kafka-benchmarks
cargo +1.96.0 install zrail --version 0.0.1 --locked
scripts/check
```

`scripts/check` is the review gate. It runs formatting, linting, tests, docs,
schema checks, zrail policy, detached-adapter policy, legacy tests, and
provenance checks.

On a bare clone, missing sibling checkouts are advisory. Strict provenance
checks the pinned public `kafkars/kafkars`, `kafkars/kafka-driver`, and
`kafkars/kafka-wire` revisions.

What needs the siblings is anything that builds or runs a real subject:

| Command | Needs |
| --- | --- |
| `scripts/check` | the pinned Rust and Node toolchains, plus zrail 0.0.1 installed with Rust 1.96 |
| the offline first run above | nothing but the pinned Rust toolchain |
| `scripts/generate-subject-config` | the three sibling checkouts, plus a librdkafka it downloads and builds on first use |
| `scripts/check-benchmarks` | the three sibling checkouts, plus a bootstrapped librdkafka |
| `scripts/bench-m0-acceptance`, `scripts/bench-suite-acceptance` | the above, plus a running broker |
| `KAFKA_BENCH_PROVENANCE=strict scripts/check-dependency-provenance` | the siblings, on their pinned revisions and clean |

Acceptance scripts exit 69 before changing anything when no broker is listening.
Their generated inputs and evidence stay under `target/`.

```sh
docker compose -f clusters/dev-compose/compose.yml up -d --wait
scripts/bench-m0-acceptance
```

The full procedure, the experiment format, and the meaning of every document in
a sealed bundle live in [`docs/BENCHMARK_HARNESS_DESIGN.md`](./docs/BENCHMARK_HARNESS_DESIGN.md);
what a result is allowed to claim lives in
[`docs/PERFORMANCE_CONTRACT.md`](./docs/PERFORMANCE_CONTRACT.md).

## Boundaries

kafka-benchmarks is not:

- **a leaderboard.** Results describe one experiment on one environment.
- **a private-internals benchmark.** Adapters use shipped public client surfaces.
- **a claim generator for laptop numbers.** Every current scenario is diagnostic only.
- **a Kafka client.** The subjects implement the protocol; this repository measures them.

## Status

Pre-0.1. The v2 engine has four-timestamp offer accounting, bounded histograms,
paired suites, capacity search, sealed-bundle reports, and a headline scenario
pack.

The public Kafkars surface now supplies exact measured-window Produce request,
partition-batch, record, and encoded-record-byte deltas. v2 seals them with
both boundary snapshots under `kafkars.kafkars-native-metrics.v1`; counters the
surface still lacks remain absent rather than inferred. The protocol-aware
loopback lane and the remaining internal counters stay in
[`docs/ROADMAP.md`](./docs/ROADMAP.md).

Nothing is published. The artifact is a sealed evidence bundle, not a crate.
Every current bundle records `claim_eligible: false`; unmet publication checks
remain explicit deferrals.

## Project

[Changelog](./CHANGELOG.md) · [Contributing](./CONTRIBUTING.md) ·
[Security](./SECURITY.md) · [Releasing](./RELEASING.md)

## License

Licensed under Apache-2.0. See [LICENSE](./LICENSE) and [NOTICE](./NOTICE).

Apache Kafka is a trademark of the Apache Software Foundation. This project is
independent and is not endorsed by the Apache Software Foundation.
