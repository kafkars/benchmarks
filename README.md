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

The design follows from taking that question literally.

- **Subjects are processes, not libraries.** Every client under measurement runs
  as its own operating-system process behind a small adapter protocol. Two
  clients linked into one binary share an allocator, a thread scheduler, and a
  page cache, and a harness that links its subjects ends up measuring itself.
  The process boundary is also what lets a client written in another language be
  compared at all.
- **Nobody grades their own homework.** An adapter reports what it believes it
  produced. A separate verifier reads the topic back and reports what the broker
  retained. Cross-client validity is decided from the verifier's report.
- **Every attempt seals.** Crashes, timeouts, and interrupts produce a sealed
  bundle recording exactly that. The runs worth studying most are the ones that
  went wrong, and a harness that only writes evidence on success is a harness
  that quietly deletes its own bad news.
- **Evidence is versioned and immutable.** Schemas are append-only: a new field
  or a new schema id, never a changed meaning under an old id. A sealed bundle
  carries two identities — one for the intent that was run, one for the bytes
  that resulted — so a bundle can be re-verified long after the machine that
  produced it is gone.
- **Validity is a separate axis from success.** A run can complete cleanly and
  still be too noisy, too short, or too unverified to support a claim. Those are
  different fields, and the harness never collapses them into one.

## Layout

```txt
crates/bench-schema      evidence documents, canonical JSON, identities
crates/benchctl          control plane: resolve, spawn, supervise, seal
crates/bench-adapter-librdkafka   adapter-protocol shim for the C reference
crates/bench-verifier    typed views over independent read-back evidence
crates/bench-report      descriptive statistics over sealed bundles

adapters/                subject adapters; standalone workspaces, not members
legacy/benchctl/         the Node harness this repository was extracted from
scenarios/               human-authored experiment definitions (TOML)
schemas/                 one JSON Schema document per schema id
conformance/             committed payload and schedule vectors
clusters/                local broker topologies for development runs
scripts/                 the gate and the harness entry points
docs/                    the performance contract and harness design
```

The adapters are deliberately not workspace members. A subject must be built
against its own pinned dependency graph, not against whatever this workspace
happens to resolve.

## Quickstart

```sh
scripts/check
```

That is the single gate: it formats, lints, tests, and documents the Rust
workspace, checks the schema registry against `schemas/`, runs the legacy
control-plane tests, and reports on sibling-checkout provenance. Lanes whose
files do not exist yet report themselves as skipped rather than passing
silently.

Running an actual benchmark needs a broker and a built adapter set. The full
procedure, the experiment format, and the meaning of every document in a sealed
bundle live in [`docs/BENCHMARK_HARNESS_DESIGN.md`](./docs/BENCHMARK_HARNESS_DESIGN.md);
what a result is allowed to claim lives in
[`docs/PERFORMANCE_CONTRACT.md`](./docs/PERFORMANCE_CONTRACT.md).

## Boundaries

kafka-benchmarks is not:

- **a leaderboard.** It produces evidence for a specific experiment on specific
  hardware. Ranking clients in general is not something a benchmark can do, and
  publishing a table that implies otherwise is the failure mode this repository
  exists to avoid.
- **a microbenchmark suite for private internals.** Adapters depend only on
  shipped public client surfaces. If a measurement requires reaching inside a
  client, it belongs in that client's repository, not here.
- **a claim generator from developer-host numbers.** Results measured on a
  laptop, or against a broker sharing that laptop, are diagnostic. They are
  useful for spotting a regression while working; they are never evidence for a
  public statement about performance.
- **a Kafka client.** Nothing here implements the protocol. The subjects do.

## Status

Pre-0.1 and moving. The harness was extracted from the private
`zsumz/kafka-client-private` repository, where it had grown into a hard-coded
two-subject script; this repository is the generalization of that work into a
lab that can measure any client behind the adapter protocol.

Nothing is published. No crate from this workspace goes to a registry, and the
artifact this repository produces is a sealed evidence bundle, not a release.

Every bundle sealed today records `claim_eligible: false`. That is deliberate,
not an oversight: the checks that would justify a public performance claim —
sufficient paired repetitions, a dedicated cluster, provenance strict enough to
name what was running — are recorded as deferred rather than quietly assumed.

## Project

[Changelog](./CHANGELOG.md) · [Contributing](./CONTRIBUTING.md) ·
[Security](./SECURITY.md) · [Releasing](./RELEASING.md)

## License

Licensed under Apache-2.0. See [LICENSE](./LICENSE) and [NOTICE](./NOTICE).

Apache Kafka is a trademark of the Apache Software Foundation. This project is
independent and is not endorsed by the Apache Software Foundation.
