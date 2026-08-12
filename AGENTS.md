# Repository contract

This repository produces evidence about Kafka clients. It is not a Kafka client,
and it is not a place to make a client look good.

## Before coding

- Read [`ARCHITECTURE.md`](ARCHITECTURE.md) and, for anything touching results,
  `docs/PERFORMANCE_CONTRACT.md`.
- Identify which side of the boundary the change is on: control plane, adapter,
  verifier, or evidence document. Code that crosses that boundary is almost
  always a design mistake in disguise.
- State what a reader of a sealed bundle will be able to conclude that they
  could not conclude before, and what they must still not conclude.
- Prefer recording a check as deliberately deferred over silently not doing it.
  A named gap in `classification.deferred_checks` is honest; an absent check
  that nobody wrote down is not.

## Non-negotiable rules

- **No async runtime in this repository's own code**, including
  dev-dependencies. A harness that borrows a scheduler cannot describe the
  scheduling behavior of what it measures. `signal-hook` is the one concession,
  because `unsafe_code` is forbidden and an interrupt must still seal. The rule
  is about what the *harness* links, not about what appears anywhere in a lock
  file: the client under test brings its own dependency graph, and the kafkars
  adapter transitively acquires `mio` through it. That is the subject's
  business. The control plane, the evidence crates, and the adapters' own code
  stay free of one.
- **No `criterion`, no `divan`, no benchmarking framework.** Timing, warmup, and
  statistics are the subject matter here, not an imported convenience.
- **The harness model is a process-spawning control plane plus adapter
  binaries.** Subjects are never linked into the control plane, and the control
  plane never links a Kafka client.
- **Adapters depend only on shipped public client surfaces.** If a measurement
  needs a private hook, either the client should ship that surface or the
  measurement belongs in the client's own repository.
- **Evidence schemas are versioned and append-only.** Add a field, or mint a new
  schema id. Never change what an existing field means under an existing id:
  sealed bundles are immutable, and a redefinition retroactively falsifies runs
  that already happened. This extends to the goldens: a change that alters the
  bytes of a committed vector, a schema document under `schemas/`, or a
  conformance fixture is a schema-affecting change, not a test fixup. Regenerate
  a golden only together with the new schema id that justifies it, and say so in
  the changelog. The one carve-out: the documents under `schemas/` are
  *documentation mirrors* of the serde types, which are the source of truth, so
  correcting a mirror that mis-described an **unchanged** wire format is a
  documentation fix rather than a schema-affecting change — the bytes on disk
  never moved, only the description of them. It stays a schema-affecting change
  the moment the wire format itself moves.
- **The histogram encoding is a byte contract, not an implementation detail.**
  `kafkars.log-linear.v1` fixes the bucketing (`SUB_BUCKET_BITS = 7`), the field
  order — `layout`, `unit`, `sub_bucket_bits`, `total`, `min`, `max`, `sum`,
  `counts` — and the sparse `counts` form: strictly ascending indexes, no zero
  counts. Compact serde output of the Rust struct is the reference encoding, and
  the C adapter must reproduce it byte for byte, because cross-language
  conformance asserts equality rather than tolerance. Percentiles report a
  bucket's inclusive upper bound so a derived latency errs conservative; `min`,
  `max`, and `sum` stay exact outside the buckets. Changing any of this means a
  new layout id.
- **Every sealed bundle is immutable.** Nothing rewrites a bundle after sealing,
  including to fix it. A wrong bundle is superseded by a new attempt, never
  edited.
- **An adapter never decides its own validity.** Verification and topic
  management are configured tools invoked by the control plane, outside the
  adapter protocol.
- **Identity documents carry integers only.** Anything hashed into an experiment
  id is free of floating-point numbers. Ratios and rates are evidence, and live
  in documents that are never hashed into an identity.

## Rust source shape

- Every Rust source file begins with a `//!` module contract.
- `lib.rs` and `mod.rs` are declarative facades: module declarations and
  re-exports only. The one carve-out is `tests/common/mod.rs`: cargo mandates
  that filename for helpers shared between integration test binaries — any
  other name in `tests/` is compiled as a test target of its own — so that file
  carries real code and is not a facade.
- Unit tests live in sibling `*_test.rs` files, declared with
  `#[cfg(test)] mod ...;` from the nearest facade.
- In the crates of *this* workspace, warnings are denied in clippy and rustdoc
  and `missing_docs` is on, so an undocumented public item fails the build. The
  adapter workspaces under `adapters/` are separate: the kafkars adapter carries
  its own, narrower lint set and no rustdoc gate, because it is a binary built
  against a pinned client rather than a documented library surface. Do not
  assume a lint is in force there because it is in force here — check
  `adapters/kafkars/Cargo.toml`.
- `unwrap`, `expect`, `todo!`, `unimplemented!`, and `dbg!` are denied. Tests
  that genuinely want a panic on a bad fixture opt in with an explicit
  `#![expect(clippy::unwrap_used, reason = "...")]`.
- Run `scripts/check` before requesting review.

## Deferred decisions

Recorded here so that they stay decisions rather than becoming accidents. The
two below are repository-contract deferrals; the current loop's deferrals live
in [`docs/ROADMAP.md`](docs/ROADMAP.md), and workloads that cannot run yet live
in [`scenarios/DEFERRED.md`](scenarios/DEFERRED.md).

- **Guardrails-crate enforcement is deferred.** The sibling client repository
  enforces dependency edges, file counts, and capability ownership with a
  dedicated `guardrails` crate and a `guardrails.toml` policy. This repository
  has no such crate yet; the async-runtime and benchmarking-framework bans live
  in the workspace lints and in this file, which means they are enforced by
  review and by the dependency tree rather than by a test. Adding the crate is
  worthwhile once the workspace stops changing shape every wave.
- **Evidence storage and publication policy is deferred.** Bundles are written
  to a gitignored `results/` tree. Nothing decides yet what is retained,
  archived, or published.
