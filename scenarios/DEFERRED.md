# Deferred scenarios

The design document's producer matrix and `producer/producer-baseline.toml`
between them describe far more workloads than
`scenarios/producer/headline/` contains. This file names every row that is
missing on purpose, says which specific thing refuses it today, and names the
change that would unblock it.

The rule this file exists to enforce: a scenario is authored only when both
adapters can `validate` it and a three-broker development cluster can host it.
A scenario that resolves and is then declined by every subject produces a
sealed bundle full of refusals, which is worse than an entry in a list —
it looks like evidence.

Adapter limits below are cited from `adapters/kafkars/src/protocol.rs` and
`crates/bench-adapter-librdkafka/src/validate.rs`, which are the two places
that decide what runs.

## Application concurrency: 16 callers, and multiple producers

**Row.** `application_threads = [1, 4, 16]` and the design document's separate
"sixteen producers measure connection, producer-ID, memory, and I/O scaling"
shape.

**Refused by.** Both adapters fix the caller count by load mode: the
closed-loop phase admits from exactly one caller, and the fixed-rate phase
requires exactly four. Both also build exactly one producer handle and decline
any `producer_instances` other than 1. These are not configuration defaults —
the phase code is written around those counts, so honouring a request for 16
would mean running 1 or 4 and reporting it as 16.

**Unblocked by.** A caller count on the adapter argument surface, and a
fixed-rate scheduler that stripes an arbitrary caller count over the shared
monotonic epoch without changing per-partition order. Multiple producer
instances additionally need per-instance topic and identity accounting so the
verifier can still attribute every record.

## Keyed records and skewed key distributions

**Row.** `keyed = [false, true]`, and the design document's partition-skew,
per-partition byte, and Zipf-style distribution reporting.

**Refused by.** Both adapters assign partitions themselves, round robin by
record sequence, and decline any `partitioning` other than
`explicit-round-robin`. The scenario schema has no key axis at all: `[payload]`
carries `bytes`, `profile`, `seed`, and `identity`, and nothing else.

**Unblocked by.** A key generator in the payload contract (shared conformance
vectors, because the key bytes must be byte-identical across languages), a
`partitioning` vocabulary that includes client-chosen hashing, and a verifier
that checks the key as well as the value. Skewed distributions need the
generator to be seeded and reproducible before the partition-skew numbers mean
anything.

## Non-constant arrival processes

**Row.** Idle-to-burst admission, Poisson arrivals, and replayed trace
arrivals.

**Refused by.** Both adapters schedule evenly spaced arrivals — record `i` at
`floor(i * 1s / rate)` — and decline any experiment whose arrival model is not
deterministic. There is one arrival model in the resolved vocabulary.

**Unblocked by.** An arrival model in the scenario schema and matching
committed schedule vectors under `conformance/schedule/`, so that two languages
generate the same arrival times rather than two plausible ones. Burst shapes
also need the result document to separate the burst window from the idle
window, which the current single measured interval does not.

## Compression, and the compression tax

**Row.** `compression = ["none", "lz4", "zstd"]` and the payload-size by
codec by compressibility interaction sweep.

**Refused by.** Both adapters configure `compression=none` unconditionally and
declare exactly `["none"]` in their capability document. The pinned librdkafka
build compounds this: `scripts/bootstrap-librdkafka` configures
`--disable-zstd --disable-lz4-ext`, so that subject has no zstd at all and only
the bundled LZ4.

**Unblocked by.** A codec on both adapter argument surfaces, a librdkafka build
with the codecs enabled and its hash re-pinned, and the payload profile work
below — a compression number taken against one payload profile cannot support a
codec claim, so the codec axis is not worth running before the profile axis
exists.

## Payload profiles beyond the identity envelope

**Row.** The design document's three retained profiles: incompressible bytes,
moderately compressible structured data, and an application-like envelope with
stable keys and headers.

**Refused by.** Nothing declines it; there is simply one generator. Both
adapters build the `deterministic-ascii-envelope` payload and no other, and the
committed vectors under `conformance/payload/` describe that one.

**Unblocked by.** Two more generators, implemented identically in Rust and C
and pinned by committed vectors. Until then a `profile` value other than
`deterministic-ascii-envelope` would name a generator that does not exist.

## Client profiles: low latency and throughput

**Row.** `profiles = ["low-latency", "balanced", "throughput"]`, and the
design document's linger-by-offered-rate interaction sweep.

**Refused by.** Both adapters fix `linger_ms=5`, `batch_records=256`, and
`batch_bytes=65536` at compile time and decline any other value. Only the
balanced profile exists. This is why `latency-floor-128b-1p.toml` declares
`linger_ms = 5` and narrows its application window instead: the window is a
scenario input and the linger is not, so the floor it measures is a
one-batch-outstanding floor rather than a zero-linger floor.

**Unblocked by.** Linger and batch settings on both adapter argument surfaces.
They are already in the scenario schema and already validated — the scenario
side of this is done, and the adapter side is not.

## TLS

**Row.** `security = ["plaintext", "tls"]`, and the TLS-by-payload-size
interaction sweep.

**Refused by.** The kafkars adapter declares `tls: false` and never negotiates
one; the C adapter's library is built `--disable-ssl`. Neither argument surface
carries a security parameter — both take a bootstrap string and nothing else —
so there is no way to ask for TLS even if the libraries supported it.

**Unblocked by.** A security mode on both argument surfaces, a librdkafka build
with OpenSSL and a re-pinned hash, a cluster profile that can point at a TLS
listener, and certificate material in `clusters/dev-compose/` that is
obviously test-only.

## The literal 1 MiB record

**Row.** `payload_bytes = [..., 1048576]`, the top of the primary payload axis.

**Refused by.** The cluster, not the adapters. A broker's default
`message.max.bytes` is 1,048,588 bytes and the librdkafka default producer-side
limit is 1,000,000; a 1,048,576-byte payload plus its identity envelope, key,
and record and batch framing exceeds both. The client would also need
`request_bytes` above the 1,048,576 both adapters fix.

**Unblocked by.** A raised-limit lane: broker and topic `message.max.bytes`,
the client's request budget, and the adapter's fixed request size all raised
together, and every one of those values recorded in the bundle. The design
document is explicit that a raised-limit result does not share a score with
default-compatible runs, so this is a separate lane rather than one more row —
which is why `near-limit-900k-3p.toml` exists at 900,000 bytes instead.

## Consumer, end-to-end, faults, and soak

**Row.** Consume paths, steady state, leader movement, reconnect, rebalance,
completion backpressure, the 30- to 120-minute soak, and the cold-start and
broker-race workloads.

**Refused by.** There is no consumer adapter. Both adapters declare
`consumer: false`, the experiment kind vocabulary has exactly one variant
(`producer`), and the only consumer in the repository is the verifier — which
reads a topic back for correctness and is deliberately not a measured subject.
Fault injection has no home either: nothing in the control plane can restart a
broker or degrade a link mid-run.

**Unblocked by.** A consumer kind in the experiment schema and a consumer
measurement document, then a consumer adapter on each subject. Faults need a
cluster-control surface separate from the adapter protocol, on the same
principle that keeps verification outside it. Soak needs the evidence-retention
policy decided first, because a two-hour run at the current per-record sampling
produces evidence nobody has said where to keep.
