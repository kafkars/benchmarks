# Legacy diagnostic harness

This documents the migrated legacy fixture path: the Node control plane now at
`legacy/benchctl/`, the adapters under `adapters/`, and the `scripts/bench-*`
entrypoints, preserved so their behavior stays a reference. It is not the
target design. The generic `benchctl` engine in `crates/benchctl` is the
replacement, and every command below is run from the repository root.

The canonical runner, adapter, workload, validity, statistical, and evidence
design is [`BENCHMARK_HARNESS_DESIGN.md`](BENCHMARK_HARNESS_DESIGN.md).

The first executable comparison now launches each client as a separate process
from one shared workload description:

- the public `kafkars` Rust facade; and
- raw librdkafka C, pinned to `v2.15.0` and its official source checksum.

The measured kafkars phase uses its public `send_batch` API and records one
aggregate completion observation for every accepted result. It admits bounded
256-record batch futures into a sliding window whose total offered records
never exceeds the configured application-outstanding limit. The raw
librdkafka adapter uses public `rd_kafka_produce_batch` calls with the same
application batch and window bounds. Its delivery callbacks settle one
aggregate batch terminal, so both measured paths observe latency from the
public batch call boundary until every record in that application batch has a
terminal result. Warmup remains outside the timed comparison.

The Apache Kafka Java producer and the packaged language bindings remain later
adapters. The implemented diagnostic workload is
[`legacy-balanced-1k.toml`](../scenarios/producer/legacy-balanced-1k.toml).
It uses four separate replicated topics so warmup never contaminates measured
verification. The measured phase assigns records round-robin to explicit
partitions in both warmup and measurement. Both clients use 600 bounded
replacement attempts with fixed 100 ms backoff inside the original 60-second
delivery deadline, so fresh-route discovery does not compare kafkars' cautious
three-retry default against librdkafka's much larger default retry budget. The
untimed warmup first acknowledges one serialized record per partition before
opening the configured concurrency window. Both measured sliding windows are
bounded by that same limit.
The normalized durability, batching, retry, queue, and application-outstanding
settings are identical. librdkafka is explicitly capped at five in-flight
requests per broker connection, and kafkars uses its public
`max_in_flight_requests_per_broker=5` producer limit. The sealer rejects a
kafkars result whose exact observed native peak exceeds that configured bound.
The raw C adapter captures librdkafka's official statistics every 100 ms into
one bounded in-memory buffer during measurement, then writes the raw JSONL only
after the timed phase. Sealing proves the cumulative measured record, request,
batch, partition, retry, timeout, and final-drain counters, preserves native
queue and latency windows, and normalizes the achieved request and batch shape.
librdkafka's in-flight observation is a sampled gauge rather than an exact
peak, so the result labels it accordingly.

The default 8,192-record application window covers at least two full pipeline
waves at this workload's three-broker, five-request, 64 KiB partition-batch
shape. Both adapters receive the same bound. Smaller windows remain valid
diagnostics, but can measure load-generator starvation instead of sustainable
client capacity.

The scheduled-load diagnostic uses the same public batch calls from four
caller threads. Record `i` retains the exact integer-nanosecond intended time
`floor(i * 1s / rate)` even when a caller or client is late. A short admission
turn linearizes overlapping public calls in canonical batch order; completion
progress remains independent. This gives the verifier a defined per-partition
order without hiding admission pressure. Both adapters report submission-to-
terminal latency, intended-time-to-terminal corrected latency, schedule delay,
complete drain, and their native Produce request shape.

Build and conformance-check both adapters with:

```bash
scripts/check-benchmarks
```

Run five balanced diagnostic paired blocks against an already-running
three-broker plaintext cluster with:

```bash
KAFKARS_BENCH_BROKER_VERSION=4.3.1 \
  scripts/bench-producer-suite localhost:19092,localhost:29092,localhost:39092
```

The suite alternates which client runs first, validates every pair, and seals
the paired median, geometric mean, range, coefficient of variation, and a
deterministic 50,000-resample paired-block bootstrap confidence interval. Five
paired blocks and at most 5% coefficient of variation for both goodput and p99
are the minimum statistical-credibility gate. Use
`scripts/bench-producer-compare` only when one diagnostic pair is intentional.

Run five balanced scheduled-load pairs at one explicit diagnostic rate with:

```bash
KAFKARS_BENCH_RECORDS=100000 \
KAFKARS_BENCH_WARMUP_RECORDS=10000 \
KAFKARS_BENCH_OFFERED_RATE=100000 \
  scripts/bench-producer-fixed-suite \
    localhost:19092,localhost:29092,localhost:39092
```

The fixed-load suite applies the same alternating order, exact append/fetch
verification, checksums, paired bootstrap interval, and 5% variation gate to
corrected and uncorrected p99 ratios. One rate remains diagnostic; claim
evaluation still requires rates derived from a valid sustainable-capacity
curve.

Measure the raw librdkafka reference curve with the predeclared stable-window
and SLO contract:

```bash
scripts/bench-producer-reference-capacity \
  localhost:19092,localhost:29092,localhost:39092
```

The runner expands or contracts to bracket a pass and failure, refines the
boundary to 5%, repeats every rate five times, and seals exact verification,
native queue trend, retry, timeout, drain, latency, and checksum evidence. A
valid but overloaded probe contributes a failure side; invalid evidence aborts
the curve.
Interrupted curves preserve incomplete attempts and can resume without
rerunning sealed probes by setting `KAFKARS_CAPACITY_RESUME=true` and passing
the same result root. A completed aggregate is immutable and cannot be resumed.

Run the four comparison points from the sealed capacity summary, without
restating or choosing their rates:

```bash
scripts/bench-producer-fixed-matrix \
  localhost:19092,localhost:29092,localhost:39092 \
  target/benchmark-results/reference-capacity-<id>/capacity-summary.json
```

The matrix copies and hashes its capacity reference and runs balanced suites at
exactly 25%, 50%, 75%, and 90%. It reports statistical credibility separately
from latency parity and leadership. Smoke profiles deliberately use fewer
repetitions, a shorter window, and a coarser search; their schemas are valid but
their evidence gates remain false.
An interrupted matrix similarly resumes complete load points with
`KAFKARS_MATRIX_RESUME=true`; any partial point is archived as an aborted
attempt before its deterministic replacement begins.

The runner creates and deletes only its uniquely named topics. It records raw
latency samples at each adapter's observable completion boundary, adapter
summaries, independent raw-C Fetch verification,
source/toolchain/host identity, whole-adapter process CPU, raw maximum RSS, and
platform-calibrated peak memory including warmup, the resolved workload plus
its source template, execution order, and
checksums under
`target/benchmark-results/`.
Raw latency and librdkafka statistics streams are validated before sealing,
then gzip-compressed without loading the complete file into the control plane's
memory. Checksums cover the compressed immutable bytes and normalized summaries
retain the fields needed for aggregate evaluation.

Every bundle from this runner is deliberately `claim_eligible: false`. The
capacity curve and derived matrix can become statistically credible, but this
developer runner still lacks calibrated stable-runner CPU/memory thresholds and
broker/host limiting-resource classification and covers only one balanced
producer workload. Raw platform resource-meter output remains in every bundle;
normalized CPU core-seconds and peak memory fail closed when unavailable. On
Darwin, peak memory is `/usr/bin/time`'s physical-footprint metric and maximum
RSS remains a separate diagnostic; on Linux, GNU time's maximum RSS is the
available peak-memory metric. Darwin instruction and cycle counters are retained
when the host exposes them.

Later adapters add:

- rust-rdkafka `BaseProducer` and `FutureProducer`;
- Apache Kafka Java client;
- later, the Java binding over the shared engine.

A runner must record the exact client and broker versions, hardware, kernel,
security mode, payload distribution, semantics, warmup, and test duration.
Results without matched semantics are diagnostic only and cannot support a
performance claim.
