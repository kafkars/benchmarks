# Cross-client benchmark harness design

Status: accepted; first diagnostic producer comparison implemented

Provenance: extracted from zsumz/kafka-client-private @ cf2b4a59.

This document defines how the repository will produce reproducible performance
evidence for the native Rust client and later language bindings. It turns the
claim thresholds in [`PERFORMANCE_CONTRACT.md`](PERFORMANCE_CONTRACT.md) and the
benchmark levels in `API_ABI_PERFORMANCE_RFC.md` (which stays in the
`kafka-client` repository) into an executable measurement design.

The performance contract owns release and marketing thresholds. This document
owns workload execution, comparison fairness, result validity, and evidence
packaging. A threshold change must update the performance contract; a harness
change that can alter a measured result must update this document.

## Decision summary

- Raw librdkafka C is the primary native baseline.
- rust-rdkafka `BaseProducer` measures Rust wrapper overhead and its
  `FutureProducer` measures the idiomatic Rust product path. They are separate
  interfaces over librdkafka, not independent Kafka engines.
- The Apache Kafka Java producer is the independent implementation and Java
  ecosystem baseline.
- Every headline workload runs in both capacity and fixed-load modes.
- All clients receive the same application-boundary outstanding-record and
  retained-byte budgets. Native client limits are configured as closely as
  possible and their exact mappings are recorded.
- Headline goodput counts broker-acknowledged records and payload bytes, not
  calls attempted or records admitted to a local queue.
- A scheduled open-loop load model prevents a slow client from reducing the
  number of latency observations made during its stalls.
- Correctness verification, bounded queue growth, complete drain, and
  environment classification precede every comparison or claim gate.
- Paired repetitions and confidence intervals replace single point-estimate
  verdicts.
- Cold-client startup and fresh-broker recovery are distinct scenarios and do
  not share the steady-state throughput score.
- The first executable slice is producer-only: `kafkars` versus raw
  librdkafka C under one balanced replicated workload.

## Goals

The harness must answer five questions without conflating them:

1. What acknowledged goodput can each client sustain inside a declared latency,
   failure, queue, CPU, and memory contract?
2. At the same offered load, which client uses less CPU and memory and produces
   better completion latency?
3. Where does time and capacity go inside each client: admission, queueing,
   batching, wire execution, or completion delivery?
4. Does each client retain correctness and bounded resources through overload,
   broker movement, reconnect, rebalance, and slow application behavior?
5. Do FFI and language-binding paths preserve the engine's performance after
   their real packaging, dispatch, and memory-ownership costs are included?

The harness must produce evidence that another operator can reproduce from the
recorded workload, source revisions, build settings, environment, and raw
results.

## Non-goals

- A default-configuration contest.
- One maximum-throughput number presented as a universal client ranking.
- A developer-laptop result used as a hard release gate.
- A benchmark-only public client abstraction. Microbenchmarks may use private
  crate seams, but cross-client comparisons use existing product surfaces.
- Hiding durability, copy, queueing, retry, partitioning, or completion
  differences behind similarly named settings.
- Consumer, C ABI, Java binding, fault, and soak coverage in the first runner
  slice. They follow the native producer baseline without weakening this
  design.

## Comparison lanes

| Lane | kafka-client surface | Baseline | Question |
| --- | --- | --- | --- |
| Native Rust product | `kafkars` public producer and operation terminal | Raw librdkafka C and rust-rdkafka `BaseProducer` | Is the shipped Rust producer competitive, and what is wrapper overhead? |
| Rust ergonomic path | Public future-based producer observation | rust-rdkafka `FutureProducer` | What does an idiomatic Rust application pay? |
| Native C ABI | Packaged batch-native `kafka-client-ffi` surface | Raw librdkafka C with matched ownership and copy behavior | Does the stable native boundary preserve engine performance? |
| Java product | Packaged Java binding including dispatch and JNI or FFM | Apache Kafka Java producer | Is the Java-facing product competitive end to end? |

Where possible, raw librdkafka and rust-rdkafka must use the same exact
librdkafka source, features, TLS library, and compression libraries. A build
difference is part of the environment identity and invalidates a wrapper-cost
claim unless it is explicitly the subject of the test.

An adapter may expose multiple existing completion paths, but the report must
name the exact path. A no-copy unsafe C lifetime contract is not compared to a
safe copy-in Rust path as though their ownership semantics were identical.

## Ownership model

The harness keeps timing, bytes, completion, and cancellation ownership
explicit.

### `benchctl`

The control plane owns:

- validated workload and environment identities;
- monotonic phase boundaries and the canonical offered-load schedule;
- topic provisioning and cluster preflight;
- adapter process lifecycle and run timeout;
- drain, verification, classification, and evidence-bundle completion;
- randomized paired-block order; and
- final immutable result publication.

It never forwards one IPC command per Kafka record. Per-record control-plane
traffic would become part of the benchmarked hot path.

### Adapter process

Each adapter owns:

- translation from canonical workload settings to one client API;
- local execution of the canonical deterministic schedule;
- payload construction from prevalidated pools outside the timed interval;
- calls into the client and client-specific polling or runtime work;
- application-boundary outstanding count and byte accounting;
- accepted, acknowledged, failed, timed-out, and unknown terminals;
- client-native metrics capture; and
- complete drain or an explicit drain failure.

The schedule algorithm and payload generator have shared conformance vectors.
Adapters run those vectors before a measured workload so C, Rust, and Java do
not silently generate different arrival times or bytes.

### Kafka client

After admission, the client under test owns accepted record bytes, batching,
requests, retries, delivery terminals, and its internal bounded resources
according to its product contract. The harness does not repair or hide client
backpressure, ordering, retry, or completion behavior.

### Verifier

The verifier consumer owns the post-run broker-visible record set and produces
one terminal verification result. A performance result is not claim-eligible
until verification succeeds.

## Run state machine

One run follows a fixed sequence:

```text
validate workload and environment
  -> provision or validate topics
  -> launch one adapter
  -> adapter setup and readiness
  -> warmup or cold-start capture
  -> measured load
  -> stop new offers
  -> drain every accepted record
  -> verify broker-visible records
  -> collect client and broker metrics
  -> classify the limiting resource
  -> seal the immutable result bundle
```

Every phase has one monotonic deadline. Expiry is a named terminal result, not
an implicit extension or a partial success. Cancellation first asks the adapter
to stop offers and drain. Forced process termination makes the run invalid and
is preserved as evidence.

## Measurement surfaces

### Capacity mode

Capacity mode uses a scheduled open-loop ramp. Each step declares an offered
rate and a stable measurement window. The runner increases offered load until
the client violates any predeclared service objective:

- p99 completion latency;
- delivery failure or timeout rate;
- accepted-but-unsettled count or byte budget;
- positive queue-growth slope over the stable window;
- process CPU or RSS ceiling; or
- incomplete drain.

The result is the maximum sustainable acknowledged goodput inside that
contract. A larger local queue cannot manufacture a higher capacity score.

The balanced reference curve predeclares a 10-second measured window after a
two-second warmup and five repetitions per rate. It begins at 25,000 records/s,
doubles until it brackets one all-repetition pass below one failure, and then
binary-refines that bracket to at most 5% of the passing rate. If the initial
rate fails, the same search contracts by halves to the 1,000 records/s floor.
Reaching the 1,000,000 records/s ceiling without a failure or the floor without
a pass is inconclusive rather than a fabricated capacity.

The disposable smoke quorum enables topic deletion and removes deleted log
segments without Kafka's ordinary file-deletion grace period. This affects
only post-verification cleanup between uniquely named benchmark topics, never
the measured append/fetch path. It prevents a repeated verified curve from
requiring disk for every already-deleted topic at once.

A valid probe requires exact append/fetch verification, every offered record
acknowledged, no delivery failure, no native retry or timeout, complete drain,
and a native queue no larger than the 8,192-record application budget. Its
corrected p99 must be at most 250 ms, schedule-delay p99 at most 50 ms, and
post-schedule drain tail at most one second. At least three 100 ms native queue
samples are required. Least-squares queue growth must not exceed the greater
of 1,000 records/s or 1% of offered load. The first validity failure aborts the
curve; only a valid SLO miss is an overload observation.

### Fixed-load mode

Fixed-load mode runs every client at the same absolute offered rate. Initial
rates are 25%, 50%, 75%, and 90% of raw librdkafka's valid sustainable capacity
for the exact workload. The reference capacity and derived rates are stored
with every run.

The adapter schedules offers independently of earlier completions. For record
`i` at rate `r`, its intended start is derived from the shared monotonic epoch
and `i / r`; a blocking call or slow completion cannot move later intended
starts. The harness records both:

- uncorrected latency from actual API submission to terminal; and
- coordinated-omission-corrected service latency from intended schedule time
  to terminal.

An offer that cannot reach the API because callers or the application budget
are full remains an offered-but-not-accepted event. It is counted as admission
pressure, not silently omitted from the workload.

Latency, CPU, and RSS comparisons are valid only when each client sustains the
fixed load, stays inside the declared resource budgets, and drains completely.

### Startup modes

Cold startup is split into two separately named workloads.

**Cold client against a ready broker** records process start to producer
construction, metadata readiness, first acknowledgment, and the first 100 and
1,000 acknowledgments.

**Client racing a newly started broker** records producer-ID and metadata
recovery, time from broker readiness to first successful append, transient
failures exposed to the application, and duplicate, gap, ordering, or unknown
outcomes.

The broker-race workload is a recovery qualification result, not a steady-state
throughput result.

### Overload, burst, soak, and degraded modes

Later suites add:

- idle-to-burst admission and batching;
- sustained overload and bounded backpressure;
- 30- to 120-minute RSS, queue, allocation, and latency-spike soak;
- broker restart and leader movement;
- rolling restart, delay, loss, and throttling;
- authentication refresh;
- consumer rebalance and slow polling; and
- full completion queues.

Fault results include detection time, lowest acknowledged goodput, queue and
RSS peak, time to recover to 90% of pre-fault goodput, and every duplicate,
gap, ordering, timeout, or unknown outcome.

## Canonical workload model

The workload schema separates dimensions that client-specific configuration
names often conflate. This is the middle of the canonical point,
[`scenarios/producer/headline/balanced-1k-12p.toml`](../scenarios/producer/headline/balanced-1k-12p.toml),
excerpted verbatim — see that file for the header comment, the `[validity]`
block, and the question it exists to answer:

```toml
name = "producer-balanced-1k-12p"
status = "diagnostic"
claim_eligible = false
load_mode = "scheduled-open-loop-fixed-rate"
records = 300000
warmup_records = 30000
offered_records_per_second = 100000

[application]
producer_instances = 1
callers_per_producer = 4
backpressure = "block-within-original-offer"
queue_bytes = 67108864
max_outstanding_records = 8192

[application_api]
admission_shape = "public-batch"
completion_shape = "aggregate-batch-terminal"
batch_records = 256

[payload]
bytes = 1024
profile = "deterministic-ascii-envelope"
seed = 44
identity = "KFB1 plus 16-byte run ID plus 64-bit sequence"

[producer]
acks = "all"
idempotence = true
compression = "none"
linger_ms = 5
batch_records = 256
batch_bytes = 65536
request_bytes = 1048576
delivery_timeout_ms = 60000
partitioning = "explicit-round-robin"
max_in_flight_requests_per_broker = 5
retry_max_replacements = 600
retry_backoff_ms = 100
```

Two things this authored form does that the resolved document does not. The
work is counted in **records rather than seconds**, so two clients do the same
amount of work rather than the same amount of waiting. And `[application_api]`
is a separate section here that the resolver folds into `application`, while
`payload.seed` becomes the experiment's single top-level `seed`: the authored
file is organized for a person deciding a workload, the resolved
`kafkars.experiment.v1` for a machine hashing one. `benchctl resolve` prints the
second from the first, and `schemas/kafkars.experiment.v1.schema.json` describes
what comes out.

The current
[`producer-baseline.toml`](../scenarios/producer/producer-baseline.toml)
remains an input inventory. Implementation has split it into the predeclared
headline set under `scenarios/producer/headline/` and targeted sweeps rather
than blindly evaluating its full Cartesian product; rows that cannot run yet are
named, with what refuses each one, in
[`scenarios/DEFERRED.md`](../scenarios/DEFERRED.md).

## Queue and backpressure fairness

Every adapter enforces the canonical application-boundary budgets in addition
to configuring the closest native client limits:

- `queue_bytes` bounds serialized key, value, header, and benchmark-envelope
  bytes accepted but not terminal;
- `max_outstanding_records` bounds accepted records without terminals;
- `backpressure` defines blocking or rejection behavior; and
- `enqueue_timeout_ms` bounds time spent trying to cross admission.

The adapter does not retain another unbounded queue in front of the client.
Scheduled offers are logical identities; payload bytes are selected or derived
only when a caller attempts admission. Native queue configuration, the adapter
mapping, any approximation, and observed native queue peaks are stored in the
result bundle.

Four counts are always distinct:

- **offered**: scheduled application attempts;
- **accepted**: records whose ownership crossed the client API boundary;
- **acknowledged**: accepted records completed successfully by Kafka; and
- **verified**: acknowledged identities observed by the verifier consumer.

Headline records/s and bytes/s are acknowledged goodput. Offered and accepted
rates diagnose admission and queue behavior; they never replace goodput.

Producer instances, callers per producer, and runtime workers remain separate.
Sharing one producer across 16 callers measures contention. Sixteen producers
measure connection, producer-ID, memory, and I/O scaling.

## Semantic equivalence

Every comparison records and validates observed behavior, not only requested
configuration.

### Replicated idempotent baseline

The real-cluster producer baseline uses:

```text
brokers = 3
replication.factor = 3
min.insync.replicas = 2
acks = all
enable.idempotence = true
unclean.leader.election.enable = false
```

This is called the replicated idempotent baseline. It does not claim an
`fsync` per message. Client and topic settings must also match retries,
ordering, request size, delivery deadline, and in-flight limits.

### Partitioning and batching

Keyed and unkeyed results report:

- records and bytes per partition;
- partition skew;
- records and bytes per batch distributions;
- Produce request sizes and counts;
- queue residence and effective linger;
- requests in flight; and
- retries and timeouts.

Two clients with the same configured linger but different achieved batch
distributions are not treated as semantically matched without explanation.

### Payloads and compression

Payload pools are generated outside the timed interval from fixed seeds. At
least three profiles are retained:

- incompressible bytes;
- moderately compressible structured data; and
- an application-like envelope with stable keys and headers.

The benchmark identity and sequence are part of the declared payload size.
Compression runs report source bytes, compressed wire bytes, and achieved
ratio. A compression result using one payload profile cannot support a general
codec claim.

A literal 1 MiB payload is a raised-limit workload because record and batch
framing can exceed common default request limits. Default-compatible large
record coverage uses a smaller declared payload. Raised-limit runs record
client, broker, and topic limits and do not share the default-compatible score.

### Copy and serialization contracts

Serialization is included or excluded for every client consistently. The
result records whether payload bytes were generated, copied, borrowed, or
owned across the public call. A low-level no-copy lane and a safe copy-in lane
are separate workload families.

## Correctness verifier

Every record carries a compact run identity, sequence, intended partitioning
input, and payload checksum. After offers stop and accepted work drains, a
verifier consumer checks:

- accepted count against every delivery terminal;
- successful terminals against broker-visible identities;
- missing and duplicate sequences;
- per-partition order;
- unexpected partitions;
- key, header, payload, and checksum integrity; and
- records reported with an unknown delivery outcome.

The verifier output is a hard validity gate. Expected fault-test unknown
outcomes are still enumerated and prevent an ordinary success classification.
No run with corrupt, missing, duplicated, or unexplained records contributes to
a performance claim.

## Metrics

### Normalized metrics

All adapters report:

- offered, accepted, acknowledged, failed, timed-out, unknown, and verified
  records and payload bytes;
- acknowledged records/s and MiB/s;
- p50, p95, p99, and p99.9 corrected and uncorrected latency;
- process core-seconds per million acknowledged records and per GiB;
- raw maximum RSS, platform-calibrated peak process memory, and retained-byte
  high-water marks;
- application-boundary outstanding count and bytes;
- request, retry, timeout, and wakeup counts;
- batch record and byte distributions;
- network bytes and request counts; and
- partition distribution and compression ratio.

Allocation counts are captured with platform-specific tools where available.
Their method and comparability classification are part of the result rather
than assumed equivalent across Rust, C, and managed runtimes.

### Client-native metrics

Adapters preserve native metrics in addition to the normalized view. The
`kafkars` adapter records the existing call, failure, mailbox, latency,
and producer ownership snapshots. The librdkafka adapter records its queue,
in-flight request, internal latency, RTT, batch, retry, timeout, and
per-partition statistics. Java runs include producer metrics, GC, and safepause
data.

Normalized values never discard raw native snapshots.

### Broker and host metrics

Each run records broker and load-generator CPU, disk, network, throttling,
request queues, page-cache-relevant host facts, frequency policy, and thermal
or steal indicators available on the stable runner.

The runner classifies the result as:

- client-limited;
- broker-limited;
- network-limited; or
- inconclusive.

Only valid client-limited runs support a client-engine leadership claim.
Thresholds for classification are calibrated and versioned per stable runner,
not invented after observing a result.

## Statistical design

Clients run in randomized paired blocks against the same hardware and cluster
state. Release decisions begin with five paired repetitions and add repetitions
when the observed noise exceeds the predeclared budget. A run that cannot
reach the required precision before the configured repetition ceiling is
inconclusive.

Reports include:

- every raw repetition;
- median result per client;
- median paired ratio;
- block-bootstrap 95% confidence interval for the paired ratio;
- coefficient of variation and the configured noise budget; and
- the randomized execution order.

The executable first runner uses a fixed-seed, 50,000-resample percentile
bootstrap over whole paired blocks. It reports the 2.5th and 97.5th
percentiles for the paired geometric-mean ratio. Statistical credibility
requires at least five paired blocks and no more than 5% coefficient of
variation for either acknowledged-goodput or p99 ratios. A degenerate
single-pair interval is retained as descriptive evidence but explicitly fails
that gate.

Claim evaluation applies the thresholds in `PERFORMANCE_CONTRACT.md` to valid
paired results:

- native parity requires at least 0.95 geometric-mean acknowledged-goodput
  ratio, with headline workloads at or above 0.90 and no broader primary
  workload below the contract's 0.85 exception floor;
- fixed-load p99 and CPU ratios must remain within 1.10;
- native leadership requires at least 1.05 geometric-mean goodput, no headline
  workload below 0.95, and a confidence interval that excludes parity; and
- Java claims include the packaged binding and its dispatch, GC, and native
  boundary costs.

Confidence intervals for goodput, latency, and CPU must stay on the acceptable
side of the relevant threshold or predeclared noise margin. A point estimate
alone never passes a claim gate. External wording is limited to the exact
workload family, versions, and machines tested.

## Result bundle

Each run writes a content-addressed immutable directory containing:

```text
summary.json
environment.json
workload.toml
adapter-config.json
latency.hdr             design target - not produced today
timeseries.jsonl        design target - not produced today
client-metrics.jsonl
broker-metrics.jsonl    design target - deferred, see docs/ROADMAP.md
verification.json
classification.json
stdout.log
stderr.log
checksums.txt
```

**This layout is the design target, not the current bundle.** What `benchctl`
seals today is:

```text
status.json                  execution-order.json    checksums.txt
experiment.source.toml       classification.json     bundle.json
experiment.resolved.json     comparison.json         verification/
subjects.lock.json           environment.json        adapters/<subject>/
topic-create.stdout.log      topic-cleanup.stdout.log
topic-create.stderr.log      topic-cleanup.stderr.log
```

`scripts/bench-m0-acceptance` asserts that list, and
[`docs/EVIDENCE.md`](EVIDENCE.md) explains what each document means and what a
reader may conclude from it. The differences from the target above are
deliberate rather than pending cleanup:

- `latency.hdr` and `timeseries.jsonl` do not exist. Distributions are carried
  inside the result document as `kafkars.log-linear.v1` histograms instead, so
  evidence memory stops scaling with run length and the Rust and C adapters can
  be asserted byte-equal. A separate per-record latency file would reintroduce
  exactly the run-sized artifact that encoding removed.
- `broker-metrics.jsonl` is deferred, not late. Collecting broker JMX against a
  laptop cluster that shares a CPU with the client under test would produce
  numbers that describe the laptop; the roadmap holds this until there is a
  stable runner to collect them on.
- `summary.json`, `workload.toml`, and `adapter-config.json` are the target's
  names for documents the Rust engine seals under different ones, and each
  subject's own output lives under `adapters/<subject>/` rather than at the
  bundle root, because a bundle carries more than one subject.

`environment.json` includes source commits, dirty-state rejection, compiler and
linker flags, feature sets, dependency and TLS/compression library versions,
broker and JVM versions, hardware topology, OS and kernel, CPU frequency
policy, memory, disks, NICs, and full broker configuration.

Raw result bundles are retained even when invalid. Reports select from immutable
bundles and identify every inclusion and exclusion reason.
Validated per-record latency and native-statistics streams may be losslessly
compressed before checksumming. Compression happens after semantic validation
and outside every measured client phase; aggregate summaries never substitute
for the compressed raw evidence.

## Matrix design and cadence

The existing producer inventory contains 1,620 combinations before
repetitions. At 30 seconds of warmup and 120 seconds of measurement, one serial
repetition takes 67.5 hours per client. It is not a nightly or ordinary release
suite.

The matrix is divided into:

- a predeclared 12- to 18-workload headline set;
- targeted one-axis and interaction sweeps;
- pairwise or fractional-factorial exploratory coverage; and
- full sweeps only where an interaction is expected.

Expected interaction sweeps include payload size by compression and
compressibility, partitions by callers and producer instances, linger by
offered rate, TLS by payload size, and queue capacity by overload behavior.

| Cadence | Contents |
| --- | --- |
| Pull request | Microbenchmark compile/smoke, loopback regression, schedule and payload conformance, verifier smoke; no noisy cross-client hard gate |
| Nightly | 12-18 headline workloads, capacity plus selected fixed-load points, three paired repetitions |
| Weekly | 50-100 targeted workloads, full capacity curves, five paired repetitions |
| Release candidate | Cold starts, fresh-broker races, faults, SASL/TLS, long soak, supported broker versions, and multiple hardware classes |

## First executable slice

The first implementation proves the measurement model before expanding the
matrix.

### Implemented diagnostic foothold

The repository now contains a deliberately non-claim-eligible first paired
runner under `legacy/benchctl/`, `scripts/bench-producer-compare`, and the
balanced `scripts/bench-producer-suite` wrapper. It implements:

- the public `kafkars` producer adapter, using `send_batch` for the measured
  phase as a sliding set of bounded batch futures and per-record `send` for
  warmup, and a raw librdkafka C adapter using `rd_kafka_produce_batch`, pinned
  to release `v2.15.0` and SHA-256
  `259015220cdca708afe838b5aa79ebf1a5fb710fb4179cf918d390aed85d5dbc`;
- independent process ownership, 64 MiB application/client queue budgets,
  bounded outstanding records, explicit balanced warmup and measured
  partitions, 600 fixed 100-millisecond replacement attempts capped by the
  original 60-second delivery deadline, one serialized acknowledged warmup
  record per partition before concurrent warmup traffic, `acks=all`,
  idempotence, no compression, five-millisecond linger, and matched batch
  settings;
- cross-language payload conformance vectors;
- matched public 256-record batch admission and
  batch-boundary-to-aggregate-terminal latency for every accepted result, plus
  acknowledged goodput;
- separate warmup and measured replicated topics;
- post-run independent raw-C Fetch verification of every identity and byte,
  duplicate/missing detection, partition selection, and per-partition order;
  and
- a checksummed evidence bundle with source, toolchain, host, order, raw
  samples, summaries, and explicit claim exclusions; and
- alternating client-first order plus sealed paired median, geometric mean,
  range, coefficient of variation, paired-block bootstrap confidence interval,
  and statistical-credibility verdict across a requested repetition count.

The diagnostic now satisfies configured native request-concurrency
equivalence. librdkafka sets
`max.in.flight.requests.per.connection=5`; kafkars sets the public
`max_in_flight_requests_per_broker=5` limit. Kafkars native metrics capture the
exact achieved total and per-broker peaks, and sealing fails if the per-broker
peak exceeds five. The raw librdkafka adapter enables the pinned library's
official statistics callback at 100 ms, captures callbacks without timed-path
file I/O into a fixed 32 MiB evidence buffer, and emits raw JSONL after the
measured phase. The sealer isolates one baseline and final cumulative snapshot,
aggregates every intervening reset-on-emit window, requires exact measured
record and partition totals plus a drained native queue, and retains Produce
request, batch, retry, timeout, sampled request-queue, internal-latency, output-
queue, RTT, and per-partition evidence. Cross-client achieved request and batch
shape is therefore classified; the librdkafka request-concurrency peak remains
explicitly sampled rather than inferred as exact.

The measured application API shape is matched. Kafkars `send_batch` and raw C
`rd_kafka_produce_batch` each offer at most 256 records per call into a sliding
window capped by the same record count. Both record completion only when every
accepted member of that application batch has a terminal result. This removes
the earlier per-record-versus-batch admission and latency-observation
exclusion; it does not yet classify achieved cross-client request and broker
batch distributions.

The implemented capacity point uses an 8,192-record application window. At
three brokers, five live requests per broker, four partitions per broker, and
64 KiB partition batches, 4,096 records provide only about one full network
wave. Two waves keep the bounded request lanes supplied while completions are
observed and replacement batches are admitted. The same window applies to both
adapters; kafkars uses that same value as its active-record capacity rather
than reserving unreachable active records behind the application window.
Because every comparison record carries an explicit partition, the metadata-
waiting partition retains only one 64 KiB batch of byte capacity. Its count
capacity holds one application window so terminal cells can remain owned
between publication and observer reclamation without increasing the 8,192
active-record or 64 MiB application-byte windows. Kafkars warms through the
same public batch API as its measured fixed-load phase and, like librdkafka,
admits the first record for each partition alone before filling the application
window. An explicit-partition run that enters metadata waiting exhausts the
single-batch byte bound and fails rather than silently gaining a second
application queue. Targeted sweeps may deliberately vary the application
window and must record the resolved value.

The implemented fixed-load diagnostic derives record `i` from the exact
integer-nanosecond schedule `floor(i * 1s / rate)`, groups at most 256 records
per public batch, and round-robins those batches across four callers. A bounded
admission turn linearizes the overlapping public calls in canonical batch
order while each caller independently observes completions and its own stripe
of the 8,192-record application budget. This prevents caller scheduling from
changing the defined per-partition order without turning completion progress
into a closed loop. The raw librdkafka main thread polls delivery and statistics
callbacks while submitters remain active; no submitter owns an unbounded queue.

Both adapters retain per-record intended, admitted, and terminal clocks, report
uncorrected, coordinated-omission-corrected, and schedule-delay percentiles,
and fail on admission pressure, terminal failure, incomplete drain, or native
evidence drift. The independent verifier requires every exact identity and
byte, expected partition, no duplicate or gap, and canonical per-partition
order. A balanced wrapper alternates client-first order across five paired
blocks and seals paired ratio summaries plus the same deterministic bootstrap
and 5% variation verdict as capacity mode.

The runner measures each whole adapter process, including its identical warmup,
with the host's external resource meter. It retains the raw output and
normalizes user-plus-system CPU core-seconds per million measured
acknowledgements, raw maximum RSS bytes, platform-calibrated peak memory, page
faults, and voluntary and involuntary context switches. Darwin uses the external
meter's peak physical-footprint field for peak-memory comparison and retains
maximum RSS separately; Linux uses GNU time's maximum RSS because that meter has
no physical-footprint field. Available Darwin instruction and cycle counters are
retained as diagnostics. This keeps measurement outside both client
implementations and makes unavailable or malformed primary evidence a sealing
failure. Whole-process measurements remain diagnostic until stable-runner
thresholds and broker/host saturation classification are calibrated.

The Rust adapter retains one compact nanosecond sample per record in its four
caller-owned segments. Percentiles use one temporary scalar vector at a time,
and raw CSV evidence is written by a constant-memory ordered merge. The adapter
does not duplicate every timing field or allocate a second all-record buffer
during caller merge; resource evidence therefore measures the client and the
shared workload rather than avoidable reporting storage.

The executable runner now seals raw librdkafka's sustainable-capacity curve,
including every probe and pass/fail reason, then derives and executes 25%, 50%,
75%, and 90% fixed-load paired suites without accepting hand-selected rates.
Each point alternates client-first order and independently applies the five-
repetition, 5%-variation, and bootstrap confidence gates. The aggregate
separates reference-capacity credibility, per-point credibility, latency
parity, and latency leadership. Smoke overrides remain visibly noncanonical.

Every current aggregate remains externally claim-ineligible until calibrated
stable-runner CPU/memory thresholds and broker/host classification exist, even
when its process-resource and latency evidence are statistically credible. The
implemented slice covers one balanced producer workload rather than the
complete headline matrix, so its ratios must not be generalized into a
product-wide parity or leadership claim.

### Workload

- 1 KiB structured payload with fixed seed;
- keyed and unkeyed variants;
- one producer, four callers, and explicit runtime-worker count;
- 12 partitions on three brokers;
- replication factor 3 and minimum ISR 2;
- `acks=all`, idempotence, plaintext, and no compression;
- balanced linger and batch settings;
- 64 MiB and 8,192-record outstanding budgets;
- capacity curve plus 25%, 50%, 75%, and 90% fixed-load points; and
- complete drain and verifier success.

### Adapters

1. `kafkars` public producer path.
2. Raw librdkafka C.
3. rust-rdkafka `BaseProducer` using the same librdkafka build.
4. rust-rdkafka `FutureProducer`.
5. Apache Kafka Java producer.
6. `kafka-client-ffi` after the batch path has a matched ownership contract.
7. The packaged Java binding when implemented.

Cross-client comparisons do not require or justify a new public
`kafkars` abstraction. Private microbenchmarks locate engine costs;
product adapters benchmark product truth.

## Repository shape

```text
kafka-benchmarks/
  crates/
    benchctl/               control-plane binary: resolve, supervise, seal
    bench-schema/           canonical documents, identities, and schema ids
    bench-adapter-librdkafka/  protocol shim in front of the raw C adapter
    bench-verifier/         verification report types
    bench-report/           summary statistics for sealed evidence
  adapters/                 standalone workspaces, not workspace members
    kafkars/
    librdkafka-c/
  legacy/
    benchctl/               migrated Node control plane, kept as a fixture
  scenarios/
    producer/               scenario TOMLs resolved into experiments
  conformance/
    payload/                committed payload vectors
    schedule/               committed schedule vectors
  schemas/                  <schema-id>.schema.json, one file per id
  clusters/
    dev-compose/            three-broker KRaft cluster for local runs
  dependencies/             pinned sibling repository revisions
  docs/                     this design, the performance contract, legacy notes
  scripts/                  check, build, and bench-* entrypoints
  .github/                  workflows and composite actions
```

Later adapters (rust-rdkafka `BaseProducer` and `FutureProducer`, the Apache
Kafka Java producer, the packaged Java binding) join `adapters/` as further
standalone workspaces speaking the same adapter protocol.

Benchmark-only dependencies remain outside the publishable product graph.
Introducing a benchmark workspace, new dependency edges, or files beyond
repository guardrails requires the deliberate policy changes and validation
already required by the repository contract.

## Open implementation decisions

- Stable-runner operating system, hardware classes, and resource-isolation
  mechanism.
- The versioned adapter control protocol and process supervision mechanism.
- Stable-runner broker, network, and host saturation classification thresholds.
- Allocation measurement tools for native and managed processes.
- The initial 12- to 18-workload headline manifest set.
- Result storage, signing, retention, and report publication location.
- The supported-broker and hardware matrix for release-candidate evidence.

These decisions must be made before the first result is treated as a gate. They
do not block implementing schedule and payload conformance, the verifier, the
result schema, or the first two producer adapters.

## References

- [Apache Kafka 4.3 producer configuration](https://kafka.apache.org/43/configuration/producer-configs/)
- [Apache Kafka 4.3 topic configuration](https://kafka.apache.org/43/configuration/topic-configs/)
- [Apache Kafka `ProducerPerformance`](https://github.com/apache/kafka/blob/trunk/tools/src/main/java/org/apache/kafka/tools/ProducerPerformance.java)
- [librdkafka statistics schema](https://github.com/confluentinc/librdkafka/blob/master/STATISTICS.md)
- [rust-rdkafka producer surfaces](https://github.com/fede1024/rust-rdkafka)
- [wrk2 coordinated-omission methodology](https://github.com/giltene/wrk2)
