# Performance contract

Performance is a release gate, not a launch-day anecdote.

The executable workload, fairness, verification, statistical, and evidence
design is specified in
[`BENCHMARK_HARNESS_DESIGN.md`](BENCHMARK_HARNESS_DESIGN.md).

## Fair-comparison rule

Every competitor uses matched semantics:

- acknowledgments and idempotence;
- compression algorithm and level;
- linger and batch size;
- partition count and application concurrency;
- security mode;
- delivery deadline;
- payload/key distribution;
- serialization included or excluded consistently;
- identical broker, host, network, warmup, and measurement windows.

The Java comparison runs through the packaged Java API, including dispatcher and
native-boundary costs.

## Primary matrix

- Payloads: 128 B, 1 KiB, 16 KiB, 256 KiB, 1 MiB.
- Keys: keyed and unkeyed.
- Partitions: 1, 12, 96.
- Concurrency: 1, 4, 16 application threads.
- Modes: plaintext, TLS, compressed.
- Profiles: low latency, balanced, throughput.
- Paths: produce, consume, steady state, leader movement, reconnect, rebalance,
  completion backpressure.

## Provisional gates

| Claim | Gate |
|---|---|
| Native parity | Geometric-mean throughput at least 95% of librdkafka across the primary matched matrix |
| Per-workload floor | No primary workload below 85% without an accepted semantic tradeoff |
| Latency/CPU parity | Corresponding p99 latency and CPU within 10% |
| Native leadership | At least 105% geometric-mean throughput, no primary workload below 95% |
| Java leadership | At least 10% geometric-mean throughput improvement on balanced and throughput profiles |
| Java latency protection | Low-latency p99 no more than 5% worse, with improved CPU efficiency |

## Architectural performance constraints

- No mandatory per-record network request.
- No mandatory FFI crossing per record.
- No mandatory heap allocation per record after warmup on optimized paths.
- Sharded ingress; no single global producer mutex.
- Per-partition accumulators and contiguous encoded slabs.
- Compression outside the network reactor.
- Batched completion delivery and queue `poll_many`.
- Bounded buffer pools and generation-stamped metadata snapshots.

## Required internal counters

```text
records/bytes admitted and rejected
producer-buffer occupancy
completion-ledger occupancy
records and bytes per batch
size-triggered and linger-triggered batches
copies and allocations
requests submitted and retried
wakeups
queueing, batching, wire, and completion latency
```
