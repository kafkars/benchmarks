# Roadmap

What this loop deliberately did not build, and why. Each item was considered,
scoped, and left out for a stated reason; none of them is an oversight, and
none of them is a promise with a date.

Workloads that cannot run are a different list, in
[`../scenarios/DEFERRED.md`](../scenarios/DEFERRED.md). This file is about the
harness.

## Protocol-aware loopback lab

A broker-free lane that speaks enough of the wire protocol to answer engine
questions without a cluster in the measurement. Deferred because it needs a
`kafka-wire` integration design first — which Produce and Fetch shapes it
serves, how it fails, and what it is allowed to be evidence *of*. A loopback
that is not designed against the protocol measures the loopback. It belongs in
the Layer 2 work, not in a scenario pack.

## kafkars-internal request and batch counters

The performance contract's required internal counters — buffer occupancy,
records and bytes per batch, size-triggered versus linger-triggered batches,
copies and allocations, requests submitted and retried, wakeups — are not
readable from a shipped public surface today. Getting them means changing
`kafka-client`, which is out of bounds for this repository: adapters depend
only on shipped public client surfaces, and a measurement that needs a private
hook belongs in the client's own repository. Until the client ships them, the
librdkafka side of every batch-shape comparison is exact and the kafkars side
is inferred, and the bundle says so.

## rust-rdkafka and Java adapters

`BaseProducer`, `FutureProducer`, and the Apache Kafka Java producer are the
next three subjects the design document names. Deferred because the adapter
protocol should be exercised by two independent implementations before it is
frozen against five, and because the Java subject brings JVM warmup, GC, and
safepoint evidence that the environment document does not model yet.

## Consumer and end-to-end packs

No consumer adapter exists, and the experiment vocabulary has one kind. A
consumer pack would be a manifest of scenarios nothing can run. The blocking
work is the consumer measurement document, not the pack.

## Broker JMX capture

Every run currently records what the load generator did and nothing about what
the brokers did, so a slow result cannot be classified as client-limited or
broker-limited from the bundle alone. Deferred because the design document
requires classification thresholds calibrated per stable runner, and this
repository has no stable runner — collecting broker metrics against a laptop
would produce a classification field nobody should trust.

## `benchctl pack` runner

`scenarios/packs/` declares which scenarios belong to which cadence; nothing
executes a pack. Deferred because a pack runner is where repetition ordering,
paired-block randomization, and cross-attempt aggregation land, and those are
the parts that must agree with the legacy control plane before the legacy plane
can be retired.

## `benchctl gc`

Nothing prunes `results/`. A day of nightly runs at the current record counts
is tens of gigabytes of latency and native-statistics evidence. Deferred
because retention is a policy question, not a command: a `gc` that decides for
itself which sealed bundles to delete is a harness that quietly deletes its own
bad news.

## Evidence-of-record storage policy

Where sealed bundles live, how long, who may read them, and which one a
published number cites. Deferred, and recorded as deferred in `AGENTS.md`,
because it must be decided before the first result is treated as a gate and it
does not block anything before that. Bundles are written to a gitignored
`results/` tree in the meantime, which is a location, not a policy.
