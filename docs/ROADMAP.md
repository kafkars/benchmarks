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

## Narration beyond one packet and one provider

`scripts/benchmark-openai-summary` wires the narration layer to the OpenAI
Responses API, following the same house pattern the sibling repositories use: a
bash script around a stdlib-only python3 heredoc, the versioned prompt artifact
as the request text, strict structured output, and the exact request written to
disk before the call. It runs after a suite has sealed, reads only the analysis
packet, and produces markdown only for prose `benchctl packet` accepted.

Two things are deliberately not built on top of it. There is no second provider
and no local model, because the guardrail is what makes a summary evidence and
the guardrail is provider-agnostic — adding backends multiplies the transport
code without changing what may be believed. And there is no summary *across*
packets, for the reason the next entry states: a narrator handed six packets
would be asked to say what the night showed, which is a claim about a workload
mix nobody specified.

## Cross-pack aggregation

`benchctl pack` now exists, and runs every entry of a manifest through the verb
its repetition count implies. What it deliberately does not do is summarize
*across* entries: there is no document that reads six scenarios at once and says
what the night showed. Deferred because a cross-scenario statistic is a claim
about a workload mix nobody has specified, and inventing one in the runner would
put it beyond review. Repetition ordering and paired blocking already live in
`benchctl suite`, where a single experiment's repetitions are the only things
they can legitimately be computed over.

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

## Findings awaiting a client decision

The large-record scenarios surfaced two `kafka-client` behaviors the lab can
measure but not change: `batch_bytes` acts as a hard cap on the encoded wire
batch (records above it fail locally, without a broker), and one failure
terminal permanently fences producer admission. Both are recorded with
sealed evidence in `scenarios/DEFERRED.md`. A related harness follow-up:
after admission fencing the kafkars adapter exits without a result document;
it could instead seal a partial v2 measurement with the failure terminals
counted.
