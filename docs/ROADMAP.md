# Roadmap

What this loop deliberately did not build, and why. Each item was considered,
scoped, and left out for a stated reason; none of them is an oversight, and
none of them is a promise with a date.

Workloads that cannot run are a different list, in
[`../scenarios/DEFERRED.md`](../scenarios/DEFERRED.md). This file is about the
harness.

## The subject-revision model

The largest gap in the harness, and the one most likely to be mistaken for a
feature that already exists.

**What "base" and "head" mean today.** Two externally built binaries, named by
absolute path in a `subjects.toml` that `scripts/generate-subject-config`
writes, plus one shared sibling pin in
`dependencies/sibling-revisions.env`. The adapter is built once, against that
one pinned `kafka-client` revision, and both subjects in a comparison are
whatever binaries happen to be on disk. **Two client revisions therefore cannot
be compared in a single attempt.** A `head/base` pair today is two adapter
builds a human arranged, and nothing in the sealed bundle proves they differ in
the way the operator believes: `kafkars.subjects-lock.v1` records each binary's
digest, which detects a swap but cannot say which client revision produced it.

The interim pattern is a git worktree: check the two client revisions out side
by side, build the adapter against each, and name the two binaries as two
subjects. It works, it is what the acceptance scripts assume, and it is
entirely outside the harness — nothing validates that the worktree was clean,
nothing records which revision each binary came from, and nothing stops a stale
binary from being compared against a fresh one.

**What the model would be.** A `subjects/` directory of reviewed subject
definitions, each naming its component revisions — client, driver, protocol —
rather than a path. The control plane would check each definition out
content-keyed under `target/subjects/<hash>`, so the same revision set is built
once and reused, and two definitions that differ in any component get different
directories by construction. A dirty worktree would be rejected outright rather
than recorded and hoped about, because a subject whose source cannot be named
is a measurement of something nobody can rebuild. The build identity of each —
compiler, flags, profile, lock state, the fields
`kafkars.benchmark-environment.v2` now records for the control plane's own
build — would be captured per subject rather than once for the attempt.

Deferred because it is a checkout-and-build subsystem rather than a field, and
because the pieces it would rest on landed only recently: per-subject roles in
the experiment identity, the subjects lock, and the build-identity fields in the
environment document. Doing it before those existed would have meant inventing
them inside it.

## Cadences that have no workflow yet

`scenarios/packs/` declares three cadences and `.github/workflows/` implements
two of them. The PR pack runs in `ci.yml`'s non-gating `diagnostic` job and the
nightly pack in `nightly.yml`; the **weekly** and **release** cadences are
described in the packs and in `RELEASING.md` and have no workflow authored.

Deferred rather than stubbed, because a scheduled workflow that runs a pack
nobody has agreed the shape of produces evidence on a cadence nobody asked for —
and because both of these want a stable runner, which this repository does not
have. Running them on a shared hosted runner would produce weekly and release
numbers with exactly the properties that make the nightly's numbers
inadmissible, on artifacts that sound authoritative.

## Cross-repository trigger from a product pull request

The design's intent is that a pull request against `kafka-client` can ask this
lab for a comparison. **Parked, not deferred**: implementing it requires adding
a workflow to `kafka-client`, and this repository's standing constraint is that
it does not modify sibling repositories. The half that lives here — a manifest,
a `workflow_dispatch` entry point, and the PR pack — already exists; the half
that lives there has to be a decision made there.

## The remaining LLM summary flavors

`scripts/benchmark-openai-summary` narrates one analysis packet under one
committed prompt, `analysis/prompts/producer-comparison.v1.md`. Three more
flavors are named in the design and none is built:

- a **design comparison** summary, which reads a packet against the performance
  contract rather than against another subject;
- a **release report**, which would summarize a release's evidence set — and is
  blocked by the same thing the release cadence is;
- a **pull-request comment renderer**, which would turn a validated summary into
  the comment body a bot posts.

Each is a prompt artifact and a renderer, not new machinery: the guardrail that
makes a summary evidence is `benchctl packet`, and it is already
flavor-agnostic. They are deferred because a prompt is a reviewed document, and
three unreviewed ones would be three ways to say something nobody checked.

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
