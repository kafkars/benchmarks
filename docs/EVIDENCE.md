# Evidence contract

A benchmark result is worth exactly as much as a reader's ability to say
what it measured. This document is the contract that ability rests on: the
offer model every latency comes from, the histogram every distribution is
carried in, the documents that aggregate and interpret them, and the two
identities that say which runs are of the same thing.

The serde types in `crates/bench-schema/src/` are the source of truth for
every shape described here. The JSON Schema files in `schemas/` are
documentation mirrors: they exist so a reader outside this workspace can
validate a sealed bundle without a Rust toolchain, and when the two
disagree the Rust type is right and the mirror is a bug.

Schemas are versioned and append-only. A field may be added under an
existing id; what an existing field *means* may never change under that id,
because sealed bundles are immutable and a redefinition retroactively
falsifies runs that already happened.

## The two identities

A bundle carries two hashes, and confusing them is the most expensive
mistake available here.

**The experiment id** hashes *intent*. It is the sha-256 of the canonical
bytes of `kafkars.experiment.v1` with exactly two kinds of key removed: the
top-level `runtime` binding, and every subject's `command`. A run against a
different bootstrap, with different topic names and a different run id, is
the same experiment executed again — that is the whole point of having an
id to aggregate repetitions by. A subject invoked through an absolute
rather than a relative path is the same subject; what the binary actually
was is recorded in `kafkars.subjects-lock.v1`, with its digest.

**The bundle digest** hashes *evidence*. It is the digest of
`checksums.txt`, which in turn covers every other file in the sealed
bundle. It detects tampering and identifies one attempt. It cannot
aggregate, and the experiment id cannot detect tampering; neither can do
the other's job.

Everything hashed into an identity is an integer. Rates, byte counts, and
record counts are exact; ratios and latencies are measurement, and
measurement lives in documents that are never hashed into an identity.
`canonical_bytes` refuses a float outright, so the rule is enforced rather
than remembered.

### Subject roles are identity-relevant

`SubjectSpec.role` is the one recent addition to the identity surface. It
takes `base`, `head`, or `anchor`, and it is absent by default:

- `base` is the subject a comparison divides by. `benchctl suite` alternates
  which subject runs first across repetitions, so the denominator is chosen by
  role rather than by execution order — otherwise the same suite would seal
  `head/base` in one repetition and `base/head` in the next, and a reader
  comparing two `comparison.json` files would see the ratio invert for no
  reason the documents explain.
- `head` is the subject the comparison is about.
- `anchor` is a third subject held fixed across attempts that says whether the
  machine itself moved. The suite **does** emit `head/anchor` pairs and reports
  them beside `head/base`; what the anchor is not is a *gate target* — no gate
  is written against it, because a machine-drift reference that could fail a
  comparison would be a second baseline. It is compared, read, and never
  divided by: when a `base` exists, the attempt-level comparison never uses the
  anchor as its denominator.

Unlike `command`, the role is *not* excluded from the id. Two experiments
that disagree about which subject is the baseline are asking different
questions even when every other field matches. Because the field is omitted
from the bytes when absent, every experiment id minted before roles existed
is unchanged.

An unknown role is refused by `ResolvedExperiment::validate` and by
`SubjectsFile::validate` rather than silently degraded to unlabeled. A
misspelled role is the same failure mode `deny_unknown_fields` exists to
prevent, one level down.

## What is in a sealed bundle

One row per file a sealed attempt writes. Read this before the sections
below: they describe the shapes, and this says which file carries which
shape and who is entitled to have written it.

| File | What it is | Written by | Schema id |
|---|---|---|---|
| `status.json` | How far the machinery got, phase by phase, and how each subject's process ended. Not validity. | control plane | `kafkars.run-status.v1` |
| `classification.json` | Whether the evidence may be believed, with a named reason per failed gate and the checks this attempt deliberately did not perform. | control plane | `kafkars.classification.v1` |
| `comparison.json` | Attempt-level ratios between subjects, dividing by the declared `base`. | control plane | `kafkars.comparison.v1` |
| `execution-order.json` | Which subject ran in which position, and what decided that. | control plane | `kafkars.execution-order.v1` |
| `experiment.source.toml` | The scenario as authored, byte for byte as it was handed in. | copied from the caller's input | — (source TOML) |
| `experiment.resolved.json` | What was actually run, with every default made explicit. The document the experiment id is hashed over. | control plane resolver | `kafkars.experiment.v1` |
| `subjects.lock.json` | What each subject's binary actually was at probe time, with its digest and the version it declared. | control plane probe | `kafkars.subjects-lock.v1` |
| `environment.json` | Repository states, toolchain and build identity, host facts, broker identity. | control plane | `kafkars.benchmark-environment.v2` |
| `adapters/<subject>/result.json` | The measurement: four timestamps, bounded histograms, offer accounting, declared execution. | the adapter under test | `kafkars.producer-benchmark.v2` |
| `adapters/<subject>/kafkars-native-metrics.json` | Public Kafkars producer snapshots bracketing measurement and their exact cumulative-counter deltas. Present for the Kafkars adapter. | the adapter under test | `kafkars.kafkars-native-metrics.v1` |
| `adapters/<subject>/status.json` | The adapter's own report of how its run verb ended. | the adapter under test | `kafkars.adapter-status.v1` |
| `adapters/<subject>/stdout.log`, `stderr.log` | The subject's captured output, truncated at the declared budget. | the adapter under test | — (text) |
| `verification/<subject>-<phase>.json` | What the broker-visible verifier read back off the topic. Never written by a subject. | the configured verifier tool | `kafkars.producer-verification.v1` |
| `checksums.txt` | A digest per file in the bundle; the checksum walk refuses symlinks and other non-regular files outright. | seal | — (text, `sha256  path`) |
| `bundle.json` | The bundle digest, which is the digest of `checksums.txt`. | seal | `kafkars.bundle.v1` |
| `seal-failure.txt` | Present only when sealing itself failed partway; names the failure beside whatever terminal files could be completed. | seal recovery | — (text) |

Two separations in that table are the whole design. An adapter writes only
its own directory — it never writes a classification, a comparison, or a
verification, because a subject that graded its own output would not be
evidence. And the verifier is a configured tool invoked by the control
plane rather than an adapter verb, for the same reason one level up.

A suite or a capacity search writes its own documents *outside* the bundle,
under the reports tree: `suite-summary.json`, `report.md`, `report.html`,
`analysis-packet.json`, and — when a run was narrated — `llm-summary.json`
with its `llm-summary-request.json` and `llm-provenance.json` beside it.
Those are derived from sealed bundles and never part of one, because a
bundle is immutable and an aggregate over several of them is not a fact
about any single attempt.

The Kafkars native sidecar lets a reader conclude how many public
driver-accepted Produce requests, partition batches, records, and encoded
record bytes accumulated between the post-warmup baseline and post-drain final
snapshot. It also preserves producer ownership gauges and lifecycle flags at
both boundaries. Its peaks are process-lifetime values and may include warmup;
the sidecar does not claim continuous gauge maxima, application payload bytes,
complete request wire bytes, retries, timeouts, allocations, or wakeups.

## The offer model

`kafkars.producer-benchmark.v2` is the measurement document the adapter
protocol's `run` verb writes. It exists because v1 could not be fixed under
its own id: v1 restarted an offer's clock when the client refused
admission, so a client that spent longer refusing work reported lower
latency for doing so.

### Four timestamps, never reset

Every offer owns one immutable identity and four monotonic timestamps:

| Timestamp | Meaning |
|---|---|
| `intended` | When the schedule said this record should be offered |
| `call_start` | When the application began admitting this offer: after its own budget wait, before the client's call |
| `accepted` | When the client took ownership of the bytes |
| `terminal` | When the record reached acknowledgement, failure, or timeout |

None of the four is ever reset. A retry after a full queue is a retry of
the *same* offer, keeping its original `intended` and `call_start`, so
backpressure time is inside every latency the document reports. The four
timestamps give the four distributions:

- `intended_to_terminal` — `terminal - intended`. This is the number a
  comparison uses. It cannot be improved by refusing work.
- `accepted_to_terminal` — `terminal - accepted`. The client-internal
  portion, useful for locating a regression, useless for claiming one.
- `call_start_to_accepted` — `accepted - call_start`. Admission wait,
  including every retry of the same offer.
- `intended_to_call_start` — `call_start - intended`. Scheduler lateness,
  present exactly when a schedule existed. It carries the application's own
  outstanding-budget wait, because that is what a schedule slipping under
  backpressure looks like.

The clock is always `monotonic-ns`. Wall clocks name things; monotonic
clocks measure them.

### Where `call_start` is taken

Two of the four distributions meet at `call_start`, so where it is stamped
decides which of them a wait lands in. The point is therefore normative
rather than incidental, and it is this:

> `call_start` is taken *after* the application's own outstanding-budget
> wait and *before* anything that is part of the admission itself.

**Excluded: the harness's own backpressure.** Both adapters cap how many
offers they will let the client own at once, and a caller that has reached
its cap waits for a completion before offering again. That wait is the
harness's policy rather than the client's behaviour, and folding it into the
admission wait would report a client as slow for obeying a bound the harness
chose. It is not thereby hidden. Under `scheduled-open-loop-fixed-rate` it
delays the public call and so appears in full in `intended_to_call_start`,
which is where a reader watches the schedule slip. Under `closed-loop` there
is no schedule to be late against, so the wait is deliberately invisible in
every latency and visible only in throughput: a closed-loop caller that
spends longer waiting for its own budget offers fewer records per second and
reports the same per-offer latency, which is what a closed-loop measurement
means.

**Included: everything the offer then waits through.** The harness's
submission-order serialization — the fixed-rate callers take turns so that
the public call sequence is the schedule's own rather than the operating
system's — then the client's admission call, then every queue-full retry of
the same offer. An offer refused nine times and taken on the tenth carries
all ten attempts in one `call_start_to_accepted` sample, because they are
attempts at one offer whose identity never changed.

Both adapters also do the offer's own bookkeeping inside this bracket —
counting it as offered, and under a schedule recording its lateness samples —
rather than after the client has answered. That is what lets `offered` count
public calls that began, so that a refused batch reports `offered` above
`accepted` instead of agreeing with it by construction; and doing it on both
sides means the few microseconds it costs are common to both rather than a
difference between them.

Both shipped adapters implement this bracket at one place each: `kafkars` in
`OfferEngine::admit`, where `AdmissionClock::start` runs after the caller's
budget loop and before `AdmissionOrder::wait`; and `librdkafka-c` in
`await_budget_then_stamp`, which performs the budget wait and the stamp in
that order in one function so the two cannot drift apart. **A future adapter
must take its stamp at the same point.** Two adapters that bracket
differently produce `call_start_to_accepted` values that are not comparable
and `intended_to_call_start` values that are not comparable either, while
both documents still satisfy every invariant below — which would put the
difference in exactly the place a reader cannot see it.

### The measured interval

`throughput.measured_duration_ns` is the phase's own wall clock — from the
schedule epoch, or from the first offer under closed loop, to the end of the
drain — read from the same monotonic clock as the four timestamps. It is not
the last terminal that happened to arrive: a phase whose offers all ended
`unknown` has no last terminal at all, and calling that interval zero divides
a goodput by nothing. Both adapters measure this interval the same way, so
the `acknowledged_records_per_second` of two subjects are over spans that can
be compared.

### Accounting invariants

`ProducerBenchmarkV2::validate` enforces the following, and names the first
one that fails:

- `offered >= accepted`.
- `accepted == acknowledged + failed + timed_out + unknown`.
- `call_start_to_accepted.total == accepted`.
- `intended_to_terminal.total == accepted_to_terminal.total ==
  acknowledged + failed + timed_out`. Unknown offers have no terminal, so
  they contribute to no terminal distribution.
- `intended_to_call_start` is present if and only if the load mode is
  `scheduled-open-loop-fixed-rate`, with `total == offered`. A closed-loop
  run has no schedule to be late against, and a scheduled run that lost its
  lateness distribution has lost the evidence that the schedule was kept.
- A measurement declared invalid states why.

The `valid` flag is the adapter's verdict on its own measurement, and it is
one input to validity, never the whole of it. `kafkars.classification.v1`
combines several independent gates: the adapter's self-verdict (an adapter
that disowns its own measurement is believed), the broker-visible
verifier's read-back, complete drain (`unknown == 0` and
`final_outstanding == 0`), the failure ceiling, the adapter process's own
exit, and the provenance cross-check between the declared and reported
adapter versions. A run is valid only when every gate passes, and each
failed gate contributes a named reason — so `run_valid: false` always says
which gate, not just that one existed. What an adapter can never do is
declare a measurement valid on behalf of the harness: a self-declared
`valid: true` passes exactly one of the gates.

Two sealing details a bundle reader should know: the checksum walk refuses
symlinks and other non-regular files outright (a bundle entry the manifest
cannot vouch for is a seal error, not a silent omission), and a bundle
whose sealing failed partway carries a `seal-failure.txt` naming the
failure next to whatever terminal files could still be completed — the
richer `status.json` written before the failure is preserved, never
overwritten by the recovery path.

### What the queue retained

`queue.max_outstanding_observed` is the largest number of offers the client
owned at once, and `queue.max_outstanding_bytes_observed` is what those offers
weighed. Both adapters accumulate the byte figure at the same admit and
terminal sites the record count is updated at, rather than multiplying the
record high-water by the payload size afterwards. Under today's payload
contract every record in a run is one fixed size, so the two agree — but the
moment a variable-size payload profile exists the multiplication silently
becomes wrong while an accumulated figure stays right, and a number that is
correct only because of a property nobody wrote down is a number waiting to
lie. The field is absent, never zero, for an adapter that does not track it.

### Declared execution vocabulary

`declared.payload_construction` is `prebuilt-pool-per-offer-sequence` in
both shipped adapters, and the string means exactly this: payload template
bytes are built before the measured interval from a bounded pool, and the
only per-offer byte work is stamping the offer's sequence number so the
broker-visible verifier can check every record individually. The two
adapters differ in *ownership* — the kafkars adapter builds an owned buffer
per offer from the pool (`owned-per-offer-from-pool`, the public API takes
ownership), the librdkafka adapter reuses pooled buffers and asks the
client to copy (`copy-in-reused-buffer`) — and that difference is declared
where it belongs, in `declared.ownership`, not hidden inside the
construction string. A reader comparing measurements should require equal
`payload_construction` and treat unequal `ownership` as a product-surface
difference to report, not to erase.

That obligation is a gate rather than advice.
`kafkars.suite-summary.v1` carries **`matched-execution-surface`**, which fails
when the two sides of any compared pair declare a different
`payload_construction` or a different `serialization`, and names both values
when it does. Those two decide what work is being timed at all: a subject that
builds its payload inside the measured interval, or that serializes there while
the other does not, is not slower at producing — it is measuring more, and a
ratio across that difference is not a weak comparison, it is not a comparison.
The gate also fails when a compared pair carries no declaration to check,
because "we did not check" is not "it matched".

Unequal `ownership` and unequal `completion_mode` do **not** fail it. Those are
real product-surface differences that a reader must be told about and must not
have erased: the kafkars adapter hands the client an owned buffer because that
is the public API it ships, and the librdkafka adapter asks for a copy because
that is the public API *it* ships. Refusing to compare them would refuse to
compare the two clients as they actually exist. Each such difference becomes a
named note in the summary, carried into every rendering, and the numbers stay.

When the gate fails, the analysis packet carries it as a deterministic
*finding* rather than only as an anomaly, because it changes what every other
finding in that packet means.

## The histogram

Every distribution above is an embedded `kafkars.log-linear.v1` histogram.
Two constraints ruled out an off-the-shelf dependency: evidence memory must
not scale with run length, and two languages must produce byte-identical
encodings so cross-language conformance can assert equality instead of
tolerance.

### Layout

With `SUB_BUCKET_BITS = 7` — 128 linear sub-buckets per power of two:

- a value below 128 gets its own exact bucket, `index = value`;
- for a larger value, `k = bit_length(value) - 1 - 7` halvings bring it into
  `[128, 256)`, and
  `index = (k + 1) << 7 | ((value >> k) - 128)`.

Every bucket at scale `k` spans `2^k` values, so the worst-case relative
error is `2^-7`, or 0.78%. The buckets tile the number line with no gaps
and no overlaps, which is a property the tests assert rather than assume.

### Reading a percentile

A percentile reports the inclusive **upper** bound of the bucket holding
that rank — the highest value the recorded sample could have had — clamped
to the exact recorded maximum. Derived latencies therefore err
conservative, never flattering. `min`, `max`, and `sum` are tracked exactly
outside the buckets, so the extremes and the mean are not estimates.

Readers derive percentiles. The writer never computes one: a percentile
computed at write time is a number nobody can recheck against the
distribution it came from.

### Serialized form

The wire form is an embedded object, not a standalone document, with fields
in the fixed order `layout`, `unit`, `sub_bucket_bits`, `total`, `min`,
`max`, `sum`, `counts`. `counts` is sparse, `[[index, count], …]`, strictly
ascending by index, with no zero counts. Compact serde output of
`EncodedHistogram` is the byte-exact reference encoding the C
implementation must reproduce, and it is pinned by a golden test.

`EncodedHistogram::validate` checks the layout string, the unit, the bit
width, strict ascent, the absence of zero counts, that the bucket counts
sum to `total`, and that `min`/`max` presence matches emptiness.

## Aggregation and analysis

Four documents sit above the per-attempt measurement, and each one narrows
what a reader is allowed to conclude.

### `kafkars.suite-summary.v1`

Repetitions of one experiment id, summarized. Medians rather than means, so
one stalled attempt does not move the reported number. Paired ratios that
name their own denominator, with bootstrap intervals whose resample count
and seed are both recorded. Dispersion beside every central value, absent
rather than zero when it could not be computed. Gates that carry the
sentence they enforce, so a failing gate is readable without the code that
ran it. `practical_threshold` records the difference the suite was set up
to care about, so that "the interval excludes zero" is never confused with
"the difference matters".

Every attempt stays in the document, valid or not, with its bundle digest.
A median over a set nobody can reconstruct is not auditable.

**Comparison and attribution are separate.** Six of the observation fields can
support a claim — goodput, the three offer-to-terminal percentiles, the
admission wait, and the two resource figures. Three cannot, and are carried
anyway because locating a difference is exactly what they are good for:

| field | what it says | why it cannot claim |
|---|---|---|
| `declared` | what was in the measured path | it is a description, not a measurement |
| `p99_intended_to_call_start_ns` | scheduler lateness, `call_start - intended` | present only under a schedule; **absent, never zero, for a closed-loop run**, because no schedule and a schedule kept perfectly are opposite statements |
| `p99_accepted_to_terminal_ns` | the client-internal portion, `terminal - accepted` | a client that refuses admission for longer looks better here by construction, which is the exact substitution v2 was minted to prevent |

The two percentiles are paired, rendered, and citable from the analysis packet.
No gate is written over either of them, and neither contributes to the packet's
verdict. Compare on `p99_intended_to_terminal_ns`; read these beside it.

`cpu_core_seconds_per_million_acknowledged` is beside `cpu_core_seconds` for a
related reason: total CPU is not comparable between subjects that moved
different amounts of traffic, and reading the raw totals side by side rewards
the subject that did less work. It is absent, never zero, when the platform
reported no resources or when nothing was acknowledged to divide by. Nothing
gates on it either.

**Too few repetitions is inconclusive, and says which way it leaned.** A suite
below the five valid paired attempts `MINIMUM_PAIRED_REPETITIONS` requires has intervals, and those
intervals can sit entirely past the practical threshold — but a bootstrap over
two paired blocks is an interval over two numbers. The packet's verdict is
therefore `inconclusive`, and the directional read is not thrown away: it moves
into a deterministic finding that names the direction and the repetition count
in one sentence, so a reader gets the signal without the document asserting it.

### `kafkars.capacity-search.v1`

The rate ladder a search walked for one subject, the objectives it judged
by, and where it stopped. Every probe stays in the document with the
reasons it failed. Confirmation probes are separate from the ladder: they
are repetitions at the settled rate, run after the bracket closed, and they
are what turns "the search stopped here" into "this rate holds".
`confirmed_rate` is present if and only if the status is `converged`; an
unconverged search that carried a rate would be stating a capacity nobody
established.

### `kafkars.analysis-packet.v1`

The boundary between measurement and interpretation. Every quantity a
reader may cite lives in `metrics` under a short stable key — `M001`,
`M002` — carrying its own name and unit, and findings cite keys instead of
restating values. `evidence_refs` does the same for provenance: a key maps
to a bundle-relative path or a bundle digest, so a claim can point at the
file it came from without embedding a path in prose.

The `verdict` — `improved`, `regressed`, `mixed`, `inconclusive`,
`invalid` — is computed here, deterministically, from the suite's intervals
and gates. `validate` refuses a finding that cites a metric the packet does
not define.

### `kafkars.llm-summary.v1`

Prose written over one packet, and bound to it.
`LlmSummary::validate_against` enforces four things mechanically: every
`metric_refs` entry names a metric the packet defines, every
`evidence_refs` entry names an evidence pointer it carries, **every finding
cites at least one metric**, and the verdict equals the packet's verdict. A
model may hedge, elaborate, and speculate — that is what `hypotheses`,
labeled with a confidence, and `caveats` are for — but it may not overrule
the deterministic layer.

The citation rule is about the findings list specifically. An entry there is
asserted as fact, reads exactly like a cited one, and carries the same
authority; an uncited one rests on nothing a reader can check. A statement
the model cannot attach to a number is a hypothesis, and `hypotheses` is
deliberately not required to cite. Stating no findings at all is fine.

`provenance` is optional and carries how the summary was produced: the
model, the prompt version, the reasoning effort, the response id, the
digests of the packet in and the summary out, and when it was written.
`scripts/benchmark-openai-summary` stamps it and also writes the same facts
beside the document as `llm-provenance.json` — the sidecar because the
surrounding tooling reads it and a *rejected* summary still leaves one, the
embed because a summary that has been moved arrives without its sidecar and
a reader then cannot ask what wrote it. `output_sha256` covers the model's
own bytes with `provenance` removed, because a digest cannot cover the field
that carries it. The strict output schema the script sends does not offer
the model a `provenance` field: a model that could write its own provenance
could write anything.

Rejecting a summary costs nothing. The packet is still there, and the
numbers in it never depended on the prose.

## Claim eligibility

`claim_eligible` is false in every document this milestone can produce, and
`validate` rejects a document that sets it true. Execution status answers
"did it run", validity answers "does it mean anything", and claim
eligibility answers "may we say so in public". The three are kept apart in
code, in documents, and in prose, and a checked gap named in
`deferred_checks` is the honest alternative to a footnote nobody opens.
