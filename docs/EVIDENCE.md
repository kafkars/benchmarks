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

- `base` is the subject a comparison divides by.
- `head` is the subject the comparison is about.
- `anchor` is a third subject held fixed across attempts, not compared
  against, that says whether the machine itself moved.

Unlike `command`, the role is *not* excluded from the id. Two experiments
that disagree about which subject is the baseline are asking different
questions even when every other field matches. Because the field is omitted
from the bytes when absent, every experiment id minted before roles existed
is unchanged.

An unknown role is refused by `ResolvedExperiment::validate` and by
`SubjectsFile::validate` rather than silently degraded to unlabeled. A
misspelled role is the same failure mode `deny_unknown_fields` exists to
prevent, one level down.

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
| `call_start` | When the application entered the client's admission call |
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
  present exactly when a schedule existed.

The clock is always `monotonic-ns`. Wall clocks name things; monotonic
clocks measure them.

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

The `valid` flag is the adapter's verdict on its own measurement, never the
validity verdict. Validity is decided from the verifier's read-back in
`kafkars.classification.v1`, because an adapter must never be the thing
that decides whether its own output was correct.

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
`LlmSummary::validate_against` enforces three things mechanically: every
`metric_refs` entry names a metric the packet defines, every
`evidence_refs` entry names an evidence pointer it carries, and the verdict
equals the packet's verdict. A model may hedge, elaborate, and speculate —
that is what `hypotheses`, labeled with a confidence, and `caveats` are for
— but it may not overrule the deterministic layer.

Rejecting a summary costs nothing. The packet is still there, and the
numbers in it never depended on the prose.

## Claim eligibility

`claim_eligible` is false in every document this milestone can produce, and
`validate` rejects a document that sets it true. Execution status answers
"did it run", validity answers "does it mean anything", and claim
eligibility answers "may we say so in public". The three are kept apart in
code, in documents, and in prose, and a checked gap named in
`deferred_checks` is the honest alternative to a footnote nobody opens.
