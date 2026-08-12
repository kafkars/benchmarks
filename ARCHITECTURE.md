# Architecture

The product of this repository is a sealed evidence bundle. Everything else —
the control plane, the adapters, the verifier, the reporting — exists to make
one bundle trustworthy enough that somebody who was not in the room can read it
and reach their own conclusion.

```txt
scenario TOML + profile + cluster + seed
              │
              ▼
        benchctl (control plane)
              │
   run ───────┤  resolve → probe → capture → spawn → supervise → verify → seal
   suite ─────┤  the same attempt, repeated in paired blocks with alternating
              │  subject order; every repetition seals on its own
   capacity ──┤  probe → judge against the SLO → raise or refine the rate →
              │  repeat; every probe seals on its own
              │
      ┌───────┴────────┬──────────────────┬─────────────────┐
      ▼                ▼                  ▼                 ▼
 adapter A        adapter B          topic tool         verifier
 (kafkars)        (librdkafka)       (configured)       (configured)
      │                │                                    │
      └── result.json (producer-benchmark.v2), status.json ──┐
                                            ▼               ▼
                              results/<experiment-id>/<attempt-id>/
                                     sealed, checksummed, immutable
                                            │
                                            ▼
                                report ── one sealed bundle, rendered
                                packet ── suite summary → analysis-packet.v1
                                            │
                                            ▼
                                          prose
```

`run` is one attempt. `suite` and `capacity` are loops over attempts that
decide *which* attempt to run next — a repetition count, or the next candidate
rate — and nothing else: neither touches the measured path, and neither can
produce a bundle that `run` could not have produced on its own. `report` and
`packet` are downstream of sealing and reach no broker.

## The four roles

**The control plane** (`crates/benchctl`) owns everything a subject must not:
experiment identity, topic naming, execution order, deadlines, environment
capture, verification, and sealing. It spawns processes and reads files. It
never links a Kafka client, and it never asks a subject whether the subject did
well.

**Adapters** own the measured path and nothing else. An adapter is a separate
executable speaking three verbs — `describe --json`, `validate --experiment`,
`run --experiment --output` — and depending only on shipped public surfaces of
the client it wraps. Two adapters ship today: a native one built against the
Rust client, and a shim that drives the unmodified librdkafka C benchmark
binary. Adapters are not workspace members; each is built against its own
pinned dependency graph, because a subject resolved by this workspace is a
subject this workspace has modified.

**The verifier** is deliberately outside the adapter protocol. It reads the
topic back to the end of every partition and reports what the broker actually
retained: verified, duplicated, missing, corrupt, out-of-order, and how many
partitions reached end-of-partition. Cross-client validity is decided from that
report. An adapter cannot vouch for itself, and the same verifier judges every
subject, so no subject benefits from a friendlier reader.

**The evidence layer** (`crates/bench-schema`, `crates/bench-verifier`,
`crates/bench-report`) is pure. It defines the documents, the canonical bytes
they hash to, and the statistics computed over them. It never reaches a broker,
spawns a process, or reads a clock, so a reporting bug can never move a
measurement.

## The evidence path

A number moves through four documents on its way from a record to a sentence,
and each boundary is narrower than the one before it.

**The adapter writes `kafkars.producer-benchmark.v2`.** Every offer carries one
immutable identity and four instants — intended, call start, accepted, terminal
— and none of them is reset because a queue was full. Time spent being pushed
back on is therefore inside every reported latency, and an offer that never
crossed the client API is counted as offered but not accepted rather than
dropped. The accounting is checked, not asserted: accepted equals acknowledged
plus failed plus timed out plus unknown, and each histogram's total must equal
the outcome count it claims to describe.

**Distributions are histograms, not arrays.** `kafkars.log-linear.v1` is a
bounded log-linear encoding — 128 linear sub-buckets per power of two, a sparse
ascending index/count list, exact `min`, `max`, and `sum` outside the buckets.
Evidence memory stops scaling with run length, and the encoding is specified
byte-for-byte so the Rust and C adapters can be asserted equal rather than
close. Percentiles are derived by the reader from a bucket's inclusive upper
bound, so a derived latency errs conservative.

**The control plane seals a bundle**, which is where the measurement stops
changing. Nothing rewrites it, including to fix it.

**`report` produces `kafkars.analysis-packet.v1`**, the boundary between
measurement and interpretation. Every quantity lives under a stable key with its
own name and unit; findings cite keys rather than restating values; the verdict
is computed from the intervals and gates and no downstream prose may contradict
it. A summary that says *improved* about a packet that says *inconclusive* is
caught mechanically rather than by a reader noticing.

## Two identities

A bundle carries two hashes, and confusing them is the most expensive mistake
available here.

- **`experiment_id`** hashes the canonical resolved experiment — the *intent*.
  It deliberately excludes the runtime binding and each subject's command path,
  so the same experiment run tomorrow, against a different bootstrap, with the
  binaries at different paths, is recognizably the same experiment.
- **`bundle_digest`** hashes the sealed evidence — the *result*. It is the
  digest of `checksums.txt`, which in turn digests every other file in the
  bundle. This two-tier arrangement is what resolves the circularity of a
  manifest that would otherwise have to contain its own hash.

Identity documents contain integers only. A float that round-trips differently
on another machine would make the same intent hash differently, so rates,
ratios, and latencies live in evidence documents that are never hashed into an
identity.

## Always seal

An attempt that started leaves a bundle. That is the invariant the control plane
is built around, and it is layered three deep: every phase result funnels into
exactly one seal call; a panic is caught and sealed as a crash; and a
last-resort drop guard makes a best-effort seal if both of those are somehow
bypassed. A subject that fails records its failure and the *other* subjects
still run, because "A crashed and B did not" is evidence.

`execution_status` is one of `complete`, `partial`, `crashed`, or `timed_out`,
and the worst outcome wins. Validity is a separate axis in
`classification.json`: a run can be complete and invalid, or partial and still
informative about the part that ran.

## Repository layout on disk

The Kafka family repositories are peers, and several checks assume it:

```txt
~/code/
├── kafka-protocol      generated wire types
├── kafka-driver        RPC and I/O
├── kafka-client        the client under measurement
└── kafka-benchmarks    this repository
```

The native adapter resolves the client through a relative path dependency into
that sibling tree, and `dependencies/sibling-revisions.env` pins the exact
revision of all three siblings that a run claims to have measured. Provenance
checking is strict in CI and advisory locally, because a developer's checkout is
routinely dirty and a bundle sealed from a dirty tree is diagnostic rather than
claimable — which the bundle itself records.

## The legacy plane

`legacy/benchctl/` is the Node control plane this repository was extracted from,
kept close to verbatim. It is not dead weight: it is the behavioral reference
for the parts the Rust engine has not yet reimplemented — bootstrap confidence
intervals, suite orchestration, native-metric gates, and librdkafka statistics
summaries. Where both exist, they must agree; where only the legacy plane
exists, the Rust classification records the check as deferred rather than
pretending it passed.
