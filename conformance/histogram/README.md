# Histogram conformance vector

The third contract every producer benchmark depends on, alongside the payload
and the schedule: the `kafkars.log-linear.v1` histogram encoding. Percentiles
are derived by readers from these buckets, so two adapters that bucket or
serialize differently produce latencies that cannot be compared even when both
are correct about the underlying durations.

## What these files are

- `mixed.input` — ASCII whitespace-separated `u64` values, exactly as both
  adapters read them from standard input. Not a random sample: it names the
  cases where two implementations diverge.
- `mixed.vector` — the **exact stdout bytes** of `histogram-vector` over that
  input: one line of compact JSON plus the trailing newline that is part of the
  golden. Treat it as opaque; do not hand-edit it, and do not "fix" it to make
  a check pass.

`scripts/check-benchmarks` asserts **Rust == C == golden**, byte for byte.

## What the input covers

| Values | What breaks without them |
| --- | --- |
| `0`–`7`, `126`, `127` | The exact-bucket region below `2^7`, where `index == value` |
| `128`, `129`, `254`, `255`, `256`, `257` | The first scale change, where a one-off in `bit_length` shows up |
| `511`–`513`, `1000`–`1029` | Dense mid-range values straddling several adjacent buckets |
| `0`, `128`, `255`, `1023`–`1025` repeated | Counts accumulate in one bucket instead of appending a second pair |
| `1099511627776` (`2^40`) and above | Scales an implementation using 32-bit intermediates cannot reach |
| `9223372036854775808` (`2^63`) | Sign errors in a language whose shifts are signed by default |
| `18446744073709551615` (`u64::MAX`), twice | The top bucket, and a `sum` that must **saturate** rather than wrap |

The saturating sum is the one that most often differs: C has no saturating add
of its own, so a straight `+=` silently wraps and produces a plausible-looking
small number. The golden pins `sum` at `u64::MAX`.

## Regenerating

A golden changes only when the observable contract changes — a new bucket
layout, a new field, a new field order. That is a schema change with a new
layout id, never a routine refresh: a diff here invalidates every latency
comparison recorded under the old bytes.

Build the adapters, then regenerate from the repository root:

```sh
binary_root=$(scripts/build-benchmark-adapters)
"$binary_root/kafkars-benchmark-adapter" histogram-vector \
  <conformance/histogram/mixed.input \
  >conformance/histogram/mixed.vector
```

The golden is generated from the Rust adapter, because compact serde output of
`bench_schema::EncodedHistogram` is the reference encoding.
`scripts/check-benchmarks` then holds `librdkafka-histogram-vector` to the same
bytes, so a regeneration the C side does not agree with fails the check rather
than blessing one implementation.

After regenerating, run `scripts/check-benchmarks` and review the vector diff in
the same change as the code that moved it.
