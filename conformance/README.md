# Conformance vectors

Committed goldens for the three cross-adapter contracts that every producer
benchmark depends on: the deterministic record payload, the canonical open-loop
admission schedule, and the `kafkars.log-linear.v1` histogram encoding.
`scripts/check-benchmarks` asserts **Rust == C == golden**, byte for byte, for
all three.

The payload and schedule vectors are documented below. The histogram vector has
its own directory and its own README, because what makes its input interesting
is a table of edge cases rather than a naming convention — see
[`histogram/README.md`](histogram/README.md).

## What these files are

Every `.vector` file is the **exact stdout bytes** of one adapter invocation —
nothing is normalized, pretty-printed, or trimmed, and the trailing newline is
part of the golden. Treat them as opaque: their value is that the bytes do not
move, not that a human can read them. Do not hand-edit a vector, and do not
"fix" one to make a check pass.

- `payload/seq-<sequence>.vector` — 1024 payload bytes plus a trailing newline
  (1025 bytes) for run id `0123456789abcdef` at the named sequence number. The
  sequences cover the boundaries that matter: `0`, `1`, `42`, and
  `18446744073709551615` (`u64::MAX`, which catches sign and wrap bugs).
- `schedule/<offered-rate>-<records>-<batch-records>-<callers>.vector` — the
  canonical batch admission schedule as CSV with the header
  `batch_index,caller,first_sequence,count,intended_ns`. The three cases cover a
  clean multiple, a ragged tail (`3-17-4-2`), and a rate high enough that
  `intended_ns` lands in the sub-microsecond range (`1000000000-513-256-4`).
- `histogram/mixed.input` and `histogram/mixed.vector` — the histogram encoding,
  documented in [`histogram/README.md`](histogram/README.md).

## Why the goldens exist

The Rust adapter and the C adapter already check each other, and that
comparison catches the failure where one implementation drifts. It cannot catch
the failure where **both** drift together — a shared refactor, a changed seed, a
retuned hash — because the two sides stay equal to each other the whole way
down while the benchmark quietly starts measuring a different workload. Results
recorded before such a change would no longer be comparable to results recorded
after it, and nothing in the run would say so.

The goldens are the third party to that comparison. They pin the bytes across
time, which turns "the adapters agree today" into "the adapters agree, and they
still produce what they produced when these vectors were sealed."

## Regenerating

A golden changes only when the observable contract changes. That is a behavior
change to review deliberately, never a routine refresh — a diff here
invalidates comparisons against every result recorded under the old bytes.

Build the adapters, then regenerate from the repository root:

```sh
binary_root=$(scripts/build-benchmark-adapters)
run_id=0123456789abcdef

for sequence in 0 1 42 18446744073709551615; do
  "$binary_root/kafkars-benchmark-adapter" payload "$run_id" "$sequence" 1024 \
    >"conformance/payload/seq-$sequence.vector"
done

for vector in "100000 1000 256 4" "3 17 4 2" "1000000000 513 256 4"; do
  read -r rate records batch_records callers <<<"$vector"
  "$binary_root/kafkars-benchmark-adapter" schedule \
    "$rate" "$records" "$batch_records" "$callers" \
    >"conformance/schedule/$rate-$records-$batch_records-$callers.vector"
done
```

The goldens are generated from the Rust adapter; `scripts/check-benchmarks`
then holds the C adapter to the same bytes, so a regeneration that the C side
does not agree with fails the check rather than silently blessing one
implementation. The equivalent C commands, for reference, are
`librdkafka-payload-vector <run-id> <sequence> 1024` and
`librdkafka-schedule-vector <offered-rate> <records> <batch-records> <callers>`.

After regenerating, run `scripts/check-benchmarks` and review the vector diff
in the same change as the code that moved it.
