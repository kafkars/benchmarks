# producer-comparison suite

> Diagnostic only — never claim-eligible. These numbers describe the machine and configuration they were measured on, and support no published comparison between clients.

- experiment: `74283dc35de37b3a6e2714c29a6c7fd176866332afdf2c5c8ccef2388dc698dd`
- attempts: 5 requested, 5 valid
- practical threshold: 0.0500
- bootstrap: 1000 resamples, seed `20240601`
- claim eligible: `false`

## Scorecard

Medians over the valid attempts. Latency is offer-to-terminal, so it includes admission wait.

| Subject | Role | Goodput (records/s) | p50 (ms) | p99 (ms) | p99.9 (ms) | Admission p99 (ms) | CPU (core-s) | Peak RSS (MiB) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| librdkafka | base | 100200.0 | 12.100 | 12.100 | 12.100 | 0.001 | 5.000 | 64.0 |
| kafkars | head | 118500.0 | 9.050 | 9.050 | 9.050 | 0.001 | not reported | not reported |

## Paired comparisons

Ratios are numerator over denominator of the medians. The interval is the paired-block percentile bootstrap over the per-attempt pairs.

| Comparison | Metric | Ratio | CI low | CI high | Direction | Clears threshold |
| --- | --- | ---: | ---: | ---: | --- | --- |
| kafkars / librdkafka | acknowledged goodput | 1.1826 | 1.1780 | 1.1850 | larger is better | yes |
| kafkars / librdkafka | p50 offer-to-terminal | 0.7479 | 0.7408 | 0.7517 | smaller is better | yes |
| kafkars / librdkafka | p99 offer-to-terminal | 0.7479 | 0.7408 | 0.7517 | smaller is better | yes |
| kafkars / librdkafka | p99.9 offer-to-terminal | 0.7479 | 0.7408 | 0.7517 | smaller is better | yes |
| kafkars / librdkafka | p99 admission wait | 1.0000 | 1.0000 | 1.0000 | smaller is better | unresolved |

## Gates

| Gate | Result | Rule | Observed |
| --- | --- | --- | --- |
| `attempts-valid` | pass | at least one attempt has to be usable evidence | 5 valid attempts |
| `paired-repetitions` | pass | a comparison needs at least 5 valid paired attempts | 5 of 5 required |
| `dispersion-within-budget` | pass | every compared pair's goodput and p99 ratio must vary by no more than 0.05 of its mean, over the same per-attempt ratios the interval is drawn from | every gated ratio dispersion is inside the budget |
| `kafkars-over-librdkafka:acknowledged_records_per_second` | pass | acknowledged goodput improves only when the whole confidence interval is above 1.050 (larger is better) | ratio of medians 1.1826, interval [1.1780, 1.1850] |
| `kafkars-over-librdkafka:p50_intended_to_terminal_ns` | pass | p50 offer-to-terminal improves only when the whole confidence interval is below 0.950 (smaller is better) | ratio of medians 0.7479, interval [0.7408, 0.7517] |
| `kafkars-over-librdkafka:p99_intended_to_terminal_ns` | pass | p99 offer-to-terminal improves only when the whole confidence interval is below 0.950 (smaller is better) | ratio of medians 0.7479, interval [0.7408, 0.7517] |
| `kafkars-over-librdkafka:p999_intended_to_terminal_ns` | pass | p99.9 offer-to-terminal improves only when the whole confidence interval is below 0.950 (smaller is better) | ratio of medians 0.7479, interval [0.7408, 0.7517] |
| `kafkars-over-librdkafka:p99_admission_wait_ns` | fail | p99 admission wait improves only when the whole confidence interval is below 0.950 (smaller is better) | ratio of medians 1.0000, interval [1.0000, 1.0000] |

## Dispersion

| Series | Metric | Coefficient of variation | Gated |
| --- | --- | ---: | :---: |
| librdkafka | acknowledged_records_per_second | 0.0056 | no |
| librdkafka | p50_intended_to_terminal_ns | 0.0214 | no |
| librdkafka | p99_intended_to_terminal_ns | 0.0214 | no |
| librdkafka | p999_intended_to_terminal_ns | 0.0214 | no |
| librdkafka | p99_admission_wait_ns | 0.0000 | no |
| librdkafka | cpu_core_seconds | 0.0000 | no |
| librdkafka | max_rss_bytes | 0.0000 | no |
| kafkars | acknowledged_records_per_second | 0.0081 | no |
| kafkars | p50_intended_to_terminal_ns | 0.0124 | no |
| kafkars | p99_intended_to_terminal_ns | 0.0124 | no |
| kafkars | p999_intended_to_terminal_ns | 0.0124 | no |
| kafkars | p99_admission_wait_ns | 0.0000 | no |
| kafkars/librdkafka | acknowledged_records_per_second | 0.0038 | yes |
| kafkars/librdkafka | p50_intended_to_terminal_ns | 0.0091 | no |
| kafkars/librdkafka | p99_intended_to_terminal_ns | 0.0091 | yes |
| kafkars/librdkafka | p999_intended_to_terminal_ns | 0.0091 | no |
| kafkars/librdkafka | p99_admission_wait_ns | 0.0000 | no |

Ratio series are gated against the noise budget. Per-subject rows are informational: they say whether the machine was steady, which is a different question from whether the comparison was.

## Request economics

What each client spent in broker traffic for the records it delivered. Only subjects whose client emits native statistics appear.

| Subject | Produce requests | Per million acknowledged | Records per request | Payload share of wire bytes | Records per batch | Retries | Timeouts |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| librdkafka | 500 | 100000.0 | 10.0000 | 0.8325 | 10.0000 | 10 | 0 |

## Attempts

| Attempt | Execution status | Valid | Bundle digest |
| --- | --- | --- | --- |
| `attempt-0` | complete | true | `unsealed` |
| `attempt-1` | complete | true | `unsealed` |
| `attempt-2` | complete | true | `unsealed` |
| `attempt-3` | complete | true | `unsealed` |
| `attempt-4` | complete | true | `unsealed` |

## Notes

- subject kafkars reports no native client statistics, so its request economics are absent rather than zero
