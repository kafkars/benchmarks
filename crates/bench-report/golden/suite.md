# producer-comparison suite

> Diagnostic only — never claim-eligible. These numbers describe the machine and configuration they were measured on, and support no published comparison between clients.

- evidence: 5 of 5 attempts valid
- public claim allowed: no
- practical threshold: 5.0%
- analysis: 1000 bootstrap resamples, seed `20240601`
- experiment: `74283dc35de37b3a6e2714c29a6c7fd176866332afdf2c5c8ccef2388dc698dd`

## Result

Ratios are numerator / denominator. Direction says which way is better. A result is favorable only when its full confidence interval clears the practical threshold.

| Comparison | Metric | Ratio | CI low | CI high | Direction | Result |
| --- | --- | ---: | ---: | ---: | --- | --- |
| kafkars / librdkafka | acknowledged goodput | 1.1826 | 1.1780 | 1.1850 | larger is better | favorable |
| kafkars / librdkafka | p50 offer-to-terminal | 0.7479 | 0.7408 | 0.7517 | smaller is better | favorable |
| kafkars / librdkafka | p99 offer-to-terminal | 0.7479 | 0.7408 | 0.7517 | smaller is better | favorable |
| kafkars / librdkafka | p99.9 offer-to-terminal | 0.7479 | 0.7408 | 0.7517 | smaller is better | favorable |
| kafkars / librdkafka | p99 admission wait | 1.0000 | 1.0000 | 1.0000 | smaller is better | unresolved |
| kafkars / librdkafka | p99 accepted-to-terminal (client-internal portion, locating not claiming) | 0.7479 | 0.7408 | 0.7517 | smaller is better | favorable |

## Scorecard

Medians from valid attempts. Latency runs from offer to terminal and includes admission wait. Lateness and accepted-to-terminal only help locate a difference; refusing work can improve both.

| Subject | Role | Goodput (records/s) | p50 (ms) | p99 (ms) | p99.9 (ms) | Admission p99 (ms) | Lateness p99 (ms) | Accepted-to-terminal p99 (ms) | CPU (core-s) | CPU per 1M ack (core-s) | Peak RSS (MiB) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| librdkafka | base | 100200.0 | 12.100 | 12.100 | 12.100 | 0.001 | not reported | 6.050 | 5.000 | 5000.000 | 64.0 |
| kafkars | head | 118500.0 | 9.050 | 9.050 | 9.050 | 0.001 | not reported | 4.525 | not reported | not reported | not reported |

## Checks

| Check | Result | Requirement | Observed |
| --- | --- | --- | --- |
| `attempts-valid` | pass | at least one attempt has to be usable evidence | 5 valid attempts |
| `paired-repetitions` | pass | a comparison needs at least 5 valid paired attempts | 5 of 5 required |
| `dispersion-within-budget` | pass | every compared pair's goodput and p99 ratio must vary by no more than 0.05 of its mean, over the same per-attempt ratios the interval is drawn from | every gated ratio dispersion is inside the budget |
| `matched-execution-surface` | pass | both sides of a comparison must declare the same payload construction and the same serialization placement, because a ratio across unlike work is not a comparison | 5 compared pairs declare the same measured work |
| `kafkars-over-librdkafka:acknowledged_records_per_second` | pass | acknowledged goodput improves only when the whole confidence interval is above 1.050 (larger is better) | ratio of medians 1.1826, interval [1.1780, 1.1850] |
| `kafkars-over-librdkafka:p50_intended_to_terminal_ns` | pass | p50 offer-to-terminal improves only when the whole confidence interval is below 0.950 (smaller is better) | ratio of medians 0.7479, interval [0.7408, 0.7517] |
| `kafkars-over-librdkafka:p99_intended_to_terminal_ns` | pass | p99 offer-to-terminal improves only when the whole confidence interval is below 0.950 (smaller is better) | ratio of medians 0.7479, interval [0.7408, 0.7517] |
| `kafkars-over-librdkafka:p999_intended_to_terminal_ns` | pass | p99.9 offer-to-terminal improves only when the whole confidence interval is below 0.950 (smaller is better) | ratio of medians 0.7479, interval [0.7408, 0.7517] |
| `kafkars-over-librdkafka:p99_admission_wait_ns` | fail | p99 admission wait improves only when the whole confidence interval is below 0.950 (smaller is better) | ratio of medians 1.0000, interval [1.0000, 1.0000] |

## Run stability

| Series | Metric | Variation | Used by check |
| --- | --- | ---: | :---: |
| librdkafka | acknowledged goodput | 0.0056 | no |
| librdkafka | p50 offer-to-terminal | 0.0214 | no |
| librdkafka | p99 offer-to-terminal | 0.0214 | no |
| librdkafka | p99.9 offer-to-terminal | 0.0214 | no |
| librdkafka | p99 admission wait | 0.0000 | no |
| librdkafka | p99 accepted-to-terminal (client-internal portion, locating not claiming) | 0.0214 | no |
| librdkafka | cpu | 0.0000 | no |
| librdkafka | peak rss | 0.0000 | no |
| kafkars | acknowledged goodput | 0.0081 | no |
| kafkars | p50 offer-to-terminal | 0.0124 | no |
| kafkars | p99 offer-to-terminal | 0.0124 | no |
| kafkars | p99.9 offer-to-terminal | 0.0124 | no |
| kafkars | p99 admission wait | 0.0000 | no |
| kafkars | p99 accepted-to-terminal (client-internal portion, locating not claiming) | 0.0124 | no |
| kafkars/librdkafka | acknowledged goodput | 0.0038 | yes |
| kafkars/librdkafka | p50 offer-to-terminal | 0.0091 | no |
| kafkars/librdkafka | p99 offer-to-terminal | 0.0091 | yes |
| kafkars/librdkafka | p99.9 offer-to-terminal | 0.0091 | no |
| kafkars/librdkafka | p99 admission wait | 0.0000 | no |
| kafkars/librdkafka | p99 accepted-to-terminal (client-internal portion, locating not claiming) | 0.0091 | no |

Ratio variation is checked against the noise budget. Subject variation only shows whether the machine was steady.

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
