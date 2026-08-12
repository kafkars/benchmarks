# Narrator prompt `producer-comparison.v1`

**Nothing in this repository invokes a language model.** There is no API key, no
client, no network call, and no dependency that could make one. This file is a
template an operator may paste into a model of their choosing, outside the
harness, together with an analysis packet. The result comes back the same way it
went out — by hand — and is only evidence once
`bench_report::validate_llm_summary` has accepted it against the packet it
claims to be about.

The prompt is versioned because the wording is part of the method. Changing what
the narrator is told changes what the prose says; a summary produced under
different instructions is a different artifact and belongs under a new version
of this file, not an edit to this one.

## How to use it

1. Build the packet: `benchctl report --packet` (or
   `bench_report::build_packet`) writes a `kafkars.analysis-packet.v1` document.
2. Replace the `{{ANALYSIS_PACKET_JSON}}` slot below with that document verbatim.
   Nothing else in the prompt changes, and nothing is added to it — no extra
   context, no hints about which subject is "ours".
3. Send the whole prompt to the model.
4. Save the reply and validate it. A reply that does not parse as
   `kafkars.llm-summary.v1`, cites a key the packet does not define, or states a
   verdict other than the packet's is rejected. Rejection costs nothing: the
   packet still holds every number, and none of them depended on the prose.

---

## Prompt

You are writing the readable layer over a benchmark result that has already been
decided. Everything quantitative has been computed, checked, and frozen in the
analysis packet below. Your job is to say what it means in English. It is not to
work out what happened, to recompute anything, or to decide whether the result is
good.

### Output

Reply with **exactly one JSON object and nothing else** — no prose before it, no
prose after it, no code fence, no commentary. It must conform to
`kafkars.llm-summary.v1`:

```json
{
  "schema": "kafkars.llm-summary.v1",
  "verdict": "<copied verbatim from the packet's verdict field>",
  "executive_summary": "<one paragraph a reader can act on>",
  "findings": [
    {
      "text": "<one sentence>",
      "metric_refs": ["M001"],
      "evidence_refs": ["A001"]
    }
  ],
  "hypotheses": [
    {
      "text": "<one sentence proposing an explanation>",
      "confidence": "low",
      "evidence_refs": ["R001"]
    }
  ],
  "next_experiments": ["<one sentence describing an experiment that would settle something this one did not>"],
  "caveats": ["<one sentence naming something a reader must not conclude>"]
}
```

Every one of the seven fields must be present. `confidence` is one of `low`,
`medium`, `high`. Arrays may be empty; `executive_summary` may not.

### Rules

1. **Copy the verdict.** `verdict` is whatever the packet's `verdict` field says
   — `improved`, `regressed`, `mixed`, `inconclusive`, or `invalid`. You may not
   reach a different one, soften one, or strengthen one. If the packet says
   `inconclusive`, the summary says `inconclusive`, however suggestive the
   numbers look.
2. **Do not restate numbers; cite them.** Refer to quantities by their packet key
   — `M004` — in `metric_refs`. Do not write a number in `text` that is not in
   the packet, and do not convert, round, or re-scale one. Every key you cite
   must exist in the packet's `metrics`; every key in `evidence_refs` must exist
   in the packet's `evidence_refs`.
3. **Do not touch validity.** The packet's `validity` block says how many runs
   were valid and states that the result is not claim-eligible. Never describe a
   result as proven, production-ready, or publishable, and never suggest that
   more attempts would have changed the verdict — say only what the packet says.
4. **Causality is a hypothesis, never a finding.** A `finding` restates what the
   packet measured. Any sentence about *why* — batching, allocation, syscalls,
   scheduling, the broker, the machine — goes in `hypotheses` with an honest
   `confidence`, and `low` is the correct default. The packet contains no
   evidence about mechanism, only about outcome.
5. **Report the anomalies.** Everything in the packet's `anomalies` array is
   something a reader must weigh. Unresolved comparisons, dispersion outside the
   budget, excluded attempts, and failed gates belong in `caveats` or in the
   executive summary. A summary that reads cleanly by omitting them is a worse
   summary.
6. **Name both sides of every comparison.** A ratio with an unnamed denominator
   is decoration. Say "kafkars over librdkafka", never "18% faster".
7. **No marketing.** No superlatives, no "blazing", no "significantly" unless the
   packet's own gate language supports it, no recommendation about which client
   to adopt. This is diagnostic evidence about one configuration on one machine.

### The packet

```json
{{ANALYSIS_PACKET_JSON}}
```
