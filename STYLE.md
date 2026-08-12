# Style

- A measurement without its dispersion and its repetition count is not a result.
- Name the baseline in the same breath as the number; a ratio with an unnamed
  denominator is decoration.
- Prefer an explicit recorded gap to an implied guarantee.
- Execution status answers "did it run", validity answers "does it mean
  anything", and claim eligibility answers "may we say so in public". Keep the
  three apart in code, in documents, and in prose.
- Every document carries its schema id as its first field.
- Identity is derived from bytes, never from a name somebody typed.
- Integers for anything hashed; floating point only for evidence.
- Time enters deterministic code as an explicit absolute value. Wall clocks name
  things; monotonic clocks enforce deadlines.
- The control plane owns time, topics, order, and verdicts. Adapters own the
  measured path and nothing else.
- Configuration names tools by argument vector, never by hard-coded adapter
  name.
- Failure paths produce evidence, not silence.
- Keep modules small enough that one ownership concept can be reviewed at once.
- Scripts are extensionless bash with `set -euo pipefail`, and must run on the
  bash a developer's macOS actually ships as well as on the CI runner's.
- Examples are executable and checked, not illustrative.
