# Contributing

Read [`ARCHITECTURE.md`](ARCHITECTURE.md), [`STYLE.md`](STYLE.md), and
[`AGENTS.md`](AGENTS.md) before changing code. The repository contract in
`AGENTS.md` applies to human contributors too; it is written down there because
it is the part of this project that is easiest to erode by accident.

For each patch:

1. Name which side of the boundary you are on: control plane, adapter,
   verifier, or evidence document.
2. State what a reader of a sealed bundle can newly conclude, and what they
   still must not.
3. If the change touches an evidence document, say whether it adds a field or
   mints a new schema id. Those are the only two options.
4. Add the test that would fail if the behavior regressed, not the test that
   passes today.
5. Keep the patch narrow enough to review as one decision.
6. Run `scripts/check` and inspect the complete diff.

Adapter work also runs `scripts/check-benchmarks`, which builds the adapters
against the pinned siblings and checks the conformance vectors three ways: the
Rust adapter, the C adapter, and the committed goldens must agree.

The repository declares Rust 1.88 as its MSRV and requires Node.js 22.18 or
newer for the control-plane lanes. Both are pinned rather than "whatever is
installed", because a benchmark harness that cannot reproduce its own toolchain
cannot ask anyone to trust its numbers.

Measurements committed to this repository must state their host and their
baseline. Numbers from a developer machine are diagnostic and are never
presented as claims.

Commits are conventional, lowercase, imperative, and carry no trailing period.

Participation is governed by [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md). Report
security issues through [`SECURITY.md`](SECURITY.md), never a public issue. By
contributing, you agree that your contribution is licensed under this
repository's Apache-2.0 license.
