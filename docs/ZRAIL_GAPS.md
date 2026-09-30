# Zrail integration gaps

This repository uses zrail 0.0.1 as the architecture authority for its Rust
workspace. Two repository requirements remain outside that authority and are
kept executable in `scripts/check-detached-policy` rather than silently
deferred.

## Zrail 0.0.2 ordinary binding resolution

Zrail 0.0.2 moves the lock to semantics epoch 2, but it cannot currently
baseline this workspace under the existing strict policy. A read-only check
reports 62 `RUST-INCLUDE-002` errors for ordinary Rust bindings, including a
method call in `crates/bench-adapter-librdkafka/src/run.rs`, a public field in
`crates/bench-report/src/economics/totals.rs`, and a local variable in
`crates/benchctl/src/supervise.rs`.

Those are normal, compiler-resolved Rust expressions rather than include or
import indirection the repository can simplify. Weakening macro policy,
accepting unresolved analysis, or restructuring working Rust to move the
diagnostic would reduce authority rather than migrate it. The repository
therefore stays on zrail 0.0.1 and does not regenerate `zrail.lock` at epoch 2.

Requested zrail change: resolve ordinary path bindings across this workspace,
or report the unsupported language form precisely enough that the repository
can make a source-level decision without suppressing unresolved analysis.

## Detached workspace with reviewed external paths

The Kafkars adapter is a separate Cargo workspace. Its client dependency now
uses the exact published registry RC and locked checksums; the historical
external-path escape is gone. Before that cutover zrail rejected its root with:

```text
dependency "kafkars": path dependency resolves outside the repository
```

That fail-closed behavior was correct for an undeclared path escape. The adapter
remains excluded from the harness workspace because its subject graph has a
separate authority boundary. It does not receive zrail's dependency layers, macro provenance, source
scopes, or content-bound ratchets. The companion checker covers module
contracts, facade shape, sibling tests, lint floors, and file budgets, but it
does not claim equivalent macro or capability analysis.

The excluded adapter manifests and lock also cannot be declared as content-bound
gate inputs because zrail rejects inputs hidden by `repository.exclude`. The
reviewed gate script itself is content-bound and validates those live files,
but their bytes are not independently represented in `zrail.lock`.

Requested zrail capability: an analysis-only external-path attestation binding
the dependency name, resolved directory, repository identity, and full commit
SHA without bringing the external package under the proposal repository's
authority.

## Resolved transitive package bans

The repository contract bans async runtimes from the harness's complete
resolved graph, including dev-dependencies, and bans Criterion and Divan from
both benchmark workspaces. Zrail 0.0.1 can reject declared dependency edges but
does not express a package-name prohibition over every package selected in a
Cargo lock file. The companion checker therefore reads the complete locks and
fails when those packages are present.

Requested zrail capability: a content-bound resolved-package deny rule scoped
to one Cargo workspace and optionally filtered by normal, build, and
development reachability.

## Deployment boundary, not a zrail product gap

The repository-owned `architecture-authority.yml` workflow provides
protected-base preview feedback and never executes proposal source. It is not
production merge authority because all workflows in this repository share one
GitHub Actions identity. A required result needs an organization ruleset
workflow or dedicated App outside the proposal's write domain; that is a
deployment task, not a change to zrail itself.
