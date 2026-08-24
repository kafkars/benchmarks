# Releasing

Nothing is published from this repository.

No crate in this workspace goes to crates.io — every manifest carries
`publish = false` — and there is no npm package, no container image, and no
binary distribution. Consuming this project means checking it out and running
it.

## The artifact is the evidence bundle

What this repository produces is a sealed bundle under
`results/<experiment-id>/<attempt-id>/`: the resolved experiment, the captured
environment, every subject's output, the verification reports, `checksums.txt`,
and a manifest carrying the bundle digest. A bundle is immutable once sealed and
is verifiable anywhere:

```sh
cd results/<experiment-id>/<attempt-id>
shasum -a 256 -c checksums.txt
```

Sharing a result means sharing the bundle, not a screenshot of one number from
it. A number quoted without its bundle cannot be checked, and this project has
no use for a performance claim nobody can check.

## Tagging

Tags mark reviewed states of the harness, not shipped software. When the harness
reaches a state whose results are meant to be cited, tag that revision so a
bundle's recorded harness revision resolves to something readable. Signed
annotated tags are the intended mechanism and are expected to follow the house
pattern used in the sibling repositories; the signing workflow is not set up
here yet, and this file should be updated with the fingerprint and procedure
when it is.

## Before tagging

1. `zcheck` green from a clean checkout.
2. `zcheck run adapters-conformance` green, with the conformance vectors
   agreeing three ways.
3. `CHANGELOG.md` updated.
4. `dependencies/sibling-revisions.env` pointing at the revisions the tag is
   meant to describe, and strict provenance passing over them:

   ```sh
   KAFKA_BENCH_PROVENANCE=strict scripts/check-dependency-provenance
   ```

   This step is not covered by item 1. Plain `zcheck` runs provenance in
   **advisory** mode, where an absent, mismatched, or dirty sibling prints a
   warning and still exits 0 — which is what lets the gate run on a machine
   sitting on another branch, and on a clone with no siblings beside it at all.
   Strict mode is the one that refuses. A tag is the point at which a bundle's
   recorded revisions become something someone else may cite, so the strict run
   has to be made deliberately, here, rather than assumed from a green gate.
