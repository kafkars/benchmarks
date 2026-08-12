## What changed

<!-- control plane, adapter, schema, scenario, verifier, report, harness scripts -->

## Contract

- [ ] Schemas are append-only: no existing schema id changed meaning, and any
      breaking change ships as a new versioned id.
- [ ] Adapters use only public client surfaces; none reaches into internals to
      make a number look better.
- [ ] Evidence immutability holds: sealed bundles are never rewritten, and any
      change to bundle layout, checksums, or identity inputs is called out here.
- [ ] Sibling pins in `dependencies/sibling-revisions.env` are unchanged, or the
      pin rule was followed (client revision chosen first, driver and protocol
      copied from that revision's own committed env).

## Comparability

<!-- Does this change what a benchmark measures? If yes, say which prior
     evidence bundles stop being comparable, and why that is acceptable. -->

## Validation

- [ ] Complete diff reviewed
- [ ] `scripts/check`
