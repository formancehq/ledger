# PIT v2 semantic compatibility fixture

This fixture freezes the monetary semantics of the latest local v2 branch used
for the v3 historical-balance implementation review:

- ref: `release/v2.4`
- commit: `8ef2eb1dfaa505113f7b1ec723f77a54073d63ac`
- `resource_aggregated_balances.go` blob: `3dc50d4d395dd112e5ac18818d622e6f0fba9bd9`
- `resource_volumes.go` blob: `74a1abf5a075f6be65968b2731596e3514ae513b`

The v2 query reads additive rows from `moves`, selects rows whose effective or
insertion timestamp is less than or equal to `pit`, and sums source amounts as
output and destination amounts as input. The tests reconstruct that operation
independently and compare it with the v3 reducer, history store, and aggregate
query.

Rows marked `v2.4` cover the compatibility contract: backdated transactions,
resolved Numscript and mirror postings, normal reversals, and
`atEffectiveDate` reversals. The remaining rows make v3-only behavior explicit:

- `v3-retention-extension`: EPHEMERAL and TRANSIENT volumes may not survive in
  the live projection, but their accepted monetary moves remain in balance history.
- `v3-color-extension`: v2.4 had no posting color dimension; v3 preserves color
  buckets by default and only collapses them when explicitly requested.

The v3 store keys rows by ledger name. Ledger names cannot be recreated after
deletion, so no numeric incarnation dimension is needed in this projection.

Precision remains part of the asset identity, as it was in v2. v3's
`useMaxPrecision` option is an additive query feature and is tested separately.

Metadata is intentionally **not** part of this fixture. v2 could join metadata
revisions at the requested time when its metadata-history feature was enabled. The accepted
v3 design applies account and metadata filters from the current read index and
does not history metadata. Consequently, a historical monetary result is v2-compatible
for the selected account set, but a filter whose metadata changed after the cutoff
may select a different current account set. This is the deliberate compatibility
boundary, not an untested claim of metadata history.
