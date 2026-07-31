# integrity-ledger-incarnation-isolation — Recreated ledger names never inherit old monetary history

## Catalog entry

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | PIT results for a ledger name resolve only the currently selected numeric ledger incarnation and never include effects recorded under an earlier deleted incarnation with the same name. |
| **Invariant** | `Always(result_uses_selected_ledger_id_only, "pit: recreated ledger name is isolated from prior incarnation history")`. This must hold for every successful query because cross-incarnation leakage is a direct monetary integrity violation. |
| **Antithesis Angle** | Delete and recreate ledgers under concurrent applies, builder lag, compaction, restore, and cross-node reads. Query timestamps before and after recreation while nodes pin different physical layouts but a common authoritative prefix. |
| **Why It Matters** | A customer reusing a ledger name could see balances or transaction effects belonging to the previous logical ledger, violating both financial correctness and tenant data separation expectations. |
| **Confidence** | High |
| **Focus** | Data Integrity |

**Open Questions:**

- None. This property covers the shipped name-based API, which resolves only
  the current numeric incarnation. Direct historical-incarnation tooling is
  outside the current public contract and would require a separate property.

## Evidence trail

- `internal/domain/balancehistory/reducer.go:103-169` tracks active ledger name to numeric ID and retains all seen IDs in deterministic reducer state.
- `internal/domain/balancehistory/reducer.go:236-272` rejects duplicate active names, reused numeric IDs, and invalid deletion lifecycle.
- `internal/domain/balancehistory/reducer.go:275-305` attaches monetary effects to the currently active numeric ID and fails if no incarnation is active.
- `internal/domain/balancehistory/state.go:23-54` validates restored active/seen incarnation mappings.
- `internal/application/ctrl/controller_default.go:956-993` resolves the requested ledger name to the current primary `LedgerInfo` and passes its numeric ID into the pinned history provider.
- `internal/application/ctrl/volume_view.go:103-116` binds ledger ID into both the historical view and the view token.
- `internal/storage/balancehistorystore/store_test.go:113-139` proves identical account names under ledger IDs 7 and 8 remain separate.
- `internal/query/pit_v2_compatibility_test.go:127-151` covers old and new incarnation results independently.

## Failure scenario to explore

1. Create ledger name `L`, post distinctive colored/precision amounts, and wait for PIT catch-up.
2. Delete `L`, recreate it, and post a disjoint signature of amounts/accounts.
3. Race compaction, replica restart, and source lag around the lifecycle boundary.
4. Query the name `L` at timestamps covering both lifetimes. Every returned trailer must carry the current incarnation ID, and results must contain only that ID's effects.
5. Repeat across replicas after a common watermark barrier.

## Instrumentation status

- **Existing SDK instrumentation:** missing.
- **Missing SUT-side guidance:** add `Always` in the PIT workload comparing trailer ledger ID to the independently observed current `LedgerInfo` ID and an `Unreachable` in the reducer for any monetary apply without an active audit-derived incarnation.
- **Workload-side check:** existing ledger lifecycle drivers can generate the sequence, but the PIT oracle is not incarnation-keyed today.
