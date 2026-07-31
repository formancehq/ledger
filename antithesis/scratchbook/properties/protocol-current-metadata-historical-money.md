# protocol-current-metadata-historical-money — Current account metadata selects historical money

| | |
|---|---|
| **Priority** | P1 |
| **Type** | Safety |
| **Property** | PIT address predicates are evaluated against historical account identities, while metadata and has-asset predicates select accounts from one current read-store snapshot before historical monetary effects are aggregated. |
| **Invariant** | On a specific replica whose local index is live and whose `lastIndexedSequence` has reached the accepted metadata-mutation log, use `AlwaysOrUnreachable(actual == foldHistoricalMoney(accountsMatchingAcceptedCurrentMetadata))` for current-only and mixed filters. A `Sometimes` checkpoint requires a current metadata mutation to change membership for the same old PIT selector while the historical amount for any still-selected account remains unchanged. |
| **Antithesis Angle** | Mutate metadata after monetary effects, gate each target replica through its own stale-local index status, and race index/history publication with PIT reads. Exercise historical identities absent from current filter results plus mixed AND/OR/NOT filters while the history projection compacts independently. |
| **Why It Matters** | This split is an intentional compatibility semantic. Accidentally historizing metadata, or restricting address filters to the current account universe, returns the wrong account set without corrupting the underlying monetary history. |
| **Confidence** | High |

## Exact SDK assertion plan

```go
assert.AlwaysOrUnreachable(
    semanticAggregateEqual(got, oracle.HistoricalMoney(
        selector,
        oracle.AccountsMatchingAcceptedMetadata(filter, metadataMutationLogSequence),
        view.GetLogWatermark(),
    )),
    "pit: current account predicates select historical monetary effects",
    details,
)
assert.AlwaysOrUnreachable(
    mixedFilterResultMatchesBooleanOracle,
    "pit: mixed historical-address and current-metadata filters preserve boolean semantics",
    details,
)
assert.Sometimes(
    samePITSelectorChangedMembershipAfterCurrentMetadataMutation,
    "pit: current metadata can change membership of an old monetary view",
    details,
)
assert.Sometimes(
    targetReplicaCurrentIndexReachedMutation,
    "pit: target replica current index reaches the metadata mutation sequence",
    details,
)
```

Before evaluating a replica, poll `GetIndexStatus` through that replica's single-target connection with `x-consistency: stale` until the target index has `currentVersion > 0` and `lastIndexedSequence >= metadataMutationLogSequence`. The metadata membership oracle comes from the workload's accepted metadata writes, not from querying the same index under test. Keep the dedicated ledger/key quiescent between this gate and the PIT read. For direct exact/prefix address leaves the oracle uses the historical identity universe, including accounts absent from current read-store results.

The gate is sufficient for ordinary metadata-value mutations because the indexbuilder commits index row mutations and the `LastIndexedSequence` cursor that certifies them in the same read-store Pebble batch. It is intentionally different from the PIT request's `minLogSequence`, which gates the history manifest but does not invoke the normal read-store wait. A schema retype is outside this first workload shape: if added, the gate must also require `currentVersion` to equal the bumped forward-encoding version because log progress proves that a rewrite was scheduled, not that its atomic switch completed.

`AlwaysOrUnreachable` is the safety form for successful responses. `Sometimes` asserts the unusual but intended semantic state and gives Antithesis a checkpoint near the race between current indexing and historical aggregation.

## Code evidence

- `antithesis/scratchbook/sut-analysis.md` records the accepted product boundary: money is historical; metadata, schemas, account types and Numscript definitions are current-state-only.
- `internal/application/ctrl/controller_default.go:1023-1064` opens one current read-store snapshot, compiles current subfilters against it, and then aggregates from the historical view.
- `internal/application/ctrl/volume_view.go:197-249` explicitly splits hard-coded address leaves from metadata/has-asset leaves.
- `internal/application/ctrl/volume_view.go:252-364` rejects unsupported parameterized/invalid forms, preserves AND/OR structure, and keeps current-only NOT semantics inside the current compiler.
- `internal/application/ctrl/volume_view.go:367-435` binds current account sets once and evaluates mixed predicates.
- `internal/application/ctrl/volume_view_test.go:140-205` models a historical address absent from current compiler results while metadata matches use the current account set; later tests cover exact and NOT address semantics.
- `internal/query/aggregate_history.go:34-63,230-276` applies the selected account predicate to sorted historical volume identities.
- `internal/adapter/grpc/server_bucket.go:527-535,770-815` shows that a live `ListAccounts` `minLogSequence` waits for the local read-store cursor, but `server_bucket.go:1408-1424` passes a PIT minimum to the controller/history path without performing that read-store wait.
- `internal/application/indexbuilder/process_logs.go:200-230,265-268` writes index mutations and `LastIndexedSequence` in one batch, flushes it, then publishes the in-memory progress and wakes waiters.
- `internal/application/ctrl/controller_default.go:1295-1329,1415-1439` builds `GetIndexStatus` from a local read-store snapshot containing both `lastIndexedSequence` and the per-replica `currentVersion`.
- `pkg/actions/indexes.go:60-102` provides the reusable local `currentVersion > 0` readiness poll. It does not by itself gate a later metadata mutation's log sequence.
- `tests/antithesis/workload/internal/pernode.go:82-182` provides single-target connections and `WithStaleConsistency`, which prevent an intended per-replica observation from being forwarded to the leader.
- `tests/antithesis/workload/bin/cmds/main/eventually_cross_node_identity/main.go:187-279` gates on `lastPersistedIndex`; its comments correctly scope that cursor to FSM-owned primary Pebble state, not the asynchronous read store.
- `docs/technical/architecture/subsystems/indexer/indexer.md:151-160,253-270` documents the atomic progress cursor, local `min_log_sequence` semantics, and the distinct per-replica version switch. `tests/e2e/cluster/metadata_index_per_replica_consistency_test.go:19-37` explicitly notes that log gating is insufficient for rewrite completion.

## Failure scenario

An account receives money in January, gains `category=vip` in July, and is queried with a January PIT plus `metadata.category == vip`. A historical-metadata implementation incorrectly excludes it; a fully current implementation includes its July money. The intended result includes the account based on July metadata but only its January-cutoff monetary effects.

## Existing versus missing instrumentation

- **Existing deterministic coverage:** filter-plan and historical-query unit tests, the one-node PIT metadata E2E, and per-replica metadata-index convergence E2E.
- **Partially existing SDK/workload support:** `DialPerNode`, `WithStaleConsistency`, and the exact-primary-index convergence driver are reusable. The existing metadata driver verifies main-store `GetAccount` read-after-write, not read-store filter visibility.
- **Missing workload instrumentation:** the four assertions above, an oracle that records accepted metadata independently from the index under test, and a same-replica status poll combining local `currentVersion > 0` with `lastIndexedSequence >= metadataMutationLogSequence`.
- **SUT-side instrumentation:** not required; local `GetIndexStatus` exposes the required read-store progress and version state. A SUT-side snapshot identifier would improve diagnosis but is not part of the public contract.

## Assumptions

- The first workload shape changes metadata values under a stable schema; it does not combine this property with `SetMetadataFieldType` rewrites.
- The metadata ledger/key is dedicated or quiesced between the local status gate and PIT read, so a later accepted mutation cannot invalidate the workload's expected current membership.
- Every per-replica status and PIT call uses the same `PerNodeConn` with stale consistency; default consistency could forward during synchronization and make the gate observe another node.
- Numeric ledger incarnation remains part of the monetary oracle even when a ledger name is reused.

## Open Questions

None.

### Investigation Log

#### Which existing workload barrier is sufficient to prove current metadata/index visibility on a specific stale-local replica?

- **Examined:** `tests/antithesis/workload/internal/pernode.go`; `eventually_cross_node_identity/main.go`; `parallel_driver_metadata/main.go`; `pkg/actions/indexes.go`; gRPC `ListAccounts`, `AggregateVolumes`, and `GetIndexStatus`; `DefaultController.GetIndexStatus`; indexbuilder `processLogs`; read-store `WaitForSequence`; the query-pipeline/indexer/API-comparison documentation; the PIT metadata E2E; and `metadata_index_per_replica_consistency_test.go`.
- **Found:** `Barrier`, linearizable `GetAccount`, and the workload's exact `lastPersistedIndex` gate certify Raft/FSM primary state only. The PIT request's `minLogSequence` is routed to history and deliberately skips `waitMinLogSequence`, so it does not certify the current metadata index. A same-node stale `GetIndexStatus` response exposes the local read-store `lastIndexedSequence` and `currentVersion`. For an ordinary metadata-value mutation under a stable schema, `currentVersion > 0` plus `lastIndexedSequence >= mutationLogSequence` is sufficient: index mutations and the progress cursor are committed in one Pebble batch. The existing `WaitForMetadataIndexReady` helper supplies the version half and can be called with stale consistency, but a new workload poll must add the sequence half.
- **Not found:** no existing Antithesis helper combines local index readiness and mutation-sequence progress, and no wire primitive waits for a metadata schema-rewrite switch. The repository instead documents polling the per-replica version; the E2E test confirms `minLogSequence` alone is insufficient for retypes.
- **Conclusion:** resolved. The property can run per replica, not only through the leader, by using the same `PerNodeConn` plus stale consistency for a combined `GetIndexStatus` gate before the PIT assertion. Keep the first workload shape on a stable schema; a future retype variant must additionally wait for the expected `currentVersion` switch.
