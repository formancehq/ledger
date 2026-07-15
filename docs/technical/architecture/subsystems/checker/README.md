# Checker

The checker (`internal/application/check`) is the integrity verification subsystem. It does **not** sit on the request path — it is invoked on demand via gRPC (`BucketService.CheckStore`) and produces a stream of `CheckStoreEvent`s describing any divergence between the persisted projections and what the audit chain says they should hold.

Two pages cover what the checker depends on and what it does.

## Documents

| Document | Description |
|----------|-------------|
| [audit-chain.md](audit-chain.md) | The BLAKE3-keyed audit hash chain — the only cryptographically-bound dataset in the system. Structure, lifecycle, tampering model. |
| [checker.md](checker.md) | The verification passes: how each persisted projection is re-derived from the audit chain and compared against what's stored. Error taxonomy. |

## Why a dedicated subsystem

The audit chain and the checker are tightly coupled by an explicit invariant: **every persisted dataset is either hash-bound (the audit log itself) or derivable from the hash-bound data, in which case the checker must verify it on every `Check()` run** ([CLAUDE.md invariant #8](../../../../../AGENTS.md)). Two scope refinements from invariant #8 apply: the rule covers the **primary FSM store** that `Check()` opens and walks — **peer secondary stores** (the `readstore` inverted index, the `usagestore` counters) are out of scope *by construction* since `Check()` never opens them and they are rebuildable caches; and a primary-store projection is exempt only when it is rebuildable through a real, *wired* rebuild path or is purely informational (cf. `BuildStatus`). This rule shapes design decisions across every other subsystem — what to refactor versus what to bind — so the checker's coverage is its own first-class architectural concern.

For new persisted state, first classify whether it is business truth, governance
truth, operational consensus state, or a rebuildable projection using
[Audit-Bound vs Technical State](../../audit-vs-technical-state.md). Per invariant
#8, every non-audit dataset persisted in the main Pebble store needs checker
coverage unless it is genuinely discarded and rebuilt by a lifecycle path or lives
in a separate rebuildable side-store. "Genuinely discarded and rebuilt" is narrow:
bloom filters qualify on the backup/restore path
(`internal/infra/attributes/prepare.go` deletes the persisted blocks so restore
rebuilds them from a full attribute scan) and on a bloom-config change applied
through cluster config (`applyClusterConfigUpdate` in
`internal/infra/state/machine_technical_updates.go` purges the `SubGlobBloom`
blocks, calls `BloomFilters.Rebuild`, and signals `StartAsyncBloomPopulate`). On the normal restart / follower-sync
path bloom blocks are instead *restored from the persisted Pebble blocks*
(`CacheSnapshotter.RestoreFromStore` / `restoreBloomFilters`; the full scan runs
only on first boot when no blocks exist), so those blocks are a durably trusted
projection between backups and are **not** covered by the rebuild control. Raft
replication is not a substitute either: it only guarantees every replica applies
the same logical proposal (it does not even guarantee byte-identical serialization
for map-bearing projections — see
[Audit-Bound vs Technical State](../../audit-vs-technical-state.md)), so a value
corrupted or tampered before it is proposed takes effect on every node and no
cross-node comparison can detect it. Two items formerly listed here are no longer integrity gaps: the **mirror
cursor**'s correctness-bearing high-water mark (`LedgerBoundaries.last_mirror_v2_log_id`)
is now verified by `compareMirrorV2LogID` (EN-1550) — the cursor *pointer* itself
is technical replication state, not a gap; and the **readstore inverted-index
contents** are a peer secondary store, out of checker scope *by construction*
(automated detect/drop/rebuild is tracked under `EN-1323` and is not yet wired —
see [Audit-Bound vs Technical State](../../audit-vs-technical-state.md)).
Persisted projections that are genuinely not yet covered — prepared
queries (`SubAttrPreparedQuery`, read by `ExecutePreparedQuery` to drive
user-visible results and with no `compare*` pass), persisted bloom blocks on
the restart path, and the persisted governance projections (signing keys
`SubGlobSigningKey`, signing config `SubGlobSigningConfig`, and maintenance mode
`SubGlobMaintenanceMode` — all read into the live key store / shared state on
recovery by `recovery.go` and consulted by admission to accept or reject writes,
with no `compare*` pass) — are tracked integrity gaps, not approved exemptions.

## Related

- [Consensus → global-log.md](../consensus/global-log.md) — what produces the audit entries the chain links.
- [Indexer → indexes.md](../indexer/indexes.md) — the index registry is a projection the checker verifies via `compareIndexes`.
- [Attributes](../attributes/) — volumes / metadata / reversion / idempotency projections the checker also verifies.
- [Chapters → lifecycle.md](../chapters/lifecycle.md) — sealing hash verification is one of the checker's passes.
