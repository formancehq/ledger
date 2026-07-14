# Audit-Bound vs Technical State

Ledger persists several kinds of state. The audit log is the only cryptographically
bound business source of truth; every other persisted dataset must either be
derivable from that source or explicitly classified as operational state that does
not decide business outcomes.

Use this rule when adding a persisted keyspace, a `TechnicalUpdate`, a lifecycle
record, or a read-side projection:

> If changing the state after the fact can change a ledger business result, a
> business write acceptance or rejection, or the trust model for business
> evidence, the state must be audit-bound or verified as a projection of
> audit-bound data.

## State Classes

| Class | Definition | Examples | Required control |
| --- | --- | --- | --- |
| Business truth | Changes balances, transactions, metadata, reversions, ledger lifecycle, indexes, or any user-visible business decision. | `AuditEntry`, `Log`, `Volume`, `Metadata`, `Transaction`, `Reference`, reversion bitsets, idempotency outcomes, index registry. | The command must produce an audit entry, and persisted projections must be checked by `internal/application/check`. |
| Governance truth | Changes who can write, how writes are accepted, or how business evidence is produced. It may not change balances directly, but it changes the control plane around business truth. | Signing keys, maintenance mode, chapter schedules, hash algorithm selection. | Prefer an audited order. If a persisted projection of governance state (e.g. the signing-key or signing-config rows under `SubGlobSigningKey` / `SubGlobSigningConfig`) can be altered after the fact so admission or lifecycle code accepts or rejects future writes differently, it must be checker-verified or read back from an audit-bound source — not merely trusted because an audited order once recorded the intent. Non-persisted operator controls (in-memory toggles) do not carry this obligation. |
| Operational consensus state | Coordinates background work or external delivery, but does not itself define ledger business truth. | Event sink cursors/status, backup job state, removed-member registry, mirror status/source-head. | Raft replication applies the same logical proposal on every replica; it is sufficient **only** when a value corrupted before it is proposed cannot silently change a business result. Raft cannot detect a value tampered or corrupted before it is proposed (its logical effect then applies everywhere) — any such projection needs checker coverage regardless of replication. |
| Rebuildable local projection | Speeds reads or background work and can be regenerated from audit-bound data. **A projection only belongs here if it is genuinely thrown away and rebuilt** — not merely rebuildable in principle. | Bloom filters (discarded and rebuilt on the backup/restore path — `internal/infra/attributes/prepare.go` deletes the persisted blocks so restore rebuilds them from a full attribute scan — and on a bloom-config change applied via cluster config, where `applyClusterConfigUpdate` (`internal/infra/state/machine_technical_updates.go`) purges the `SubGlobBloom` blocks, calls `BloomFilters.Rebuild`, and signals `StartAsyncBloomPopulate`; note that on normal restart / follower sync they are instead *restored from the persisted Pebble blocks*, so those blocks are durably trusted between backups — see the floor note below), cache mirrors, snapshots/spool. | Rebuild path is the control **when the projection is actually rebuilt as part of a lifecycle path**. A *durably persisted* projection **in the main store** that is trusted between rebuilds and can shape a business-visible result (e.g. persisted bloom blocks reloaded on restart — see below) additionally needs main-store checker coverage, because nothing rebuilds it before it is served. A projection in a *peer* per-replica store (the readstore inverted index) is instead the index builder's rebuild-health concern, out of main-store checker scope — see "Readstore and Indexes" below. |

The boundary is about impact, not package location. A value under a technical
keyspace can become business-impacting if future code starts using it to decide
whether a business command is accepted or which business data is returned.

Invariant #8 ([AGENTS.md](../../../AGENTS.md)) is the floor: every non-audit
dataset persisted in the main Pebble store is a projection that the checker must
verify, unless it is genuinely discarded and rebuilt by a lifecycle path or lives
in a separate rebuildable side-store outside the checker's scope. "Genuinely
discarded and rebuilt" is narrow: bloom filters qualify on the
backup/restore path, where `internal/infra/attributes/prepare.go` deletes the
persisted bloom blocks so a subsequent restore rebuilds them from a full
attribute scan, and on a bloom-config change applied through cluster config,
where `applyClusterConfigUpdate` (`internal/infra/state/machine_technical_updates.go`)
purges the `SubGlobBloom` blocks, calls `BloomFilters.Rebuild`, and signals
`StartAsyncBloomPopulate` to repopulate from a full attribute scan. On the
normal restart / follower-sync path bloom blocks are
**not** discarded — `CacheSnapshotter.RestoreFromStore`
(`internal/infra/state/cache_snapshotter.go`) loads the persisted bloom blocks
from Pebble via `restoreBloomFilters`, and the full attribute scan
(`StartAsyncBloomPopulate`) runs only on first boot when no persisted blocks
exist. So the persisted bloom blocks are a durably trusted projection between
backups, and the discard-and-rebuild control does not cover a block corrupted or
tampered while it sits on disk. Raft replication is **not** a substitute for
checker coverage: it only guarantees that every replica applies the *same logical
proposal* and reaches the *same logical state* (it does not even guarantee
byte-identical serialization — see the note in "Readstore and Indexes" on
non-deterministic map marshaling). So a value that is corrupted or tampered
*before* it is proposed takes effect on every node identically, and no cross-node
comparison — logical or byte-wise — can catch it. Where the sections below
describe a persisted main-store projection that the checker does not yet verify
(prepared queries,
persisted bloom blocks on the restart path, and the persisted governance
projections — signing keys `SubGlobSigningKey`, signing config
`SubGlobSigningConfig`, and maintenance mode `SubGlobMaintenanceMode`, each read
into the live key store / shared state on recovery and consulted by admission to
accept or reject writes), that is a tracked integrity gap, not an approved
exemption. Those governance projections must either be checker-verified against
the audited orders that recorded the intent, or read back from an audit-bound
source before they are trusted (see the Governance-truth row above).

## TechnicalUpdate Gate

`Proposal.technical_updates` bypasses the order/log path, so a new
`TechnicalUpdate` variant must not be added just because it is convenient. Before
adding a variant, classify it with the table above and answer:

- Can it change a ledger balance, transaction, metadata value, ledger mode, index
  semantics, or prepared query semantics?
- Can it cause a future business order to be accepted, rejected, skipped, or
  replayed differently?
- Can it change the proof model used to trust audit entries?
- If it is only a projection, can the checker re-derive and compare it?

If any answer is yes, the variant needs either an audited order or an explicit
checker pass that verifies the stored value against audit-bound data.

## Mirror Cursor

Mirror ingestion is the highest-risk boundary because the mirror cursor is an
operational value that controls future business proposals. The normal apply path
is correct: the worker proposes mirror ingest orders and a `MirrorSyncUpdate` in
the same Raft proposal, and the FSM queues the cursor write through the same
`WriteSet` so it only advances if the mirrored orders apply successfully.

That atomicity is the load-bearing property: because the `MirrorSyncUpdate` rides
in the *same* proposal as the ingest orders and drains through the *same*
`WriteSet.Merge` / `batch.Commit()`, the persisted cursor **can never lag or lead
the ingested data through any normal or crash path** — a crash lands both or
neither. So through normal operation the stored cursor always equals the highest
audited `MirrorIngest.v2LogId`, and the only way to move it off that value is to
tamper the persisted cursor byte directly (after restore, manual repair, or a
Pebble-level edit). The two tamper directions behave very differently, and
neither is an *integrity-checker* concern:

- A cursor tampered **below** the highest audited `v2LogId` replays.
  `Worker.processBatch` (`internal/application/mirror/worker.go`) reloads the
  cursor and calls `FetchLogs(cursor, ...)`, which returns source logs strictly
  *after* the cursor. `TranslateBatch` then emits fresh `MirrorIngest` orders for
  logs that were already ingested, and `processMirrorCreatedTransaction`
  (`internal/domain/processing/processor_mirror.go`) reapplies every posting with
  `force=true`, overwriting the transaction state and re-adding the amounts to the
  volume pair via `applyPosting`. There is **no deduplication on `v2LogId`** and
  volume mutation is additive, so the balance effects apply a second time — but
  the replay **self-heals into an audit-consistent state**: the duplicate
  `MirrorIngest` orders go through the real proposal path, so they are legitimately
  hash-chained, the cursor re-advances atomically, and the doubled volumes match
  an audit that now contains the duplicates. A `compare*` pass that re-derives
  from the audit (`compareVolumes` / `compareTransactions`) therefore replays the
  *same* orders the FSM applied and **cannot see the doubling**; a cursor-equality
  pass also passes because the cursor has re-advanced. Verifying the cursor at
  rest catches nothing here — the real mitigation is FSM-level `v2LogId`
  idempotency in `processMirrorIngest` (skip an ingest whose `v2LogId` is already
  applied), a functional hardening tracked separately.
- A cursor tampered **beyond** the highest audited `v2LogId` makes the worker skip
  source logs — silent v2→v3 under-ingestion. This *is* stable and at-rest
  detectable, but only as a **parity** property against the external v2 source:
  the expected bound is the worker's tracked source-head (`SubPLMirrorSourceHead`,
  refreshed by `refreshSourceHead`), not anything the checker can re-derive from
  the v3 audit alone (the skipped logs never entered the v3 audit, which stays
  internally consistent). Its home is the mirror worker's recovery reconciliation
  against the source-head, not an audit-vs-store integrity compare.

`MirrorCursor` is therefore classified as **technical replication state**, not a
checker business invariant: the ledger's business truth (balances, transactions,
metadata) is carried by the audit-bound `MirrorIngest` orders and already verified
by the normal checker passes, independent of the cursor. It lives at
`ZonePerLedger` / `SubPLMirrorCursor` in the main Pebble store. Edge cases worth
recording for the worker/recovery side (not the checker):

- **Empty / never-ingested ledger:** the cursor is absent and reads back as `0`
  (`query.ReadMirrorCursor` defaults to `0`, and `processBatch` maps `0` to
  `expectedNextLogID == 1`); a ledger with no audited mirror-ingest logs must
  hold cursor `0`.
- **Deleted ledger:** the cursor **is** cleared as part of cleanup, but only
  after a deferred purge, so there is a transient window to account for.
  `DeleteLedger` records a deferred cleanup (`savePendingLedgerCleanup` →
  `State.PendingLedgerCleanups`). The cursor row survives until a covering
  chapter purge reaches the delete sequence: `WriteSet.executePurge`
  (`internal/infra/state/write_set.go`) then calls `deleteLedgerData`
  (`internal/infra/state/batch.go`), which point-deletes `SubPLMirrorCursor`
  together with `SubPLMirrorSourceHead`, `SubPLMirrorStatus`, and the
  pending-cleanup marker. So the cursor is present only during the
  pending-cleanup window and **absent** once cleanup completes; there is no
  "archived" ledger state. A checker must therefore expect the cursor to be
  gone after cleanup, and treat only a ledger *still awaiting* cleanup (delete
  logged, purge not yet reached) as the transient case to exclude — not assume
  a deleted ledger keeps a stale cursor forever.

There is deliberately **no** `compare*` pass for `MirrorCursor` in
`internal/application/check`. Per invariant #8 the checker guards the business
invariants of the main store against the audit; the cursor is not one of them
(see the two tamper directions above — the *behind* case is checker-invisible and
needs functional `v2LogId` idempotency, the *ahead* case is a v2-parity property
owned by the mirror worker's source-head reconciliation). This is a classification
as technical state, not an unapproved integrity gap: the audit-bound
`MirrorIngest` orders and the existing volume/transaction passes already secure
the business truth. Hardening the two tamper directions (ingest idempotency; a
worker/recovery reconciliation of the cursor against source-head and the highest
audited `v2LogId`) is tracked as separate functional follow-up, not as a checker
compare pass.

## Readstore and Indexes

The index **registry** is business-visible and is already verified by the
checker: `compareIndexes` re-derives presence and identity from the audited
`CreateIndex`, `DropIndex`, `RemovedMetadataFieldType` (metadata-field-type
removal can cascade-drop an attached index), and `DeleteLedger` (purges every
`SubAttrIndex` entry scoped to the ledger) logs. It deliberately verifies only
registry presence and identity — **not** the contents of the readstore inverted
index.

The readstore inverted index is a **separate, per-replica Pebble store** (opened
independently in `internal/storage/readstore`), **not** written through the FSM
apply path. Each node's index builder (`internal/application/indexbuilder`)
populates it **locally** from the replicated log/audit stream, and
`IndexVersionState.CurrentVersion` (readiness) is explicitly per-replica. It is
therefore a **peer secondary store, out of the main-store checker's scope by
construction** (invariant #8) — the same category as the `usagestore` counter
cache. `Check()` opens and walks the main store; it never opens the readstore, so
`compareIndexes` deliberately verifies only the registry (which lives in the main
store) and not the inverted-index contents (which do not).

Once a per-replica index is READY (`IndexVersionState.CurrentVersion` non-zero)
the query compiler in `internal/query/compile.go` compiles reference, metadata,
address, asset, timestamp, and related filters **directly** to readstore
iterators via `requireIndexReady`, with **no on-scan fallback**. So the index
decides which rows an API caller gets back — which makes its integrity a real
concern, but a *per-replica read-model* concern, not a *main-store
audit-vs-store* one. The integrity contract is the peer-store contract: the
inverted index is a pure derived projection of the audit chain (the
cryptographically-verified source of truth), so corruption is repaired by
dropping and re-indexing from the audit, not by a main-store compare pass.
Because Raft replicates the *source* audit/log stream and not the readstore
bytes, a bit-flip in one replica's index is local and not propagated to the
others; detecting and repairing it is the index builder's **rebuild-health**
responsibility (re-derive / verify a READY index against the audit before or
while it is served), tracked under `EN-1323`.

This is deliberately **not** folded into `compareIndexes` or any other main-store
checker pass. The checker's mandate is the authoritative main store; the readstore
is a rebuildable peer read-model reached only through its own recovery path.
Extending audit-derived verification to per-replica index contents belongs to the
indexbuilder's rebuild-health effort (`EN-1323`), not to invariant #8.

A note on what cross-replica identity actually means for main-store projections,
so the contrast is precise. Raft plus the deterministic FSM guarantee that every
replica applies the **same logical proposal** and reaches the **same logical
state** — they do **not** guarantee byte-identical Pebble serialization. Several
projections are marshalled **non-deterministically** on purpose: any
map-bearing message serializes in Go's randomized map-iteration order, so the
same proposal can persist byte-divergent values on different replicas.
`AppliedProposal.TransientVolumes` documents this explicitly (`appendAppliedProposal`
in `internal/infra/state/batch.go`: "two nodes will persist byte-divergent
Cold-zone entries for the same proposal"), as do `Log` metadata maps and
`saveIdempotencyKey` ("marshal order need not match across replicas"). Only the
hash carrier is locked down: `AuditEntry` is marshalled via
`MarshalDeterministicVT` (sorted map keys) precisely because it feeds the audit
hash chain, and the sealer re-marshals deterministically for the cross-node state
hash. This is why the checker must compare projections **logically** (decode and
compare, or replay from the audit) rather than byte-wise — byte-equality is not a
valid cross-replica integrity assumption for map-bearing main-store values, only
for the deterministic audit stream.

## Prepared Queries

Prepared queries are persisted, business-visible state: they are named query
definitions stored under `SubAttrPreparedQuery`, cached and bloom-filtered like
other cache attributes, and they shape which business data an API caller gets
back. They are created through an audited order (`CreatePreparedQuery`), so the
audit source of truth exists.

The stored projection is **already authoritative for business-visible responses
today** — this is a current integrity gap, not a future transition.
`ExecutePreparedQuery` reads the stored definition via `ReadPreparedQuery`
(`internal/query/prepared_query.go`) straight from Pebble
(`internal/query/executor.go`) and compiles that stored filter to produce the
rows an API caller gets back. A tampered `SubAttrPreparedQuery` value therefore
changes query results right now.

The stored projection is **not verified by the checker**: there is no `compare*`
pass for prepared queries in `internal/application/check`. Because the projection
is authoritative today, closing this gap requires a checker pass that re-derives
the expected prepared-query set from the audited create/update/delete logs and
compares it to what is stored — not a deferred "when it becomes authoritative"
step. Until that pass exists, treat prepared-query integrity as a tracked gap
rather than a proven guarantee.

## Idempotency Eviction

Idempotency outcomes are business-visible during their configured retention
window and must be checked against the audited success/failure reason that froze
them. Eviction after TTL is technical policy: the leader scans expired keys,
embeds the cutoff and key hashes in the proposal, and the FSM applies that
proposal deterministically.

If product semantics ever change to "idempotency forever", eviction stops being
purely technical and must be redesigned as business-impacting state.

## Cluster Configuration

Cluster configuration is Raft-replicated technical state. Most fields tune
runtime behavior, such as cache rotation and bloom configuration. `hash_algorithm`
is more sensitive: it changes how future audit entries are hashed and therefore
changes the trust model for business evidence.

Changing the hash algorithm does not rewrite historical business truth, but it is
governance-significant. The persisted cluster-config row is not merely advisory:
`LoadFSMStateFromStore` (`internal/infra/state/fsmstate.go`) reads
`SubGlobClusterConfig` back on recovery and rebuilds `FSMState.HashGenerator` from
the stored `hash_algorithm` (`processing.NewHashGenerator(...)`), and that same
`HashGenerator` is what `Machine` uses to compute the audit-chain hash for every
future entry (`internal/infra/state/machine.go`). A persisted row tampered while
it sits on disk therefore silently changes how future audit entries are hashed —
i.e. it shifts the trust model for the only cryptographically-bound dataset.

No `compare*` pass in `internal/application/check` re-derives `SubGlobClusterConfig`
against an audit-bound source, and there is no audit-bound read-back of the stored
`hash_algorithm`, so this is a **tracked integrity gap**, not an approved
exemption — the same status as the other uncovered projections above. An operator
trail alone is weaker than the controls this page mandates for state that shapes
business evidence: closing the gap requires either modeling the hash-algorithm
change as an audited governance order (so the stored value can be read back from
an audit-bound source) or a checker pass that verifies the persisted row against
that source.

## Backup Destination Identity

Backup job state is operational. The canonical destination key intentionally
excludes `base_path` while `base_path` is reserved and ignored by backup runners.
If `base_path` starts affecting the physical destination, update destination
identity at the same time. Otherwise the FSM-level backup mutex can protect the
wrong unit of work.

## Review Checklist

When reviewing a new persisted state or update path, require a short answer for
each item:

- What user-visible behavior changes if the value is wrong?
- Is the behavior business truth, governance truth, operational progress, or a
  rebuildable projection?
- If it is business truth, where is the audit entry?
- If it is a projection, which checker pass verifies it?
- If it is operational, can corrupting it skip or suppress future business
  proposals?
- If a future feature reuses this state, what would make the classification
  change?
