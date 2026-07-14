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
| Rebuildable local projection | Speeds reads or background work and can be regenerated from audit-bound data. **A projection only belongs here if it is genuinely thrown away and rebuilt** — not merely rebuildable in principle. | Bloom filters (discarded and rebuilt on the backup/restore path — `internal/infra/attributes/prepare.go` deletes the persisted blocks so restore rebuilds them from a full attribute scan — and on a bloom-config change applied via cluster config, where `applyClusterConfigUpdate` (`internal/infra/state/machine_technical_updates.go`) purges the `SubGlobBloom` blocks, calls `BloomFilters.Rebuild`, and signals `StartAsyncBloomPopulate`; note that on normal restart / follower sync they are instead *restored from the persisted Pebble blocks*, so those blocks are durably trusted between backups — see the floor note below), cache mirrors, snapshots/spool. | Rebuild path is the control **when the projection is actually rebuilt as part of a lifecycle path**. A *durably persisted* projection that is trusted between rebuilds and can shape a business-visible result (e.g. readstore inverted indexes while a READY index is authoritative, or persisted bloom blocks reloaded on restart — see below) additionally needs checker coverage, because nothing rebuilds it before it is served. |

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
(the mirror cursor, the readstore inverted-index contents, prepared queries,
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

That atomicity protects normal failures, but it does not by itself prove that the
stored cursor is still consistent with audited mirror orders after restore,
manual repair, or future tooling. The cursor is wrong in **both** directions:

- A cursor that advances **beyond** the highest audited `MirrorIngest.v2LogId`
  makes the worker skip source logs without any audit entry showing the missing
  proposals — silent under-ingestion.
- A cursor that drops **below** the highest audited `v2LogId` makes
  `Worker.processBatch` (`internal/application/mirror/worker.go`) reload the
  cursor and call `FetchLogs(cursor, ...)`, which returns source logs strictly
  *after* the cursor. `TranslateBatch` then re-emits `MirrorIngest` orders for
  logs that were already ingested. **The FSM now enforces a contiguous applied
  prefix (EN-1550).** `processMirrorIngest`
  (`internal/domain/processing/processor_mirror.go`) records the highest applied
  source id in `LedgerBoundaries.last_mirror_v2_log_id` — a true contiguous
  prefix, since the worker ingests contiguously including `FillGap` orders — and,
  *before* applying any posting or mutating any state, first rejects a malformed
  `v2LogId == 0` (source v2 log ids are 1-based, so 0 is tamper/corruption) with
  `ErrMirrorV2LogIDInvalid` (`KindInternal`) — it is never applied, since a 0 is
  never recorded as `last` and a silently-applied 0 would be re-appliable forever.
  It then decides three ways against the next slot (`expected = last + 1`):
  - `v2LogId <= last`: already applied. Idempotent **no-op** — return `(nil, nil)`,
    which `ProcessOrders` treats as "no log" (no sequence id consumed, no
    audit-visible `Log`, no sink absorb). Postings are not re-forced, so balances
    cannot double (pre-EN-1550 this replay caused silent **double-application** via
    `force=true` additive volume mutation with no dedup on `v2LogId`).
  - `v2LogId == expected`: the next contiguous log. Apply and advance `last`.
  - `v2LogId > expected`: a **gap** — the cursor/high-water mark is ahead of the
    applied prefix. Impossible under contiguous ingestion, so it is
    corruption/tampering. The FSM **fails loud** (`ErrMirrorV2LogIDGap`,
    `KindInternal`) and mutates nothing — it never silently applies past the gap
    (which would desync nodes) or skips it. The rejection is deterministic (same
    input → same rejection on every node); the worker surfaces it as a repeating
    apply error rather than corrupting state.

  The guard is a pure function of applied per-ledger state; v2 log ids are 1-based
  and strictly increasing per source (`TranslateBatch`), so it is FSM-deterministic
  and needs no new preload/coverage key (it lives inside the already-covered
  boundaries). Only the **behind** direction is handled here; recovering a cursor
  that is *ahead* of the true source head is worker-side source-head recovery and
  remains out of scope.

Because it is only correct at one exact value, treat `MirrorCursor` as a
persisted per-ledger projection (`ZonePerLedger` / `SubPLMirrorCursor`, in the
main Pebble store) that must **equal** the highest audited `MirrorIngest.v2LogId`
for the target ledger — not merely be less than or equal to it. Explicit edge
cases:

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

This equality is **not** currently enforced: there is no `compare*` pass for
`MirrorCursor` in `internal/application/check`, so per invariant #8 this is a
known integrity gap, not an approved exemption. Raft replication does not close
it — a cursor value corrupted or tampered before it is proposed takes effect on
every node identically. Mirror recovery, repair tooling, or a checker pass
that re-derives the expected cursor from the audited mirror-ingest logs must
enforce the equality before the stored cursor can be trusted.

`LedgerBoundaries.last_mirror_v2_log_id` (the EN-1550 idempotency high-water
mark) **is** covered by the checker as a full invariant-#8 **equality** check.
`compareMirrorV2LogID` (`internal/application/check/checker.go`) verifies, per
ledger, that the stored `last_mirror_v2_log_id` **equals** `max(audited
MirrorIngest.v2_log_id)`, emitting `CHECK_STORE_ERROR_TYPE_MIRROR_V2LOGID_MISMATCH`
on **any** divergence. Because the FSM enforces a contiguous applied prefix (the
gap reject above), the persisted high-water mark must be exactly the max audited
`v2_log_id` at rest — no more, no less:
- `stored > max`: claims a source v2 log no audit entry recorded (future ingests
  wrongly skipped).
- `stored < max`: the projection lost applied ground (an already-applied ingest
  would be re-applied).

The audited max is derived from the live audit chain
(`recordMirrorIngestMutations`) layered over a baseline floor
(`foldBaselineBoundaries` seeds it from the archived
`LedgerBoundaries.last_mirror_v2_log_id`, which the compact baseline snapshot now
carries — `writeBaselineAttributes` includes `Boundary` rows), so a ledger whose
mirror ingests live only in an archived chapter is not undercounted. A regular
(never-mirrored) ledger has stored `0` and audited max `0` → equal → not flagged.
The comparison is driven from the **union** of stored boundary rows and
audited-mirror ledgers, so a mirror ledger with audited ingests but no stored row
(audited max > 0, row absent) is treated as stored `0` and flagged — an absent
`last_mirror_v2_log_id` for an audited *active* mirror ledger is caught, not
skipped. The absent-row check is suppressed only for a ledger audited as
**deleted** (a `DeleteLedger` log replayed in the verified range): `WriteSet.Absorb`
removes the boundary row on deletion, so its missing row is legitimate, not
corruption. Present-row equality still applies to every ledger.

There is **no** legacy / no-backfill leniency: existing pre-field clusters are
**unsupported** — we do not ship a compat shim, and a store that predates
`last_mirror_v2_log_id` would fail this equality (and simply re-mirror), which is
acceptable pre-GA.

## Readstore and Indexes

The index **registry** is business-visible and is already verified by the
checker: `compareIndexes` re-derives presence and identity from the audited
`CreateIndex`, `DropIndex`, `RemovedMetadataFieldType` (metadata-field-type
removal can cascade-drop an attached index), and `DeleteLedger` (purges every
`SubAttrIndex` entry scoped to the ledger) logs. It deliberately verifies only
registry presence and identity — **not** the contents of the readstore inverted
index.

The readstore inverted index is rebuildable in principle, but it is **already
authoritative for business-visible query results today** — this is a current
integrity gap, not a future transition. Once the per-replica
`IndexVersionState.CurrentVersion` for an index is non-zero (READY), the query
compiler in `internal/query/compile.go` compiles reference, metadata, address,
asset, timestamp, and related filters **directly** to readstore iterators via
`requireIndexReady`, with **no fallback** to scanning attributes or replaying the
audit (`compile.go` even documents "there is no on-scan fallback" for the asset
builtin). A corrupted or tampered READY index therefore decides which rows an API
caller gets back, while `compareIndexes` never re-derives those inverted-index
entries from the audit. Between rebuilds nothing regenerates the index, so the
rebuild path is not a control here.

This is a tracked gap (see the `compareIndexes` comments referencing the index
content-verification effort, and `EN-1323` on the version/build-status split).
Closing it requires the checker to re-derive the readstore inverted-index
contents from the audited logs, or a rebuild-health mechanism that proves the
projection matches the audit before a READY index is served.

Raft does not help here, but the reason differs from the main-store projections
above. The readstore is a **separate, per-replica Pebble store** (opened
independently in `internal/storage/readstore`), not written through the FSM apply
path at all. It is populated **locally** by each node's index builder
(`internal/application/indexbuilder`) from the replicated log/audit stream, and
`IndexVersionState.CurrentVersion` (readiness) is explicitly per-replica. Raft
replicates the *source* audit/log stream, not the readstore bytes, so a bit-flip
in one replica's readstore is **not** propagated to the others. Consequently Raft
neither detects nor repairs replica-local index corruption: each replica can
serve a differently-corrupted (or healthy) index, and only per-replica checker
or rebuild-health validation can catch it.

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
