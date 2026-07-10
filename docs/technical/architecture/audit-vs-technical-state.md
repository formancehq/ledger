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
| Operational consensus state | Coordinates background work or external delivery, but does not itself define ledger business truth. | Event sink cursors/status, backup job state, removed-member registry, mirror status/source-head. | Raft replication keeps replicas identical; it is sufficient **only** when a corrupted-but-identically-replicated value cannot silently change a business result. Raft cannot detect a value that is tampered or corrupted before it is proposed and then replicated identically everywhere — any such projection needs checker coverage regardless of replication. |
| Rebuildable local projection | Speeds reads or background work and can be regenerated from audit-bound data. **A projection only belongs here if it is genuinely thrown away and rebuilt** — not merely rebuildable in principle. | Bloom filters (dropped on every backup/restore and rebuilt from a full attribute scan, see `internal/infra/attributes/prepare.go`), cache mirrors, snapshots/spool. | Rebuild path is the control **when the projection is actually rebuilt as part of a lifecycle path**. A *durably persisted* projection that is trusted between rebuilds and can shape a business-visible result (e.g. readstore inverted indexes while a READY index is authoritative — see below) additionally needs checker coverage, because nothing rebuilds it before it is served. |

The boundary is about impact, not package location. A value under a technical
keyspace can become business-impacting if future code starts using it to decide
whether a business command is accepted or which business data is returned.

Invariant #8 ([AGENTS.md](../../../AGENTS.md)) is the floor: every non-audit
dataset persisted in the main Pebble store is a projection that the checker must
verify, unless it is genuinely discarded and rebuilt by a lifecycle path (bloom
filters) or lives in a separate rebuildable side-store outside the checker's
scope. Raft replication is **not** a substitute for checker coverage: it only
guarantees that every replica holds the *same* bytes, so a value that is
corrupted or tampered before replication is faithfully copied to every node and
no cross-node comparison can catch it. Where the sections below describe a
persisted main-store projection that the checker does not yet verify, that is a
tracked integrity gap, not an approved exemption.

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
- A cursor that drops **below** the highest audited `v2LogId` is equally unsafe.
  `Worker.processBatch` (`internal/application/mirror/worker.go`) reloads the
  cursor and calls `FetchLogs(cursor, ...)`, which returns source logs strictly
  *after* the cursor. `TranslateBatch` then emits fresh `MirrorIngest` orders for
  logs that were already ingested, and `processMirrorCreatedTransaction`
  (`internal/domain/processing/processor_mirror.go`) reapplies every posting with
  `force=true`, overwriting the transaction state and re-adding the amounts to the
  volume pair via `applyPosting`. There is **no deduplication on `v2LogId`** and
  volume mutation is additive, so a lowered cursor replays already-audited
  transactions and applies their balance effects a second time — silent
  double-application.

Because it is only correct at one exact value, treat `MirrorCursor` as a
persisted per-ledger projection (`ZonePerLedger` / `SubPLMirrorCursor`, in the
main Pebble store) that must **equal** the highest audited `MirrorIngest.v2LogId`
for the target ledger — not merely be less than or equal to it. Explicit edge
cases:

- **Empty / never-ingested ledger:** the cursor is absent and reads back as `0`
  (`query.ReadMirrorCursor` defaults to `0`, and `processBatch` maps `0` to
  `expectedNextLogID == 1`); a ledger with no audited mirror-ingest logs must
  hold cursor `0`.
- **Deleted / archived ledger:** workers stop but the cursor row is **not**
  cleared, so it persists in Pebble; any equality check must treat a
  deleted/archived ledger as out of scope rather than compare a stale cursor
  against an empty audit set.

This equality is **not** currently enforced: there is no `compare*` pass for
`MirrorCursor` in `internal/application/check`, so per invariant #8 this is a
known integrity gap, not an approved exemption. Raft replication does not close
it — a cursor corrupted or tampered before it is proposed is replicated
identically to every node. Mirror recovery, repair tooling, or a checker pass
that re-derives the expected cursor from the audited mirror-ingest logs must
enforce the equality before the stored cursor can be trusted.

## Readstore and Indexes

The index **registry** is business-visible and is already verified by the
checker: `compareIndexes` re-derives presence and identity from audited
create/drop/delete logs. It deliberately verifies only registry presence and
identity — **not** the contents of the readstore inverted index.

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
projection matches the audit before a READY index is served — Raft replication
does not help, since a bit-flip present before replication is copied to every
replica identically.

## Prepared Queries

Prepared queries are persisted, business-visible state: they are named query
definitions stored under `SubAttrPreparedQuery`, cached and bloom-filtered like
other cache attributes, and they shape which business data an API caller gets
back. They are created through an audited order (`CreatePreparedQuery`), so the
audit source of truth exists.

The stored projection is **not yet verified by the checker**: there is no
`compare*` pass for prepared queries in `internal/application/check`. Until such
a pass re-derives the expected prepared-query set from the audited create/update/
delete logs and compares it to what is stored, treat prepared-query integrity as
a known gap rather than a proven guarantee. When a prepared-query projection
becomes authoritative for a business-visible response, add the missing checker
pass before relying on it.

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
governance-significant. Keep that distinction explicit in docs and operations:
either expose a clear operator trail for cluster-config changes or model the
change as an audited governance order.

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
