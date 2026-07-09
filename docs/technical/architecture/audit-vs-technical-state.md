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
| Business truth | Changes balances, transactions, metadata, reversions, ledger lifecycle, indexes, prepared queries, or any user-visible business decision. | `AuditEntry`, `Log`, `Volume`, `Metadata`, `Transaction`, `Reference`, reversion bitsets, idempotency outcomes, index registry. | The command must produce an audit entry, and persisted projections must be checked by `internal/application/check`. |
| Governance truth | Changes who can write, how writes are accepted, or how business evidence is produced. It may not change balances directly, but it changes the control plane around business truth. | Signing keys, maintenance mode, chapter schedules, hash algorithm selection. | Prefer an audited order. If represented as technical state, document why and expose an operator-visible control trail. |
| Operational consensus state | Coordinates background work or external delivery, but does not itself define ledger business truth. | Event sink cursors/status, backup job state, removed-member registry, mirror status/source-head. | Raft replication is sufficient when corruption cannot silently change business truth. |
| Rebuildable local projection | Speeds reads or background work and can be regenerated from audit-bound data. | Readstore inverted indexes, bloom filters, cache mirrors, snapshots/spool. | Rebuild path plus checker coverage when the projection is persisted and business-visible. |

The boundary is about impact, not package location. A value under a technical
keyspace can become business-impacting if future code starts using it to decide
whether a business command is accepted or which business data is returned.

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
manual repair, or future tooling. A cursor that advances beyond the highest
audited `MirrorIngest.v2LogId` can make the worker skip source logs without any
audit entry showing the missing proposals.

Treat `MirrorCursor` as a technical projection with a business-adjacent effect:
it should never be greater than the maximum v2 log id found in the target
ledger's audited mirror-ingest logs. If mirror recovery, repair tooling, or
stronger checker coverage is added, include this comparison.

## Readstore and Indexes

The index registry is business-visible and is already verified by the checker:
presence and identity are re-derived from audited create/drop/delete logs.

The readstore inverted index is a rebuildable projection. It is not the source of
business truth, but it can affect API responses while it is corrupted or stale.
When a readstore projection becomes authoritative for a business-visible query,
add a checker or rebuild-health mechanism that proves the projection matches the
audited logs before the system treats it as healthy.

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
