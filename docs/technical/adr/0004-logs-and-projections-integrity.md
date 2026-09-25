# 0004 — Logs and projections integrity

**Status:** Proposed (2026-09-25). EN-2341 records the Ledger-local mechanics for logs
and read-side projections. It does not redefine the cross-service guarantees in EN-2339 or
the reusable audit-chain pattern in [Formance RFC-0018](https://github.com/formancehq/internal-rfcs/blob/main/rfcs/0018-formance-audit-chain.md).

## Context and requirement

Ledger v3 separates authoritative business history from replica-local read state. The FSM
commits the main store and audit chain; background builders consume committed history and
write separate read stores. Those projections must be restartable and rebuildable without
being mistaken for business truth.

This ADR makes the local mechanics and restore classification explicit. It follows the
product-to-technical traceability contract: the observable requirement is logical parity
after replay, not byte identity of a discarded local projection.

## Decision

1. **Logs and audit history are authoritative inputs.** The audit chain, audit items,
   committed logs and applied-proposal history are retained/exported according to the
   backup contract. They are the evidence used to rebuild local projections.
2. **Peer read stores are derived, local and rebuildable.** They are not a second source of
   business truth and are classified as discarded/rebuilt local state during restore.
3. **The index builder consumes committed history asynchronously.** It uses its own read
   handle, retries transient failures, and enters a terminal not-serving state on an
   invariant failure. A two-pass batch writes projection rows and advances the progress
   cursor atomically; progress must never move ahead of data.
4. **Crash and schema recovery are cursor-driven.** Pending-version/backfill cursors resume
   work after restart. Schema rewrites use explicit version transitions, and replicas
   rebuild logically equivalent state independently; there is no cluster-wide IndexReady.
5. **Checker scope is explicit.** The checker validates primary audit-bound state and the
   projection registry. It may check reverse-map row presence, but per-replica index
   contents and `IndexVersionState` are not currently full integrity guarantees. The open
   content-integrity gaps tracked by EN-1514 and EN-1323 remain visible rather than being
   hidden behind a stronger claim.
6. **Restore parity is logical.** For every projection, the restore classification records
   whether it is preserved, rebuilt or discarded, its authoritative evidence, fold rule,
   checkpoint seed and delete/recreate behavior. The required contract is
   `live(prefix + delta) == restore(checkpoint, delta)` in logical state.

## Mechanics in scope

- committed-log append and background consumption;
- read-store keyspaces, cursors, pending versions and atomic progress;
- backfill, range purge, restart and transient/terminal failure handling;
- registry restore and projection rebuild;
- checker comparisons and the reverse-map exception;
- incremental-backup classification and restore evidence.

Query consistency, read-your-own-write, projection certificates and inter-service guarantees
are deliberately out of scope here; they belong to the global architecture RFC and the
read-path contracts. Cryptographic audit-chain authority and tamper semantics belong to
Formance RFC-0018.

## Consequences

This design allows a damaged or missing peer read store to be dropped and rebuilt without
rewriting business history. It also means projection lag, rebuild duration and some index
content corruption are operational concerns that require observability and explicit response.
Replica-local readiness must not be presented as a cluster-wide integrity verdict.

## Alternatives considered

- Treat every read store as authoritative: rejected because replica-local state is
  asynchronous and rebuildable.
- Advance the cursor before projection writes: rejected because a crash would permanently
  skip committed history.
- Promise checker verification of all index values: rejected because current checker coverage
  does not validate all inverted-index contents; EN-1514/EN-1323 remain follow-up work.
- Preserve every peer projection in backups: rejected because it couples restore correctness
  to local derived bytes instead of authoritative history.

## Validation and follow-up

- [ ] Add observability for projection lag, cursor-ahead/behind states, rebuild duration and
  terminal index-builder failures.
- [ ] Define ownership and remediation for inverted-index content integrity (EN-1514,
  EN-1323).
- [ ] Keep restore tests proving non-empty post-checkpoint deltas for projections that are
  rebuilt.
- [ ] Link any future projection integrity checker to the audit-chain RFC and update this
  ADR if its scope changes.

## Evidence

- [Architecture overview](../architecture/overview.md)
- [Audit versus technical state](../architecture/audit-vs-technical-state.md)
- [Audit chain](../architecture/subsystems/checker/audit-chain.md)
- [Checker](../architecture/subsystems/checker/checker.md)
- [Indexer](../architecture/subsystems/indexer/indexer.md)
- [Indexes](../architecture/subsystems/indexer/indexes.md)
- [Incremental restore contract](../architecture/subsystems/backup/incremental-restore-contract.md)
- Jira: [EN-2341](https://formance-team.atlassian.net/browse/EN-2341)
