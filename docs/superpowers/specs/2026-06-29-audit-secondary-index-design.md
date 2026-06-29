# Async secondary index over the Audit zone — Design (EN-1339)

- **Ticket**: [EN-1339](https://formance-team.atlassian.net/browse/EN-1339) — Build async secondary index over audit zone
- **Blocks**: EN-1305 (audit `ListOptions.filter`), PR #558
- **Depends on (merged)**: PR #453 (bucket-scoped Index registry), PR #553 (per-replica versioned forward index)
- **Pattern reference**: `internal/application/indexbuilder/`
- **Invariant context**: CLAUDE.md #3, #4, #8

## Problem

`ListAuditEntries` cannot filter the audit log the way every other list endpoint can.
Audit reads today (`internal/query/audit.go`) are sequence-ordered full scans accepting
only an `afterSequence` cursor — no field filtering. PR #558 attempted to honor
`ListOptions.filter` by compiling a scan-time predicate over the entire Cold/Audit zone:
functionally correct but an unbounded, user-facing linear scan, made worse by the
`order_type` predicate forcing a per-entry `AuditItem` load + `SerializedOrder` unmarshal
inside the scan loop.

Every other list endpoint (`ListTransactions`, `ListAccounts`, `ListLogs`) instead compiles
the filter through `query.Compile` into an `EntityIterator` that *seeks* a secondary index.
The gap: there is no secondary index over the Audit zone for audit listing to seek. This
ticket builds and maintains that index (and its rebuild machinery); EN-1305 later wires the
reader through `query.Compile`.

## Goal

Maintain a persisted, per-replica secondary index over the Audit zone that supports
seek-by-field (equality, match-any, range) for the fields callers filter on, built by an
async worker, with **no change to the Raft hot path, preload, `WriteSession`, or the
checker**.

## Key decisions

### D1 — Standalone always-on worker (not the bucket-scoped Index registry)

A new dedicated worker, **not** an entry in the bucket-scoped `Index` registry and **not**
folded into `indexbuilder.Builder`.

Rationale:
- Registering a `commonpb.Index` lives in the FSM cache (`Scope.Indexes()`, persisted under
  `SubAttrIndex`) and is written by the Raft apply path (`processCreateIndex`). Using it would
  be an **FSM-path change**, violating the ticket's "no FSM apply / preload / WriteSession
  change" criterion, and would pull the audit index into `checker.compareIndexes`
  (invariant #8) — which the ticket explicitly forbids.
- The audit index fields are **fixed and built-in**; there is nothing for a user to
  create/drop, so the registry's `BUILDING → READY` lifecycle and `CreateIndex/DropIndex`
  proposals buy nothing.
- Different producer than `Builder`: `Builder` tails the **Log** zone with a Log cursor; this
  worker tails the **Audit** zone with an audit-sequence cursor and decodes `AuditEntry`.
  A separate worker keeps a single responsibility (`builder.go`/`process_logs.go`/`backfill.go`
  are already very large).

### D2 — Storage home: `readstore` (deviates from the ticket's literal "ZoneCold sub-zone")

The index lives in the existing **`readstore`** Pebble (`internal/storage/readstore/`) — a
separate per-replica database described in-code as "a derived view that can be rebuilt from
the Raft log."

This is a **deliberate deviation** from the acceptance-criterion wording ("New audit-index
sub-zone defined in `internal/storage/dal/` … forbidigo exclusions justified if needed"),
which points at the Raft-replicated main store's `ZoneCold`. Reasons the main store is the
wrong home:
- **Invariant #4 / concurrency**: the main store is written only by the FSM apply path plus a
  few non-concurrent declared lifecycle paths. An async worker writing to it continuously,
  concurrently with steady-state applies, is a new and risky concurrency model on the
  replicated store.
- **Snapshot bloat / coupling**: Raft snapshots are full Pebble checkpoints of the main store
  (`createMainStoreCheckpoint`, `internal/infra/node/applier.go`). Anything in `ZoneCold`
  ships in every `InstallSnapshot` and couples to restore/cold-archival.
- **Precedent + fit**: the existing forward index already lives in `readstore`, already holds
  non-ledger-scoped keys (Log cursor, applied-proposal progress, index-version states), and is
  where the `EntityIterator` machinery EN-1305 will reuse already lives.

The audit-index keys still use `dal`-style key builders; only the physical Pebble differs.

### D3 — Purge coupling: tolerate dangling, reclaim on rebuild

Audit entries are archived + purged per chapter (`Archiver` + `IterateColdKVPairs`, sequence-
range delete). The index keys are `(field, value, seq)` — not sequence-prefixed — so the
sequence-range purge cannot reach them. Per the ticket's reader design, the index entries are
**not** actively deleted on purge: the (EN-1305) reader re-reads each `AuditEntry` by sequence
and skips on `ErrNotFound`. Dangling keys are harmless (audit sequences are monotonic and never
reused) and are reclaimed wholesale by the trivial drop + rebuild. No checker pass, no lockstep
purge scan.

## Architecture

```
                main dal.Store (Raft-replicated)              readstore (per-replica, derived)
   FSM apply ──► [ZoneCold][SubColdAudit][seq] AuditEntry      ┌──────────────────────────────┐
   (unchanged)  [ZoneCold][SubColdAuditItem][seq][idx] Item    │ [auditIdxPrefix][fc][val][seq]│
                          │  (read-only, off hot path)         │ last_indexed_audit_sequence   │
                          ▼                                     └──────────────────────────────┘
              ┌───────────────────────────┐  write index keys           ▲
              │  auditindexer.Indexer      │ ───────────────────────────┘
              │  (worker on all nodes)     │
              └───────────────────────────┘
```

### Component: `internal/application/auditindexer/`

- `Indexer` struct, modeled on `indexbuilder.Builder`:
  - Holds `*dal.Store` (read), `*readstore.Store` (write + cursor), logger, meter, batch size,
    rebuild threshold, a `worker.Worker`, and an atomic last-indexed-sequence gauge.
  - `Start()` / `Stop()` lifecycle; `Start` launches the background loop and registers OTEL
    gauges (`audit_index.last_indexed_sequence`, `audit_index.audit_last_sequence`,
    `audit_index.lag`).
- Loop (mirrors `Builder.loop`):
  1. Read cursor from `readstore`. If missing, or `lastAuditSeq - cursor > rebuildThreshold`,
     drop the keyspace + reset cursor to 0 (auto-rebuild).
  2. Initial time-bounded catch-up from the cursor to the last audit sequence.
  3. Steady state: wake on a ~100ms ticker (and the existing `LogCommitted` notification as a
     hint), process new entries in batches, advance + persist the cursor per committed batch.
     The ticker — not the log signal — is what guarantees failure-only proposals (which advance
     the audit sequence without producing a log) get indexed. Lag is eventual, as the ticket
     accepts.

### Indexing a single `AuditEntry`

For each entry in `(cursor, lastAuditSeq]`:
1. Read the `AuditEntry` header from `[ZoneCold][SubColdAudit][seq]`.
2. Read its `AuditItem`s from `[ZoneCold][SubColdAuditItem][seq][*]` (for `order_type` and
   `log_seq`).
3. Emit index keys (below) into the batch. Value is always empty.
4. Advance the cursor to `seq`.

## Storage: key layout (`readstore`)

New constants in `internal/storage/readstore/keys.go`. One keyspace, discriminated by a
field-code byte; empty values.

```
[auditIdxPrefix][field_code byte][encoded_value][audit_seq BE8]  ->  ∅
```

| `field_code` | source | value encoding | query semantics |
|---|---|---|---|
| `outcome`        | `AuditEntry.Outcome` oneof          | 1 byte: `0`=failure, `1`=success         | equality |
| `ledger`         | `AuditEntry.Ledgers[]`              | UTF-8 + `0x00` terminator                 | match-any (one key per name) |
| `caller_subject` | `CallerSnapshot.Identity.Subject`   | UTF-8 + `0x00` terminator                 | equality (skip when nil) |
| `order_type`     | `AuditItem.SerializedOrder` → token | token + `0x00` terminator                 | match-any (one key per distinct token) |
| `timestamp`      | `AuditEntry.Timestamp`              | BE `uint64` unix nanos                    | range |
| `proposal_id`    | `AuditEntry.ProposalId`             | BE `uint64`                               | range |
| `log_seq`        | `AuditItem.LogSequence` (`> 0`)     | BE `uint64`                               | range, match-any (one key per item) |

A seek to `[auditIdxPrefix][field_code][value]` plus a range scan yields the matching
`audit_seq`s directly. Big-endian numeric encoding makes range fields sort correctly; the
`0x00` terminator on string fields makes the value boundary unambiguous before the fixed
8-byte trailing sequence.

`sequence` itself gets no entry: it is already the primary key of the Audit zone.
`error_type`, `caller.scope`, `caller.god` are deferred (the worker is uniform across fields;
adding them later is additive).

### Comparer interaction (implementation risk to verify)

`ReadStoreComparer` splits keys at the `[prefix][ledger\x00]` boundary so bloom filters key on
ledger-scoped prefixes. The audit-index prefix must be confirmed to be treated as
**non-splittable** by the comparer — the same way the existing non-ledger-scoped readstore keys
(progress cursor, index-version states) already coexist. If the comparer's `Split` returns the
full key for unrecognized prefixes, no change is needed; otherwise the comparer must learn the
audit-index prefix. This is verified first in implementation (a comparer unit test over the new
keys), before building the worker on top.

## Order-type tokens (cross-ticket coupling)

Derive a stable string token from the `Order` oneof
(`Order_LedgerScoped`/`Order_SystemScoped` → `LedgerScopedOrder_Apply` →
`LedgerApplyOrder_CreateTransaction`, etc.). Define the token vocabulary **once** in a shared
`domain` helper (e.g. `domain.AuditOrderType(order) string`) so EN-1305's filter DSL filters on
the identical tokens. The worker and the (future) reader both reference this single helper; the
vocabulary must not be duplicated.

## Read surface (this ticket) vs reader wiring (EN-1305)

This ticket ships **seek+range index helpers** over the new keyspace — functions that, given a
field + value (or range), return matching audit sequences — and unit-tests them. It does **not**
wire `ListAuditEntries` or extend `query.Compile`: that `EntityIterator` integration is EN-1305
and is explicitly out of scope here. The helpers are shaped so EN-1305 can build an
`EntityIterator` on top without reworking the key layout.

## Rebuild path

- **ledgerctl** (modeled on `cmd/ledgerctl/store/rebuild_indexes.go`): a command that opens the
  stores, range-deletes the audit-index keyspace in `readstore`, resets the cursor to 0, and
  replays from the earliest surviving audit sequence to produce a complete index.
- **Auto on boot**: the worker's startup checks the cursor; if it is missing **or**
  `lastAuditSeq - cursor` exceeds `--audit-index-rebuild-threshold`, it drops + resets before
  catching up (rather than an incremental catch-up over a huge gap).

A drop + rebuild yields a byte-identical index to one built incrementally (determinism is
covered by an integration test).

## Configuration (server flags)

- `--audit-index-batch-size` — entries per Pebble batch commit (default mirrors
  `indexbuilder.DefaultBatchSize`).
- `--audit-index-rebuild-threshold` — cursor-gap threshold that triggers a boot-time drop +
  rebuild.
- `--disable-audit-index` — ops kill switch; default enabled.

## fx wiring

- Provide `*auditindexer.Indexer` from `(*dal.Store, *readstore.Store, logging.Logger,
  metric.MeterProvider, Config)` in `internal/bootstrap/module.go`, alongside the existing
  `indexbuilder.Builder` provider.
- Register `Start`/`Stop` on the fx lifecycle, mirroring the `Builder` lifecycle hook.

## Invariants honored

- **#3 / #4 (hot path)**: no FSM apply / preload / `WriteSession` change. The worker reads the
  audit zone off the hot path and writes only to `readstore` — no `OpenWriteSession` on the main
  store, so no new `forbidigo` exclusion is needed.
- **#8 (audit is source of truth)**: the index is a pure access path with empty values — no
  payload denormalization, not hash-bound, not a projection. **No change to
  `internal/application/check/checker.go`.** Corruption is recoverable by drop + rebuild.

## Testing

- **Unit (table-driven)**:
  - `readstore` comparer over audit-index keys (verifies §"Comparer interaction").
  - Key encoding/ordering per field (range fields sort correctly; string terminators).
  - Field extraction from `AuditEntry`/`AuditItem`: multi-ledger match-any, multi-order distinct
    `order_type`, failure entries, nil `CallerSnapshot`, items with `LogSequence == 0` skipped.
  - Order-type token mapping for each `Order` oneof variant.
- **Integration**:
  - Index catches up after FSM apply.
  - Lag stays bounded under sustained load.
  - Rebuild-from-scratch yields a byte-identical index to the incrementally built one.
  - Restart resumes from the persisted cursor (no re-indexing from 0, no gaps).

## Acceptance-criteria mapping

| AC | Covered by |
|---|---|
| New audit-index keyspace (key layout documented) | §Storage (in `readstore`, per D2; documented above) |
| Async indexer worker following the Audit zone via persisted cursor | §Architecture / `auditindexer.Indexer` |
| Rebuild path via `ledgerctl` + auto on boot when cursor missing/behind threshold | §Rebuild path |
| All listed fields indexed with seek + range semantics | §Storage key layout |
| Integration test (catch-up, lag, rebuild parity, restart resume) | §Testing |
| No change to `checker.go` | D3 + §Invariants honored |
| No change to FSM apply / preload / `WriteSession` | D1, D2 + §Invariants honored |

## Out of scope

- `ListAuditEntries` / `query.Compile` reader wiring (EN-1305).
- Payload denormalization in index values (would make it a projection → checker requirement).
- `error_type`, `caller.scope`, `caller.god` fields (additive later).
