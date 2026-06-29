# Mirror Worker

## Overview

A **mirror ledger** is a Ledger v3 ledger that **ingests its transactions from an external source** rather than from client API calls. The source is typically a Ledger v2 instance (HTTP API or direct PostgreSQL read), and the mirror worker translates v2 logs into v3 Raft commands, one batch at a time, until the ledger is either promoted to normal mode or deleted.

The mirror exists for migration: it lets a v3 cluster stand up alongside a live v2 system, replay history, and stay in sync until a cutover. It also doubles as a generic "read-only follower of an external source" primitive.

Source: `internal/application/mirror/` (worker + manager) and `internal/adapter/v2/` (source adapters).

## Worker model

One mirror worker runs per mirror ledger, **on the leader only**. The Manager (`internal/application/mirror/manager.go:30-47`) reconciles workers against the current set of mirror ledgers (`ReadMirrorLedgers`) on every leadership change and on relevant Raft commits:

- Ledger created in mirror mode → spin up a worker.
- Ledger promoted, deleted, or mirror config changed → stop the corresponding worker.

Reconciliation is in `Manager.reconcile()` (`manager.go:112-179`).

The Worker (`worker.go:27-167`) is a polling loop:

| Setting | Default | Source |
|---------|---------|--------|
| Batch size | 100 logs | `MirrorSourceConfig.batch_size` |
| Poll interval | 5 s | Worker-local |
| Prefetch | Next batch fetched async while previous one is applying | `worker.go:425-447` |

On startup the worker loads its cursor from Pebble once (`worker.go:254`) and then keeps it in memory; subsequent ticks rely on the in-memory value and persist updates through the FSM's WriteSet.

## Source adapters

`internal/adapter/v2/source.go:6-10` defines the contract:

```go
type Source interface {
    FetchLogs(ctx, afterID, pageSize) (logs, hasMore, error)
}
```

Two concrete implementations:

| Adapter | Mechanism | File |
|---------|-----------|------|
| HTTP | `GET /v2/{ledger}/logs?pageSize=X&after=Y` against a v2 server, OAuth2 credentials supported | `source_http.go:14-88` |
| PostgreSQL | Direct `SELECT` on the v2 `{bucket}.logs` table; the v2 schema is discovered via `_system.ledgers` | `source_postgres.go:17-79` |

Both adapters return v2 log entries in their native shape; translation to v3 orders happens upstream of the source interface.

## The translation layer

`internal/application/mirror/translator.go` (`TranslateBatch`) walks the fetched v2 logs, fills gaps if the source jumped log IDs (e.g. due to deletions or filtered logs on the v2 side), and produces a sequence of v3 `MirrorLogEntry` payloads. Each entry carries the v2 log ID plus a oneof:

```
oneof payload {
    CreatedTransaction
    SavedMetadata
    RevertedTransaction
    DeletedMetadata
    FillGap            // synthesised when a v2 log ID is missing
}
```

`FillGap` is the explicit "we know there's a v2 log here but we have no payload for it" marker — it lets the v3 ledger advance its own logical sequence even when the source skipped one.

## The Raft command

`misc/proto/raft_cmd.proto:200-212` — `MirrorIngestOrder{MirrorLogEntry entry}`. Each order ingests **one** v2 log entry. A batch of 100 fetched logs becomes 100 orders inside one proposal.

The FSM apply path (`internal/infra/state/machine_technical_updates.go:211-241`, `applyMirrorSyncUpdate`) queues three cursor-related projections in the WriteSet, **atomically with the orders**:

- `MirrorCursor` — the highest v2 log ID successfully ingested.
- `MirrorSourceHead` — the latest v2 log count observed (so the controller can report a `FOLLOWING` vs `CATCHING_UP` status).
- `MirrorStatus` — the last error, if any.

Atomicity matters: if any of the orders in the batch fails (e.g. balance mismatch in a translated CreatedTransaction), the whole proposal rolls back and the cursor does **not** advance. The worker will retry the same batch on the next tick.

## Storage layout

`internal/query/mirror.go` — cursor and status keys under the per-ledger zone:

| Key prefix | Content | Read helper |
|------------|---------|-------------|
| `[ZonePerLedger][SubPLMirrorCursor][ledger]` | `uint64` — last v2 log ID applied. | `ReadMirrorCursor` (line 26) |
| `[ZonePerLedger][SubPLMirrorStatus][ledger]` | Persisted last error. | `ReadMirrorStatus` (line 40) |
| `[ZonePerLedger][SubPLMirrorSourceHead][ledger]` | `uint64` — latest count observed from source. | `ReadMirrorSourceHead` (line 54) |

The cursor row is **monotone** by design — even if the source is reconfigured, the cursor never moves backwards.

## Promotion

A mirror ledger can be promoted to a normal ledger via `PromoteLedgerOrder` (`raft_cmd.proto:246`). The FSM emits a `PromotedLedgerLog` (`common.proto:315`), the WriteSet flags `mirrorConfigChanged = true`, and `Manager.reconcile()` stops the worker on the next reconciliation tick.

After promotion, the ledger accepts normal write requests. The cursor and source-head rows are kept for forensic purposes but no longer advance.

## Configuration

Mirror mode is configured at ledger creation:

```protobuf
message CreateLedgerOrder {
  ...
  optional MirrorSourceConfig mirror_source = N;
}

message MirrorSourceConfig {
  string ledger_name = 1;
  oneof type {
    HttpMirrorSourceConfig     http     = 2;
    PostgresMirrorSourceConfig postgres = 3;
  }
  uint32 batch_size = 4;
}
```

A ledger created with `mirror_source` set has `LedgerInfo.mode = MIRROR`, which is what the manager looks at to decide whether to spin up a worker.

## Lifecycle on failure

| Trigger | Worker behaviour |
|---------|------------------|
| Manual delete | Manager stops the worker on reconcile. Cursor / status rows remain in Pebble. |
| Source unreachable | `FetchLogs` returns an error → the worker writes the error into `MirrorStatus` via a small technical-update proposal, then retries with exponential backoff (`worker.go:225-237`). |
| Translation error (e.g. malformed v2 log) | Same path — error persisted, batch is **not** advanced, retried until the operator intervenes or the source heals. |
| Promotion | Manager stops the worker. The cursor row is preserved for audit. |
| Pebble write-stall | The worker pauses (`worker.go:208-217`) until back-pressure clears, then resumes. |

There is no automatic "skip the broken log" mode. Operators investigate, fix the upstream condition, and the worker resumes.

## Performance notes

- **Async prefetch**: the next batch is fetched from the source while the previous batch is still applying through Raft + FSM. This overlaps source latency with consensus latency.
- **Coverage pre-declaration**: the worker pre-computes the per-order `Needs` for the whole batch in one pass (`worker.go:568-630`), so the per-proposal preload work is amortised.
- **Single-writer**: the cursor row is only ever written by the FSM applying a `MirrorIngestOrder`, so there is no contention to manage.

## What the mirror does not do

- **It does not reconcile against v2 hashes.** The worker trusts the source's log content; it does not cross-check that the resulting v3 state has the same balances as v2. That kind of comparison is a future work item (and would require v2 exposing canonical state hashes).
- **It does not run on followers.** Leadership change suspends the worker until the new leader's manager picks it up.
- **It does not act as a generic CDC sink.** The only sources are Ledger v2 instances; arbitrary event streams are the [events](events.md) subsystem's concern.
