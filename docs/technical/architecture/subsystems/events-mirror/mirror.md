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

The Worker (`worker.go:27-175`) is a polling loop:

| Setting | Default | Source |
|---------|---------|--------|
| Batch size | 100 logs | `MirrorSourceConfig.batch_size` |
| Poll interval | 5 s | Worker-local |
| Prefetch | Next batch fetched async while previous one is applying | `worker.go:464-490` |

On startup the worker reads `LedgerBoundaries` from Pebble once, before its first fetch, and takes both its ingestion position (`last_mirror_v2_log_id`) and `NextTransactionId` from it. The value it keeps in memory afterwards is a cache, not an authority: it advances only after both Raft acceptance and successful FSM application, and it is dropped on any batch error so the next tick re-reads the durable boundary. See [Audit-Bound vs Technical State](../../audit-vs-technical-state.md) for why this is the only durable ingestion position.

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

`internal/adapter/v2/translator.go` (`TranslateBatch`) walks the fetched v2 logs, fills gaps if the source jumped log IDs (e.g. due to deletions or filtered logs on the v2 side), and produces a sequence of v3 `MirrorLogEntry` payloads. Each entry carries the v2 log ID plus a oneof:

```
oneof payload {
    CreatedTransaction
    SavedMetadata
    RevertedTransaction
    DeletedMetadata
    FillGap            // synthesised when a v2 log ID is missing
}
```

When `V2Log.Date` is present, the translated entry carries that parsed source
log `date`. Created and reverted transactions require it: the FSM uses the
chain-bound value for the resulting transaction's `insertedAt` and `updatedAt`
and does not substitute the v3 apply time. This preserves the source insertion
chronology during a long-running migration while keeping the transaction's
business `timestamp` as a distinct field. A created or reverted transaction
without a source date is rejected before mutation. Synthetic `FillGap` entries
created for missing source log IDs carry no date because no source row exists.

The resulting transaction, including these dates, is embedded in the persisted
ledger log. Incremental backup exports that log row and `RebuildDelta` replays
it into the restored transaction projection, so a post-checkpoint mirror ingest
keeps the same `insertedAt` and `updatedAt` across a cross-cluster restore.

`FillGap` is the explicit "we know there's a v2 log here but we have no payload for it" marker — it lets the v3 ledger advance its own logical sequence even when the source skipped one.

### CEL rewrite rules

Optional `rewrite_rules` on `MirrorSourceConfig` are CEL rewrite rules applied, in order, to every mirror log entry as v2 logs are translated. They can rename address segments, transform metadata, or drop transactions. See [Mirror CEL rewrite engine](cel-rewrite.md) for the full CEL surface, determinism model, and drop→fill-gap behaviour.

Each rule carries a `scope` (one of `created_transaction`, `reverted_transaction`, `saved_metadata`, `deleted_metadata`, `any_variant`), a `match` CEL predicate typed against that scope, and a list of typed `actions` (rewrite_address, set_metadata, delete_metadata, set_account_metadata, drop, …). Actions available in a rule are constrained by its scope at the wire level. When `stop` is true and the rule matches, no further rules run. The engine is applied once per assembled `MirrorLogEntry` in `TranslateBatch` (`internal/adapter/v2/celrewrite`).

Properties:
- **Pure, deterministic projection** — the source v2 ledger is untouched; rewriting only shapes the v3 orders the leader proposes, so followers apply identical bytes. The CEL environment exposes no non-deterministic function.
- **Wire-level scope safety** — the rule's `scope` oneof restricts which actions can appear inside it: `set_account_metadata` inside a `saved_metadata` rule is impossible to construct because the proto doesn't include that variant in `SavedMetadataAction`.
- **Compile-checked at admission** — rules are validated by `celrewrite.NewRewriter` via `ErrMirrorRewriteRuleInvalid` before the config is persisted; at translation time a rewrite that produces an invalid address fails the batch, so the cursor does not advance and the worker retries (the standard translation-error path).
- **Drop preserves IDs** — a `drop` action emits a `FillGap` carrying the dropped transaction ID in `skipped_transaction_ids`, so log-ID contiguity and transaction-ID advancement are both preserved.

## The Raft command

`misc/proto/raft_cmd.proto:200-212` — `MirrorIngestOrder{MirrorLogEntry entry}`. Each order ingests **one** v2 log entry. A batch of 100 fetched logs becomes 100 orders inside one proposal.

The ingestion position itself is advanced by the order-apply path (`processMirrorIngest` writes `LedgerBoundaries.last_mirror_v2_log_id`), not by the technical update. `applyMirrorSyncUpdate` (`internal/infra/state/machine_technical_updates.go`) queues only the two reporting projections in the WriteSet, **atomically with the orders**:

- `MirrorSourceHead` — the latest v2 log count observed (so the controller can report a `FOLLOWING` vs `CATCHING_UP` status).
- `MirrorStatus` — the last error, if any.

Atomicity matters: if any of the orders in the batch fails (e.g. balance mismatch in a translated CreatedTransaction), the whole proposal rolls back and the boundary does **not** advance. The worker will retry the same batch on the next tick.

When the worker is fully caught up it fetches no logs, so the ingest path never runs — and that path is what normally carries both the source head and the error clear. The caught-up worker therefore publishes them on its own via a standalone `MirrorSyncUpdate` (`Worker.publishIdleStatus`). Two separate conditions trigger it, because they move independently:

- **The observed head changed.** Without this the reported state stays pinned at `SYNCING`, most visibly after a restore: `RebuildDelta` reconstructs `last_mirror_v2_log_id` but not `MirrorSourceHead`, so a correctly restored mirror with nothing left to ingest would report `SYNCING` until a new source log arrived.
- **A recorded error still needs clearing.** A source that fails and then recovers *without producing a new log* leaves the head unchanged, so gating on the head alone would suppress the clear and the API would keep serving a stale error indefinitely. An empty source could never clear at all.

A head that has never been observed carries no information and is skipped, but an observed head of zero is a legitimate value that must still be able to clear an error — which is why the worker records *that* it has observed a head, separately from the value. Once a publication is confirmed applied, an idle mirror stops re-proposing (EN-1773).

The error report marks the status as needing a clear *before* it proposes, not after the apply is confirmed. The propose helper reports confirmation, not application — a wait abandoned on context cancellation does not un-commit the Raft entry — so waiting for confirmation could leave the worker believing the status is clean while an error is persisted. The failure directions are asymmetric: marking dirty for a proposal that never applied costs one idempotent idle publish, while the reverse costs a permanent error on a healthy mirror.

## Storage layout

`internal/query/mirror.go` — reporting keys under the per-ledger zone:

| Key prefix | Content | Read helper |
|------------|---------|-------------|
| `[ZonePerLedger][SubPLMirrorStatus][ledger]` | Persisted last error. | `ReadMirrorStatus` |
| `[ZonePerLedger][SubPLMirrorSourceHead][ledger]` | `uint64` — latest count observed from source. | `ReadMirrorSourceHead` |

Sub-prefix `0x05` under this zone is **reserved and unused**: it formerly held a `MirrorCursor` row, removed in EN-1513.

The ingestion position is not stored here. It lives on the per-ledger `LedgerBoundaries` record as `last_mirror_v2_log_id`, which `ReadMirrorSyncProgress` derives the reported `cursor`, `FOLLOWING` state, and `remaining_logs` from. It is **monotone** by design: the FSM only ever advances it by one contiguous source log at a time, so it never moves backwards even if the source is reconfigured.

## Promotion

A mirror ledger can be promoted to a normal ledger via `PromoteLedgerOrder` (`raft_cmd.proto:246`). The FSM emits a `PromotedLedgerLog` (`common.proto:315`), the WriteSet flags `mirrorConfigChanged = true`, and `Manager.reconcile()` stops the worker on the next reconciliation tick.

After promotion, the ledger accepts normal write requests. The boundary's `last_mirror_v2_log_id` and the source-head row are kept for forensic purposes but no longer advance.

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
  reserved 5;  // was address_rewrite_rules (regex); replaced by rewrite_rules
  repeated MirrorRewriteRule rewrite_rules = 6;  // see "CEL rewrite rules"
}
```

A ledger created with `mirror_source` set has `LedgerInfo.mode = MIRROR`, which is what the manager looks at to decide whether to spin up a worker.

## Declaring indexes on a mirror ledger (operator)

A mirror ledger is read-only to clients, but index create/drop is explicitly allowed on it (`isMirrorSafeApply` whitelists `CreateIndex`/`DropIndex`), so a mirror can carry the same query indexes as its source. Because mirror ledgers are typically provisioned entirely through the Kubernetes operator's `Ledger` CRD, the CRD exposes a declarative `spec.indexes` block so the index set is part of the same GitOps manifest:

```yaml
apiVersion: ledger.formance.com/v1alpha1
kind: Ledger
spec:
  name: my-ledger
  clusterRef: my-cluster
  mode: mirror
  mirrorSource: { ... }
  indexes:
    transaction: [reference, address, sourceAddress]   # transaction builtins
    account: [asset]                                    # account builtins
    metadata:                                           # metadata-key indexes
      - target: account
        key: category
        type: string
```

The operator reconciles the index set by driving `ledgerctl` over pod-exec — there is **no** proto/FSM/checker change; index maintenance rides entirely on the existing `indexes create` / `indexes drop` / `ledgers set-metadata-type` commands. Key semantics (see `misc/operator/internal/controller/ledger_index_reconcile.go`):

- **Ownership-scoped.** The operator tracks the indexes it created in `status.appliedIndexes` and only ever drops those. Externally-created indexes and index kinds the CRD cannot express (the `id` tx-builtin, log builtins, ledger-target metadata, bucket-scoped audit indexes) are left untouched.
- **Unmanaged by default.** Omitting `spec.indexes` (nil) means the operator never lists, creates, or drops indexes on the ledger. A present-but-empty `indexes: {}` means "managed with zero indexes" — it drops only the indexes the operator previously created.
- **Mutable, unlike the ledger itself.** `spec.indexes` is excluded from the immutability spec-hash, so editing it reconciles instead of raising `SpecDrifted`. Convergence is reported via the `IndexesSynced` status condition.
- **Metadata schema is reconciled too.** A metadata index requires its field to be declared in the schema first, so the operator issues `ledgers set-metadata-type` before creating the index — and re-issues it when the declared `type` changes (which the server treats as a schema change that bumps the index forward-encoding version).

This applies to any mode; it is documented here because mirror ledgers are the primary case where the ledger has no other declarative surface for indexes.

## Lifecycle on failure

| Trigger | Worker behaviour |
|---------|------------------|
| Manual delete | Manager stops the worker on reconcile. The status / source-head rows remain in Pebble until the covering cleanup purge runs. |
| Source unreachable | `FetchLogs` returns an error → the worker writes the error into `MirrorStatus` via a small technical-update proposal, then retries with exponential backoff (`worker.go:253-275`). |
| Translation error (e.g. malformed v2 log) | Same path — error persisted, batch is **not** advanced, retried until the operator intervenes or the source heals. |
| Promotion | Manager stops the worker. The boundary's `last_mirror_v2_log_id` is preserved for audit. |
| Pebble write-stall | The worker pauses (`worker.go:240-250`) until back-pressure clears, then resumes. |

There is no automatic "skip the broken log" mode. Operators investigate, fix the upstream condition, and the worker resumes.

## Performance notes

- **Async prefetch**: the next batch is fetched from the source while the previous batch is still applying through Raft + FSM. This overlaps source latency with consensus latency.
- **Coverage pre-declaration**: the worker pre-computes the per-order `plan.Coverage` for the whole batch in one pass (`extractMirrorNeeds`, `worker.go:710-800`), so the per-proposal preload work is amortised.
- **Single-writer on the live path**: `last_mirror_v2_log_id` is only ever written by the FSM applying a `MirrorIngestOrder`, so there is no contention to manage. The one other writer is offline: `backup.RebuildDelta` reconstructs the mark from the replayed delta during a restore, when no worker and no FSM are running (EN-1776).

## What the mirror does not do

- **It does not reconcile against v2 hashes.** The worker trusts the source's log content; it does not cross-check that the resulting v3 state has the same balances as v2. That kind of comparison is a future work item (and would require v2 exposing canonical state hashes).
- **It does not run on followers.** Leadership change suspends the worker until the new leader's manager picks it up.
- **It does not act as a generic CDC sink.** The only sources are Ledger v2 instances; arbitrary event streams are the [events](events.md) subsystem's concern.
