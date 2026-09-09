# Query Checkpoints

Query checkpoints are coordinated point-in-time snapshots of both the main Pebble store and the read index. They enable consistent historical queries without affecting live operations.

## Lifecycle

1. **Create** via `ledgerctl query-checkpoint create` or automatic cron schedule.
2. **Query** using `checkpoint_id` on any read RPC. Every read that exposes the
   field honors it: `GetTransaction`, `ListTransactions`, `GetAccount`,
   `ListAccounts`, `GetLedger`, `GetLedgerStats`, `AggregateVolumes`,
   `GetNumscript`, `ListNumscripts`, `GetLog`, `ListLogs`, and `InspectIndex`.
   A non-zero `checkpoint_id` routes the read to a controller bound to the
   checkpoint's main store and read index instead of the live ones.
3. **Delete** via `ledgerctl query-checkpoint delete <id>` when no longer needed.

Checkpoint IDs are assigned sequentially by the FSM (1, 2, 3, ...).

## Creation Flow

1. Client sends `CreateQueryCheckpoint` request (via ClusterService RPC or BucketService Apply).
2. The request is proposed through Raft consensus.
<<<<<<< HEAD
3. The FSM commits pending state and records `QueryCheckpointState` metadata in
   Pebble, including the entry's Raft applied index `H`.
4. The Applier creates a physical Pebble checkpoint of the main store at `{dataDir}/query-checkpoints/{id}/main/`.
5. The index builder detects the `CreatedQueryCheckpointLog`, publishes the
   normal read projection certificate `H` with the batch that crosses a live
   checkpoint log, and waits for the audit projection certificate to cover the
   same `H`. Cross-cluster restore explicitly marks every surviving
   `QueryCheckpointState` as `restored_from_backup`: `PrepareForBackup` marks
   rows carried by the full checkpoint and `RebuildDelta` marks rows rebuilt
   from incremental logs. The builder consumes that durable provenance instead
   of comparing unrelated source- and destination-cluster Raft numbers. It
   withholds an intermediate normal certificate for a restored checkpoint and
   only publishes the captured restored-store target after folding its complete
   log head; the audit wait is clamped to that target as well.
6. Only after every promised projection covers `H`, the builder flushes the
   WAL-less read store and materializes `{dataDir}/query-checkpoints/{id}/readindex/`.
   The flush is required: otherwise newly committed memtable-only rows and
   certificates have no WAL or SST for Pebble to link. Materialization is
   **per-replica** and **atomic**: build into `readindex.tmp/`, fsync, rename,
   then write `.ready` last. The independently maintained audit projection may
   already contain rows newer than `H`; checkpoint audit reads trim every
   compiled candidate to the audit sequence visible in the frozen main store.
   A crash before the marker never exposes a partial checkpoint.
   Link/compaction races are retried from a clean temp directory.
7. Both stores are opened read-only when a query specifies `checkpoint_id`.
   Reads verify the frozen projection certificate against the main checkpoint's
   durable applied index rather than trusting `.ready` alone.
=======
3. The FSM commits pending state and records `QueryCheckpointState` metadata in Pebble.
4. The Applier creates a physical Pebble checkpoint of the main store at `{dataDir}/query-checkpoints/{id}/main/`. Materialization is **per-replica** and **atomic**, on the same pattern as the read index below: it builds into a sibling `main.tmp/`, fsyncs, atomically renames into place, then writes the `.ready` marker **last**. The atomicity is load-bearing rather than cosmetic — `pebble.DB.Checkpoint` owns its destination for the duration of the call and writes the MANIFEST that makes a directory openable **before** it copies the WAL files, and `WithFlushedWAL` syncs the WAL rather than flushing the memtable, so a directory caught between those steps opens cleanly while missing every memtable-resident write (up to and including the ledger's own `LedgerInfo`). A re-run against an already-marked directory is a no-op, which the Applier needs because it re-crosses the checkpoint log when replaying the spool or the WAL after a restart.
5. The index builder detects the `CreatedQueryCheckpointLog` and, **at the exact moment it crosses that log**, materializes the read index checkpoint at `{dataDir}/query-checkpoints/{id}/readindex/`. Because the builder breaks its batch on the checkpoint log, the live read index at that instant reflects precisely `MaxSequence` — the checkpoint's point-in-time. The read index runs without a WAL, so a Pebble checkpoint of it carries only flushed SSTs; the builder flushes the memtable before checkpointing so every row folded up to that instant is in the frozen index. Materialization is **per-replica** (every node's builder does this independently) and **atomic**: it builds into a sibling `readindex.tmp/`, fsyncs, atomically renames into place, then writes the `.ready` marker **last**. A crash before the marker leaves no `.ready` file, so the checkpoint is never observed half-built. Pebble hard-links SST files last, so a checkpoint can fail mid-link (a concurrent compaction removing an SST); the builder retries the checkpoint on a `link ... no such file or directory` error, cleaning the temp directory between attempts.
6. Both stores are opened read-only when a query specifies `checkpoint_id`.
>>>>>>> 2576ff6b6 (fix(dal): materialize the query-checkpoint main store atomically (EN-1978))

## Readiness and Error Contract

Both halves materialize asynchronously and **per-replica** (steps 4 and 5), by different components that can reach the checkpoint log at different moments. Readiness on a node is signalled solely by the two local `.ready` markers — one per directory, each vouching only for its own — and a read requires both. Openability is **not** a readiness signal: an unmarked directory can open cleanly while incomplete, so the markers are checked before the read-only open rather than inferred from it. There is **no** cross-node readiness map and **no** background reconciler.

<<<<<<< HEAD
- **`CreateQueryCheckpoint` blocks on the creator node's marker.** The handler waits (`readStore.WaitForCheckpoint`) for the local `.ready` marker before returning, so an immediate read at the returned `checkpoint_id` **routed back to the creator node succeeds**. It waits on the marker, not on the index-builder progress cursor — the cursor fast path was the EN-1460 root cause: the cursor is persisted in the batch that *precedes* the physical checkpoint creation, so it reaches the target sequence ~100-150 ms before the directory exists.
- **Audit is part of the readiness promise.** The checkpoint log carries `H`.
  The normal builder does not publish `.ready` until the audit projection has
  certified `H`, so a filtered audit query cannot be frozen against an
  incomplete audit index. Every creation trigger passes through a shared
  admission preflight, so public `Apply`, the cluster RPC and the automatic
  scheduler all fail explicitly with `ErrAuditDisabled` when the projection is
  permanently disabled and `ErrIndexBuilding` while it is rebuilding. An
  enabled projection starts in the rebuilding state and only becomes ready
  after boot has classified its persisted cursor and completed the initial
  rebuild/catch-up. A failed rebuild
  remains in rebuilding state until a later successful rebuild/catch-up; it
  cannot advertise a false readiness window. An already-proposed create waits
  through a transient rebuild and resumes only after the replacement projection
  is ready and has certified `H`. A rebuild racing after that wait is detected
  by an audit lifecycle generation captured before the snapshot and checked
  atomically with `.ready` publication; a changed generation leaves the marker
  absent and retries materialization from a clean directory. A disabled
  projection leaves the checkpoint unavailable. In every waiting case the
  caller's deadline/cancellation ends its marker wait. Unfiltered or
  sequence-only live audit reads remain independent of the audit index, but a
  checkpoint promises the complete projection set.
  An audit indexing failure during boot or steady state is also advertised as
  transiently rebuilding to admission, while the checkpoint already in flight
  is left unavailable and the normal builder continues past its log. The audit
  worker keeps retrying and restores readiness after a successful fold; because
  there is no checkpoint reconciler, the failed checkpoint is deleted and
  recreated through the normal client/operator recovery path.
- **A read on a node that has not yet materialized the checkpoint returns a typed, retryable error.** Checkpoint reads are served locally on whichever node receives the request (no leader routing). On a node whose builder has not yet crossed the checkpoint log, `openCheckpointStores` finds no `.ready` marker but sees the checkpoint in the replicated `QueryCheckpointState` registry, and returns `ErrCheckpointNotReady` — reason `CHECKPOINT_NOT_READY`, mapped to gRPC `Unavailable`. This mirrors the per-replica `INDEX_BUILDING → Unavailable` pattern for metadata indexes: clients retry until that node materializes the checkpoint inline. The read never returns partial state.
- **A read for a checkpoint id that does not exist returns `NotFound`.** If there is no `.ready` marker *and* no `QueryCheckpointState` entry for the id, `openCheckpointStores` returns `NotFound` (permanent) so clients stop retrying — distinct from the retryable `Unavailable` above.
- **Unrecoverable checkpoints degrade to `NotFound`, not wrong data.** There is no historical reconstruction: inline materialization is already exactly point-in-time. If a node crashes between the atomic rename and the marker, that node will never have a `.ready` marker for the checkpoint. Since the checkpoint is still registered, reads there return the retryable `Unavailable` and never self-heal — the operator/client recreates the checkpoint (aligned with the existing `AcquireCheckpoint` client workaround, which deletes-and-recreates on timeout). Deleting the checkpoint then makes reads return `NotFound`.
=======
- **`CreateQueryCheckpoint` blocks on the creator node's markers.** The proposal's result is released only once the applier has materialized and marked the main store, and the handler then waits (`readStore.WaitForCheckpoint`) for the local read-index `.ready` marker before returning, so both markers exist on the creator node and an immediate read at the returned `checkpoint_id` **routed back to the creator node succeeds**. It waits on the marker, not on the index-builder progress cursor — the cursor fast path was the EN-1460 root cause: the cursor is persisted in the batch that *precedes* the physical checkpoint creation, so it reaches the target sequence ~100-150 ms before the directory exists.
- **A read on a node that has not yet materialized the checkpoint returns a typed, retryable error.** Checkpoint reads are served locally on whichever node receives the request (no leader routing). On a node where either directory is not yet marked ready, `openCheckpointStores` sees the checkpoint in the replicated `QueryCheckpointState` registry and returns `ErrCheckpointNotReady` — reason `CHECKPOINT_NOT_READY`, mapped to gRPC `Unavailable`. This mirrors the per-replica `INDEX_BUILDING → Unavailable` pattern for metadata indexes: clients retry until that node materializes the checkpoint inline. The read never returns partial state.
- **A read for a checkpoint id that does not exist returns `NotFound`.** If a `.ready` marker is missing *and* there is no `QueryCheckpointState` entry for the id, `openCheckpointStores` returns `NotFound` (permanent) so clients stop retrying — distinct from the retryable `Unavailable` above.
- **Unrecoverable checkpoints degrade to `NotFound`, not wrong data.** There is no historical reconstruction: re-deriving a checkpoint at a past `MaxSequence` is infeasible (logs are purged per chapter after cold-storage archival) and unnecessary (inline materialization is already exactly point-in-time). If a node crashes between an atomic rename and its marker, or purged the checkpoint's logs before its builder reached them, that node will never have both `.ready` markers for the checkpoint. Since the checkpoint is still registered, reads there return the retryable `Unavailable` and never self-heal — the operator/client recreates the checkpoint (aligned with the existing `AcquireCheckpoint` client workaround, which deletes-and-recreates on timeout). Deleting the checkpoint then makes reads return `NotFound`.
>>>>>>> 2576ff6b6 (fix(dal): materialize the query-checkpoint main store atomically (EN-1978))

The `.ready` markers and the checkpoint directories are rebuildable filesystem lifecycle state (a projection of the audit log), not a persisted Pebble projection, so they are outside the checker's scope.

## Retention cap (query-checkpoint limit)

The number of *live* query checkpoints is bounded by the Raft-replicated cluster policy (`ClusterPolicy.query_checkpoint_limit`; see [the cluster policy](../fsm/deterministic-fsm.md#35-raft-replicated-cluster-policy)). Enforcement is in the **FSM apply path, after the idempotency gate**, so a committed order resolves identically on every node:

- **`CreateQueryCheckpoint`** is rejected with `ErrCheckpointLimitReached` (`CHECKPOINT_LIMIT_REACHED`) when the live count is already at the limit. Creation never evicts; an operator deletes a checkpoint to free a slot. A limit of `0` (the unset default before any policy is committed) means uncapped — write readiness holds business writes until a policy is committed, so a real create sees a configured limit.
- **`DeleteQueryCheckpoint`** is rejected with `ErrCheckpointNotFound` (`CHECKPOINT_NOT_FOUND`) for an id that is not live, and with `ErrCheckpointIDRequired` for id `0`.

The live-id set is deterministic FSM state (`FSMState.LiveQueryCheckpointIDs`), rehydrated at boot by scanning the `SubGlobQueryCheckpoint` rows and updated by the create/delete handlers, so apply enforces the cap and existence without a Pebble read. The rows are a checker-verified projection: `compareQueryCheckpoints` re-derives the live set (with each row's `max_sequence`, `created_at`, and `applied_index`) from the `CreatedQueryCheckpoint` / `DeletedQueryCheckpoint` logs and diffs it against the stored rows, and the audit-rebuild path recreates the rows from those logs.

Because enforcement sits after the idempotency gate, a keyed retry replays the first apply's frozen outcome: a create that filled the cap replays its original success, and a create that hit the limit replays that rejection (`ErrCheckpointLimitReached` is a definitive, freezable outcome) even after a slot later frees — one outcome per idempotency key.

## Automatic Checkpoint Creation (Cron Scheduler)

Checkpoint creation can be automated via a cron schedule. The schedule is a runtime-modifiable configuration stored in Raft.

### Configuration

```bash
# Create a checkpoint every day at midnight
ledgerctl query-checkpoint set-schedule "0 0 * * *"

# Create a checkpoint every hour
ledgerctl query-checkpoint set-schedule "0 * * * *"

# Disable automatic creation
ledgerctl query-checkpoint delete-schedule

# Show current schedule
ledgerctl query-checkpoint get-schedule
```

The cron expression uses the standard 5-field format (`minute hour day-of-month month day-of-week`) or the extended 6-field format with an optional leading seconds field (`second minute hour day-of-month month day-of-week`).

### How It Works

The `QueryCheckpointScheduler` runs on every node but only triggers checkpoint creation on the **Raft leader**. When the cron fires, the leader proposes a `CreateQueryCheckpoint` order through the admission layer — the same path as `ledgerctl query-checkpoint create`.

1. The schedule is persisted in Pebble (key prefix `0xE4`) and replicated via Raft.
2. When the schedule changes, a notification signal wakes the scheduler goroutine to recompute the next fire time.
3. On leader change, the new leader's scheduler is already running and will fire at the next scheduled time.

Checkpoints accumulate over time. Old checkpoints are **not** automatically cleaned up — use `ledgerctl query-checkpoint delete` to remove them when no longer needed.

**File**: `internal/infra/state/query_checkpoint_scheduler.go`

### Protobuf Messages

```protobuf
// Raft-replicated log entries
message SetQueryCheckpointScheduleLog {
  string cron = 1;
}
message DeletedQueryCheckpointScheduleLog {}

// gRPC requests (via Apply)
message SetQueryCheckpointScheduleRequest {
  string cron = 1;
}
message DeleteQueryCheckpointScheduleRequest {}

// gRPC query (ClusterService)
rpc GetQueryCheckpointSchedule(GetQueryCheckpointScheduleRequest) returns (GetQueryCheckpointScheduleResponse);
```

## gRPC API

| Method | Service | Description |
|--------|---------|-------------|
| `CreateQueryCheckpoint` | ClusterService | Create a checkpoint (write, leader-only) |
| `DeleteQueryCheckpoint` | ClusterService | Delete a checkpoint (write, leader-only) |
| `ListQueryCheckpoints` | ClusterService | List all checkpoints (read, any node) |
| `GetQueryCheckpointInfo` | ClusterService | Get checkpoint details (read, any node) |
| `GetQueryCheckpointSchedule` | ClusterService | Get the current schedule (read, any node) |
| `Apply(SetQueryCheckpointScheduleRequest)` | BucketService | Set the schedule (write, leader-only) |
| `Apply(DeleteQueryCheckpointScheduleRequest)` | BucketService | Delete the schedule (write, leader-only) |
| `Apply(CreateQueryCheckpointRequest)` | BucketService | Create a checkpoint (write, leader-only) |
| `Apply(DeleteQueryCheckpointRequest)` | BucketService | Delete a checkpoint (write, leader-only) |

## CLI Commands

```bash
# Create a checkpoint
ledgerctl query-checkpoint create

# Delete a checkpoint
ledgerctl query-checkpoint delete <id>

# List all checkpoints
ledgerctl query-checkpoint list

# Show checkpoint details
ledgerctl query-checkpoint info <id>

# Set automatic creation schedule
ledgerctl query-checkpoint set-schedule "0 0 * * *"

# Disable automatic creation
ledgerctl query-checkpoint delete-schedule

# Show current schedule
ledgerctl query-checkpoint get-schedule
```

## Storage

| Prefix | Key | Value |
|--------|-----|-------|
| `0xE2` | `[KeyPrefixQueryCheckpoint][checkpointID BE]` | `QueryCheckpointState` protobuf (`max_sequence`, `created_at`, `applied_index`, plus technical `restored_from_backup` provenance) |
| `0xE3` | `[KeyPrefixNextQueryCheckpointID]` | `uint64` — next checkpoint ID counter |
| `0xE4` | `[KeyPrefixQueryCheckpointSchedule]` | Cron expression string (empty = disabled) |

Physical checkpoint data is stored outside Pebble:

```
data/
  query-checkpoints/
    1/
      main/              # Pebble checkpoint of main store
        .ready           # readiness marker, written last by the applier
      readindex/         # Pebble checkpoint of read index
        .ready           # readiness marker, written last by the index builder
    2/
      main/
        .ready
      readindex/
        .ready
```
