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
3. The FSM commits pending state and records `QueryCheckpointState` metadata in Pebble.
4. The Applier creates a physical Pebble checkpoint of the main store at `{dataDir}/query-checkpoints/{id}/main/`.
5. The index builder detects the `CreatedQueryCheckpointLog` and, **at the exact moment it crosses that log**, materializes the read index checkpoint at `{dataDir}/query-checkpoints/{id}/readindex/`. Because the builder breaks its batch on the checkpoint log, the live read index at that instant reflects precisely `MaxSequence` — the checkpoint's point-in-time. Materialization is **per-replica** (every node's builder does this independently) and **atomic**: it builds into a sibling `readindex.tmp/`, fsyncs, atomically renames into place, then writes the `.ready` marker **last**. A crash before the marker leaves no `.ready` file, so the checkpoint is never observed half-built. Pebble hard-links SST files last, so a checkpoint can fail mid-link (a concurrent compaction removing an SST); the builder retries the checkpoint on a `link ... no such file or directory` error, cleaning the temp directory between attempts.
6. Both stores are opened read-only when a query specifies `checkpoint_id`.

## Readiness and Error Contract

The read index materializes asynchronously and **per-replica** (step 5). Readiness on a node is signalled solely by the local `.ready` marker; there is **no** cross-node readiness map and **no** background reconciler.

- **`CreateQueryCheckpoint` blocks on the creator node's marker.** The handler waits (`readStore.WaitForCheckpoint`) for the local `.ready` marker before returning, so an immediate read at the returned `checkpoint_id` **routed back to the creator node succeeds**. It waits on the marker, not on the index-builder progress cursor — the cursor fast path was the EN-1460 root cause: the cursor is persisted in the batch that *precedes* the physical checkpoint creation, so it reaches the target sequence ~100-150 ms before the directory exists.
- **A read on a node that has not yet materialized the checkpoint returns a typed, retryable error.** Checkpoint reads are served locally on whichever node receives the request (no leader routing). On a node whose builder has not yet crossed the checkpoint log, `openCheckpointStores` finds no `.ready` marker but sees the checkpoint in the replicated `QueryCheckpointState` registry, and returns `ErrCheckpointNotReady` — reason `CHECKPOINT_NOT_READY`, mapped to gRPC `Unavailable`. This mirrors the per-replica `INDEX_BUILDING → Unavailable` pattern for metadata indexes: clients retry until that node materializes the checkpoint inline. The read never returns partial state.
- **A read for a checkpoint id that does not exist returns `NotFound`.** If there is no `.ready` marker *and* no `QueryCheckpointState` entry for the id, `openCheckpointStores` returns `NotFound` (permanent) so clients stop retrying — distinct from the retryable `Unavailable` above.
- **Unrecoverable checkpoints degrade to `NotFound`, not wrong data.** There is no historical reconstruction: re-deriving a checkpoint at a past `MaxSequence` is infeasible (logs are purged per chapter after cold-storage archival) and unnecessary (inline materialization is already exactly point-in-time). If a node crashes between the atomic rename and the marker, or purged the checkpoint's logs before its builder reached them, that node will never have a `.ready` marker for the checkpoint. Since the checkpoint is still registered, reads there return the retryable `Unavailable` and never self-heal — the operator/client recreates the checkpoint (aligned with the existing `AcquireCheckpoint` client workaround, which deletes-and-recreates on timeout). Deleting the checkpoint then makes reads return `NotFound`.

The `.ready` marker and the checkpoint directories are rebuildable filesystem lifecycle state (a projection of the audit log), not a persisted Pebble projection, so they are outside the checker's scope.

## Automatic Checkpoint Creation (Cron Scheduler)

Checkpoint creation can be automated via a cron schedule. The schedule is a runtime-modifiable configuration stored in Raft, following the same pattern as chapter schedule (`SetChapterSchedule`).

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
| `0xE2` | `[KeyPrefixQueryCheckpoint][checkpointID BE]` | `QueryCheckpointState` protobuf |
| `0xE3` | `[KeyPrefixNextQueryCheckpointID]` | `uint64` — next checkpoint ID counter |
| `0xE4` | `[KeyPrefixQueryCheckpointSchedule]` | Cron expression string (empty = disabled) |

Physical checkpoint data is stored outside Pebble:

```
data/
  query-checkpoints/
    1/
      main/              # Pebble checkpoint of main store
      readindex/         # Pebble checkpoint of read index
        .ready           # readiness marker, written last by the index builder
    2/
      main/
      readindex/
        .ready
```
