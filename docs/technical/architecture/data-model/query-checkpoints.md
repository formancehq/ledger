# Query Checkpoints

Query checkpoints are coordinated point-in-time snapshots of both the main Pebble store and the read index. They enable consistent historical queries without affecting live operations.

## Lifecycle

1. **Create** via `ledgerctl query-checkpoint create` or automatic cron schedule.
2. **Query** using `checkpoint_id` on any read RPC (`ListTransactions`, `GetAccount`, `ListLedgers`, etc.).
3. **Delete** via `ledgerctl query-checkpoint delete <id>` when no longer needed.

Checkpoint IDs are assigned sequentially by the FSM (1, 2, 3, ...).

## Creation Flow

1. Client sends `CreateQueryCheckpoint` request (via ClusterService RPC or BucketService Apply).
2. The request is proposed through Raft consensus.
3. The FSM commits pending state and records `QueryCheckpointState` metadata in Pebble.
4. The Applier creates a physical Pebble checkpoint of the main store at `{dataDir}/query-checkpoints/{id}/main/`.
5. The index builder detects the `CreatedQueryCheckpointLog` and creates a read index checkpoint at `{dataDir}/query-checkpoints/{id}/readindex/`.
6. Both stores are opened read-only when a query specifies `checkpoint_id`.

## Automatic Checkpoint Creation (Cron Scheduler)

Checkpoint creation can be automated via a cron schedule. The schedule is a runtime-modifiable configuration stored in Raft, following the same pattern as period schedule (`SetPeriodSchedule`).

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
message DeleteQueryCheckpointScheduleLog {}

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
      main/       # Pebble checkpoint of main store
      readindex/  # Pebble checkpoint of read index
    2/
      main/
      readindex/
```
