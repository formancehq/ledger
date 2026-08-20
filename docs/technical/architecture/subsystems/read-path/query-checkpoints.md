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

## Retention and the Live Checkpoint Limit

The number of live query checkpoints is bounded by a **per-node startup limit** (`--query-checkpoint-limit`, default **10**), enforced softly at admission. There is no automatic eviction — once the live count reaches the limit, creation fails until an operator deletes one.

**Why a limit, why configurable, and why 10 by default.** Each query checkpoint is a full `db.Checkpoint()` of the main store plus a read-index snapshot — directories of hard-linked SSTs that pin disk as the live store compacts. Left unbounded, a scheduler or a client loop grows disk and `ListQueryCheckpoints` payloads without end (the EN-1501 hazard). The *right* ceiling depends on deployment (disk headroom, checkpoint cadence, how many point-in-time views a workload needs), so it is an infrastructure setting rather than a baked-in constant. **10 is a conservative default**, not a derived optimum: enough for typical point-in-time use while keeping worst-case disk bounded.

- **Enforcement is soft, at admission — the FSM apply path is unconditional.** Before proposing a `CreateQueryCheckpoint`, the leader counts its current live checkpoints (`query.ReadLiveQueryCheckpointIDs`, a proposer-side Pebble read) and rejects at the limit with `ErrCheckpointLimitReached` — reason `CHECKPOINT_LIMIT_REACHED`, gRPC `ResourceExhausted` / HTTP 429. The committed command carries no limit and the apply path counts nothing: `processCreateQueryCheckpoint` always applies identically on every node (FSM determinism, invariant #2), so the node-local limit never enters replicated state. This is why the limit can be a plain startup flag rather than a Raft-committed setting.
- **Delete existence is also checked at admission.** `DeleteQueryCheckpoint` for an id that is zero, not live, or already deleted earlier in the same bulk is rejected before proposal with `ErrCheckpointNotFound` — reason `CHECKPOINT_NOT_FOUND`, gRPC `NotFound` / HTTP 404. A committed delete applies unconditionally: emitting the `DeletedQueryCheckpointLog` on every node is what drives the per-node physical file cleanup.
- **Same-bulk effects are folded in.** Within one Apply bulk, admission tracks creates and deletes staged by earlier requests (in the admission-side bulk overlay), so a batch that deletes a checkpoint then creates one is admitted atomically at the cap, and a second create in one bulk counts the first. The evaluated live count is `committed + bulkCreates − bulkDeletes`.
- **Bounded overshoot, no reservation machinery.** Because the committed count is read at propose time and the limit is not serialized into the command, concurrent in-flight creates *across separate proposals* can each pass the check before any commits, briefly overshooting the limit by up to the number of simultaneous creates. This is accepted deliberately — a slot reservation / serialization scheme would add complexity for a soft disk ceiling that tolerates a small transient overshoot.
- **Rolling-upgrade behavior.** Enforcement lives on whichever node is leader when a create is admitted. A pre-upgrade leader that lacks the check will not enforce; an upgraded leader will. Because the check is admission-side and the committed command is unchanged, the two never disagree about *applied* state — only about whether a given create was admitted — so replicas stay deterministic throughout the upgrade. Operators wanting uniform enforcement should complete the rollout.
- **Checker coverage.** The stored checkpoint rows are verified against the audit chain **both ways**: the checker re-derives the live set from the `CreatedQueryCheckpointLog` / `DeletedQueryCheckpointLog` stream (baseline-seeded under archiving) and flags a stored row with no create / a later delete *and* an audit-live checkpoint with no stored row (`CHECK_STORE_ERROR_TYPE_QUERY_CHECKPOINT_MISMATCH`), per invariant #8. For a present row it also compares `max_sequence`, `created_at`, and the key-vs-payload `checkpoint_id`. The audit-rebuild path (`internal/infra/backup`) recreates the metadata rows and the monotonic next-ID counter from the logs — the physical files cannot be, so a rebuilt checkpoint reads `Unavailable` until deleted — so a missing row is always corruption, never a restore artifact. The limit itself is not a persisted projection, so there is no limit checker pass. `NextQueryCheckpointID` is monotonic and is **not** the live count (deletes do not decrement it).
- **Orphaned directory reclamation.** A follower caught up by a snapshot install receives the leader's row-absent state without running the per-entry file-delete hook, so its local `query-checkpoints/<id>/` can survive with no live row. Recovery (`RecoverState`, at boot and after every follower sync) sweeps `query-checkpoints/*` against the live IDs read from Pebble (`query.ReadLiveQueryCheckpointIDs`) and removes any directory with no live row — best-effort, so a transient filesystem error never blocks recovery. Without it those hard-linked Pebble checkpoints would pin disk indefinitely, since deletion is otherwise only log-driven.

Because the live cardinality is bounded by the limit, `ListQueryCheckpoints` stays small and is intentionally not paginated.

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

Checkpoints are never automatically evicted, but the number live at once is capped by the [per-node limit](#retention-and-the-live-checkpoint-limit). Once the limit is reached the scheduler stops creating checkpoints — logging the condition once rather than on every tick — and resumes automatically after an operator frees a slot with `ledgerctl query-checkpoint delete`.

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

The live-checkpoint limit is a per-node startup flag (`--query-checkpoint-limit`, default 10), not a runtime command.

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
