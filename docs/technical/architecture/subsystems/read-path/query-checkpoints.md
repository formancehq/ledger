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

1. Client submits a `create_query_checkpoint` request through `BucketService.Apply` — the only gRPC entry point for it since EN-1954. It is a checkpoint trigger, so admission accepts it only as the last action of a batch.
2. The request is proposed through Raft consensus.
3. The FSM commits pending state and records `QueryCheckpointState` metadata in
   Pebble, including the entry's Raft applied index `H`.
4. The Applier materializes the main store at `{dataDir}/query-checkpoints/{id}/main/`, **per-replica** and **atomic**: build into `main.tmp/`, fsync, write `.ready` into the temp directory, rename, fsync the parent. `pebble.DB.Checkpoint` writes the MANIFEST that makes a directory openable *before* it copies the WAL files, so a directory caught mid-copy opens cleanly while missing every write still resident in the memtable; the marker is what vouches for completeness, and because it is written before the rename the final path never exists without it. Residual state after a crash is nothing, a temp directory (marked or not), or the finished result. The Applier is gated for the whole materialization — later entries wait in the spool, the live store stays at `H` — and the spool is reset on restart, so `RecoverAndReplay` reopens with the live store still at `H`: before replaying the WAL it rebuilds the main store of every registered checkpoint whose `applied_index` equals the live index and whose marker is missing, discarding any temp residue. A call against a marked directory is a no-op, which keeps the materialization idempotent; the applier does not re-cross the trigger entry (its cursor commits in the same batch as that entry, and apply skips entries at or below the cursor), so the no-op is a guard rather than a path the applier depends on.
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
   certificates have no WAL or SST for Pebble to link. The physical snapshot
   includes internal progress, per-index version state, backfill cursors, and
   the EN-1771 per-ledger `EMPTY`/`NON_EMPTY` history tracker at that same
   certified boundary. Materialization is **per-replica** and **atomic**: build
   into `readindex.tmp/`, fsync, rename, then write `.ready` last. The
   independently maintained audit projection may already contain rows newer
   than `H`; checkpoint audit reads trim every compiled candidate to the audit
   sequence visible in the frozen main store. A crash before the marker never
   exposes a partial checkpoint. Link/compaction races are retried from a clean
   temp directory. A checkpoint
   log is always the only log of its builder batch, and the indexed cursor (and
   the AppliedProposal cursor with it) is committed **after** the
   materialization, in a batch of its own; the batch that crosses the log
   commits only the certificate. A builder that dies anywhere between resumes at
   the checkpoint log, re-indexes nothing (the log writes no rows) and
   materializes again — a marked directory makes that a no-op. Residual state
   after a crash is therefore a cursor one log behind plus nothing, an unmarked
   directory (temp or final, discarded on the re-cross) or a marked one.
7. Both stores are opened read-only when a query specifies `checkpoint_id`, and
   only once **both** directories carry their `.ready` marker — openability is
   not a completeness signal (see step 4). Reads additionally verify the frozen
   read projection certificate against the main checkpoint's durable applied
   index rather than trusting `.ready` alone.

## Readiness and Error Contract

Both halves materialize asynchronously and **per-replica** (steps 4 and 6), by different components that can reach the checkpoint log at different moments. Readiness on a node is signalled solely by the two local `.ready` markers — one per directory, each vouching only for its own — and a read requires both. Openability is **not** a readiness signal: an unmarked directory can open cleanly while incomplete, so the markers are checked before the read-only open rather than inferred from it. There is **no** cross-node readiness map and **no** background reconciler: each half finishes an interrupted materialization itself on restart, because neither store's durable progress passes the checkpoint point before its half is marked (steps 4 and 6).

- **For a newly executed creation with response payloads enabled, `Apply` waits for the serving node's marker unless deletion supersedes it.** The `Apply` handler executes in this order: `ctrl.Apply` returns the committed logs and execution provenance → for a new execution, the handler scans those logs for a `CreatedQueryCheckpointLog` and, for each one, waits (`readStore.WaitForCheckpoint`) for that checkpoint's local `.ready` marker → response signing → `skip_response` payload stripping → return. The wait therefore precedes stripping, which nils out the payload carrying the id, and it locates the log by payload type rather than by position, because the checkpoint trigger is the batch's *last* action. The Applier marks the main store before it resolves the creation, so when the call returns both markers exist on that node and, if the checkpoint remains live, an immediate read at the returned `checkpoint_id` **routed back to that node succeeds**. It waits on the marker, not on the index-builder progress cursor — the cursor fast path was the EN-1460 root cause: the cursor is persisted in the batch that *precedes* the physical checkpoint creation, so it reaches the target sequence ~100-150 ms before the directory exists.
- **For a newly executed creation with response payloads enabled, a follower waits for its own marker after the leader waited for the leader's.** A follower's `Apply` forwards the batch to the leader through `BucketGrpcClient.Apply`; the leader's handler waits for the leader's marker, and the forwarding node then waits for its own. The node the client is actually talking to is therefore ready when the call returns, at the cost of a second wait on the follower path.
- **For a new creation that remains live, with `skip_response=true`, only the leader is guaranteed ready.** The follower forwards `skip_response` unchanged. The leader waits for its local marker and strips the payload carrying the checkpoint ID before returning to the follower. The follower therefore has no checkpoint payload to wait on and can return before its own marker is ready. The client receives no checkpoint ID in this response; a subsequent checkpoint read on a replica that is not ready returns the retryable error described below.
- **An idempotent replay returns the historical outcome immediately.** A keyed
  creation can be retried after deletion or while its first call is still
  materializing. The retry returns the original log/checkpoint IDs without
  creating a new log, recreating the checkpoint, or waiting for a marker.
  Success records the original mutation; it does not promise that the resource
  still exists or is currently ready. The FSM sets `Replayed` at the deduplication
  gate; admission and the controller retain it alongside the resolved logs.
  The leader emits the server-produced `ledger-apply-replayed` response trailer,
  and forwarding followers require and preserve it. A request header cannot
  suppress the wait. Inferring replay from log payloads or marker existence
  would confuse historical results with fresh creations or follower lag.
- **Deletion supersedes an in-flight new creation's readiness wait.** Each wait
  checks a fresh, consistent main-store snapshot: an absent checkpoint row
  with a next checkpoint ID greater than this ID proves it was allocated and
  subsequently deleted. Absence before allocation is follower lag and continues
  waiting. The wait rechecks after read-index progress and at most every 100 ms,
  so deletion still ends it if indexing stalls. It returns the committed
  creation success, without recreating files. A deletion racing after readiness
  can still make a subsequent read return `NotFound`; success is not a resource
  lease. The registry deletion commits before filesystem cleanup, so the wait
  can end while cleanup is still removing directories. No durable tombstone or
  new restore state is introduced; the existing checkpoint counter and registry
  provide the lifecycle evidence.
- **Audit is part of the readiness promise.** The checkpoint log carries `H`.
  The normal builder does not publish `.ready` until the audit projection has
  certified `H`, so a filtered audit query cannot be frozen against an
  incomplete audit index. Every creation trigger passes through a shared
  admission preflight, so public `Apply` and the automatic scheduler both fail
  explicitly with `ErrAuditDisabled` when the projection is permanently
  disabled and `ErrIndexBuilding` while it is rebuilding. An enabled projection
  starts in the rebuilding state and only becomes ready after boot has
  classified its persisted cursor and completed the initial rebuild/catch-up.
  A failed rebuild
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
- **A read on a node that has not yet materialized the checkpoint returns a typed, retryable error.** Checkpoint reads are served locally on whichever node receives the request (no leader routing). On a node where either half is still unmaterialized — the applier has not finished the main store, or the builder has not yet crossed the checkpoint log — `openCheckpointStores` finds a missing `.ready` marker but sees the checkpoint in the replicated `QueryCheckpointState` registry, and returns `ErrCheckpointNotReady` — reason `CHECKPOINT_NOT_READY`, mapped to gRPC `Unavailable`. This mirrors the per-replica `INDEX_BUILDING → Unavailable` pattern for metadata indexes: clients retry until that node materializes the checkpoint inline. The read never returns partial state.
- **A read for a checkpoint id that does not exist returns `NotFound`.** If there is no `.ready` marker *and* no `QueryCheckpointState` entry for the id, `openCheckpointStores` returns `NotFound` (permanent) so clients stop retrying — distinct from the retryable `Unavailable` above.
- **Transient live materialization failures are retried before later mutations.** The projection batch commits before filesystem materialization; the native cursor commits only after it. If materialization fails, the builder retains the checkpoint id and audit horizon in memory and retries that side effect at the start of the next `processLogs` call, before re-reading from the cursor still parked at the checkpoint log. It does not replay the committed batch, which could duplicate lifecycle mutations. Later logs, backfills, and event GC remain behind this retry boundary so the frozen readstore still represents the original checkpoint horizon.
- **A damaged directory fails permanently instead of retrying forever.** Both markers present means each half was complete when it was published, so a read-only open that then fails is damage, not lag: the error surfaces as-is and reaches the client as a sanitized permanent `Unknown` carrying a correlation ID (the node log holds the cause), never the retryable `Unavailable` that `actions.GRPCRetryPolicy` would retry 50 times. Recovery is the same delete-and-recreate as the cases below.
- **A crash inside a materialization is finished on restart; nothing else degrades to wrong data.** There is no historical reconstruction — each half instead keeps its recovery source intact until it is marked: the live main store stays at `H` while the Applier is gated, and the read index's cursor stays before the checkpoint log (steps 4 and 6). A replica that dies at any point of either materialization reopens, rebuilds the missing half from that source, and serves the checkpoint. Reads meanwhile return the retryable `Unavailable`. Two cases stay `Unavailable` on a replica for good, because that replica never had the source: one that never applied the creation entry (it joined through a later snapshot, or had to resync from the leader after the crash), and one whose audit projection is disabled or failed (the builder moves past the log without materializing). For those the operator/client deletes and recreates the checkpoint (the `AcquireCheckpoint` client helper does so on timeout); deleting it makes reads return `NotFound`.

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

The `QueryCheckpointScheduler` runs on every node but only triggers checkpoint creation on the **Raft leader**. When the cron fires, the leader proposes a `CreateQueryCheckpoint` order through the admission layer directly, as a leader-internal system actor. That is not a gRPC handler, so it is outside the Apply-only rule; `ledgerctl query-checkpoint create` reaches the same order through `Apply`.

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
| `ListQueryCheckpoints` | ClusterService | List all checkpoints (read, any node) |
| `GetQueryCheckpointInfo` | ClusterService | Get checkpoint details (read, any node) |
| `GetQueryCheckpointSchedule` | ClusterService | Get the current schedule (read, any node) |
| `Apply(SetQueryCheckpointScheduleRequest)` | BucketService | Set the schedule (write, leader-only) |
| `Apply(DeleteQueryCheckpointScheduleRequest)` | BucketService | Delete the schedule (write, leader-only) |
| `Apply(CreateQueryCheckpointRequest)` | BucketService | Create a checkpoint (write, leader-only). Returns the id and max sequence in the `CreatedQueryCheckpointLog`; new execution waits for readiness unless deleted; replay returns the historical result |
| `Apply(DeleteQueryCheckpointRequest)` | BucketService | Delete a checkpoint (write, leader-only) |

ClusterService keeps only the three read RPCs. EN-1954 removed its
`CreateQueryCheckpoint` and `DeleteQueryCheckpoint` mutation RPCs, and with them
its admission dependency, so every audited write reaches Raft through `Apply`.

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
        .ready           # readiness marker, published with the directory
      main.tmp/          # only while materializing, or left by a crash
      readindex/         # Pebble checkpoint of read index
        .ready           # readiness marker, written last by the index builder
      readindex.tmp/     # only while materializing, or left by a crash
    2/
      main/
        .ready
      readindex/
        .ready
```
