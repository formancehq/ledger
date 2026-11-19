# Backup and restore

## Current contract

A backup is one complete Pebble checkpoint, published to S3 or Azure Blob Storage
under a destination namespace (`bucket-id`). Each successful backup replaces the
current manifest. A destination has one current restore point; Kubernetes
`BackupRun` retention does not retain object-store history.

The leader proposes a `BackupOrder.Start` through Raft before it starts work.
The FSM reserves a destination slot keyed by `CanonicalDestinationKey`; another
backup of the same destination is rejected. The leader cancels its executor on
leadership loss, and the next leader fails orphaned jobs to release their slots.
The FSM records lifecycle orders without blocking on object upload.

`RunBackup` requires the committed audit key. It calls `Store.Flush` before
`Store.CreateTemporaryCheckpoint`, which calls
`pebble.DB.Checkpoint(..., pebble.WithFlushedWAL())`. The backup flush materializes
committed writes in SSTs before capture. Concurrent writes may still be carried
by Pebble WAL files in the checkpoint. The backup walks **every** file in the
completed checkpoint directory; neither SST-only selection nor filename-only
comparison is valid. Pebble's `MANIFEST-NNNNNN` can grow without changing its
name. Every file is hashed and stored under a content-addressed key, so
unchanged bytes are reused and published objects are never overwritten.

The checkpoint includes the applied Raft index and last log/audit sequences.
A transaction confirmed before capture is in the checkpoint, whether in an SST
or its Pebble WAL. A concurrent transaction may fall on either side of the
capture point. The Raft WAL is not uploaded.

## Publication and failure ordering

`internal/infra/backup/manager.go` enforces this order:

1. Finish the local Pebble checkpoint and enumerate/hash its files.
2. Read the previous manifest and list destination objects.
3. Upload every object the new manifest needs but storage lacks.
4. Publish the new manifest.
5. Prune checkpoint objects and obsolete export objects no longer referenced.

No deletion precedes step 4. If upload or manifest publication fails, the
previous manifest still points to its intact objects. An upload can leave an
unreferenced object, which a later post-publication prune removes. Pruning is
best effort; failures leave harmless garbage and are retried on a later run.
The prune owns only the current chain's key shapes, so a nested `bucket-id`
listing does not authorize deleting another chain's objects. Distinct chains
should nevertheless use disjoint namespaces.

A checkpoint file is verified on download against its manifest size and the
SHA-256 embedded in its content-addressed key. This detects corrupted objects;
it does not authenticate a coordinated rewrite of the manifest and objects.
Backup access also exposes the audit key in the full checkpoint and requires
appropriate object-store access controls.

## Restore

The online restore service downloads the manifest and checkpoint files into a
fresh staging directory, validates file paths through a root-scoped filesystem
capability, and checks size and hash. A manifest without checkpoint files or
with legacy incremental exports or a policy sidecar is rejected before staging
can be finalized. `restore validate` runs Checker and is required by the gRPC
finalization path. Offline `ledgerctl store bootstrap --validate` runs the same
Checker, but its flag is optional. A failed offline download can leave partial
staging files; the next attempt resets that directory. Neither path activates
failed staging.

Cross-cluster preparation preserves business and audit state, including the
once-committed audit key, but removes source-cluster identity, peers, backup
jobs, cache and Bloom blocks. It preserves the checkpoint's applied index as the
new genesis boundary (or 1 for a genesis checkpoint). Query-checkpoint rows
are marked as restored because their physical read-index directories are not
part of this backup. The `RESTORED` marker instructs the new node to recover
FSM counters from Pebble and create a new Raft WAL snapshot at that boundary.
Joiners therefore receive checkpoint sync instead of replaying source-domain
entries onto an empty store. The per-replica read store is rebuilt locally.

`internal/infra/backup/restore.go` still contains the old export replay code for
rolling RC compatibility with previously emitted orders, but new backup and
restore entry points never call it. Legacy backup artifacts are intentionally
unsupported; see the migration section in `docs/ops/backup-restore.md`.

## Published compatibility surfaces

`ClusterService.IncrementalBackup` remains a published RPC during the RC. The
new server routes it to the normal full-checkpoint executor and returns zero
legacy export counts. `ledgerctl store incremental-backup` is a hidden alias
for `store backup`. The operator uses an existing `schedule.incremental` cron
as the checkpoint cadence during upgrade, then schedules only Full runs;
operators should move that cadence to `schedule.full`. In-flight Incremental
runs are allowed to finish. Existing `IncrementalBackupOrder` Raft
numbers and FSM application remain so old and new voters interpret committed
entries identically during a rolling upgrade; new leaders never propose them.
The old in-memory exporter is currently retained for these compatibility tests
but is unreachable from new service and CLI paths.

## Tests and limits

- `internal/infra/backup/crash_safety_test.go` pins manifest publication after
  uploads and before deletion, content-addressed object immutability, and
  failure retry behavior.
- `internal/storage/dal/store_operations_test.go` proves a pre-capture
  committed write survives after checkpoint WAL files are removed in an idle
  fixture, showing that the backup flush materializes it into SSTs.
- `tests/e2e/business/backup_s3_test.go` and `backup_azure_test.go` cover
  repeated checkpoint publication against object stores.
- `tests/e2e/cluster/restore*_test.go` cover cross-cluster recovery of business
  data, audit and operational projections from later checkpoints.
- Operator E2E and Antithesis restore cycles take a second checkpoint after
  additional writes, destroy the source, and verify restored state.

The leader performs all hashing and uploading; compaction can replace SSTs and
increase transfer volume. Backup is physical disaster recovery, not a logical
point-in-time query feature. Historical restore points require separate
`bucket-id` namespaces or external versioning/archive orchestration.
