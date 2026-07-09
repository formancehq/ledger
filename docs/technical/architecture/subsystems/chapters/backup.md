# Backup and Restore

## Overview

A **backup** is a snapshot of the **entire Pebble database**, exported to durable storage for disaster recovery. This is distinct from [cold storage](cold-storage.md), which only exports archived *chapters* of logs. Two different access patterns, two different code paths, two different concerns:

| | Cold storage | Backup |
|-|-|-|
| Scope | A sealed chapter's logs + audit entries | The whole Pebble database |
| Granularity | One file per chapter | A manifest + many SST segments |
| Trigger | Per-chapter Raft order (`ArchiveChapter`) | Per-destination Raft order (`Backup`), often scheduled |
| Recovery target | Selective revert via [receipts](receipts.md) | Whole-cluster reconstruction |

Backups are Raft-coordinated so the cluster can't double-write to the same destination and so a failure mid-upload is recoverable.

Source: `internal/infra/backup/` (lower-level storage + manifest) and `internal/application/backup/` (FSM-side orchestration).

## Drivers

| Driver | Build tag | File |
|--------|-----------|------|
| S3 | `s3` | `internal/infra/backup/s3.go` (stub at `s3_disabled.go` without the tag) |
| Azure Blob | `azure` | `internal/infra/backup/azure.go` (stub at `azure_disabled.go` without the tag) |

`internal/infra/backup/storage.go:51-57` dispatches on the `kind` field of `StorageConfig`: only `"s3"` and `"azure"` are recognised. **There is no filesystem driver for backup** (filesystem is supported for [cold storage](cold-storage.md), but backups must go to a real object store). The default light binary therefore has no functional backup target — operators who need backup build with `just build-full` or the matching `-tags` flag.

## How a backup is taken

A Pebble *checkpoint* is the engine primitive: hard-link every live SST file into a separate directory at a point in time. The checkpoint is **quasi-free** (no copy), and writes to the live database keep going untouched.

```mermaid
sequenceDiagram
    participant Op as Operator / Cron
    participant FSM
    participant Peb as Pebble
    participant Exec as Executor<br/>(leader)
    participant Dst as Backup destination

    Op->>FSM: BackupOrder(destination)
    FSM->>FSM: lock destination, status=START
    FSM-->>Exec: signal to run
    Exec->>Peb: Checkpoint(tmpdir)
    Peb-->>Exec: SST file set
    Exec->>Exec: diff vs previous manifest
    Exec->>Dst: upload new/changed SST files
    Exec->>Dst: delete obsolete SST files
    Exec->>Dst: write new manifest
    Exec->>FSM: CompleteBackupOrder(stats)
    FSM->>FSM: status=COMPLETE, persist manifest pointer
```

The work is split between **Raft-coordinated lifecycle orders** (`BackupOrder`, `CompleteBackupOrder`, `FailBackupOrder` at `raft_cmd.proto:431-593`) and **executor work on the leader** (`internal/infra/backup/manager.go:40-180`). The FSM never blocks on the upload — it only records start, success, and failure.

### Per-destination mutual exclusion

Two simultaneous backups to the same destination are rejected at FSM apply time. The mutex is keyed by a hash of the `BackupDestination` (driver kind + endpoint + bucket + path), so the same Pebble database can back up to S3 and a filesystem in parallel, but cannot run two S3 backups to the same bucket.

### Manifest + incremental segments

A backup is **incremental by default**: the executor diffs the current checkpoint's SST file set against the previous manifest, uploads only the new files, and tags the old files as still-needed in the new manifest. Files that are no longer referenced by any manifest are deleted from the destination.

The manifest itself (`internal/infra/backup/manifest.go`) records:

- The Pebble checkpoint timestamp and applied Raft index.
- The last audit + log sequence numbers.
- The file map (`name → checksum → size`).
- Any exports (incremental segments not yet rolled into a checkpoint).

A fresh backup against an empty destination is just a "full" backup with an empty previous manifest. A backup right after a previous one transfers only the deltas.

## Restore

`internal/infra/backup/restore.go` is the entry point. The flow is conceptually the inverse:

1. Read the manifest from the destination.
2. Download every SST file the manifest references into a fresh Pebble directory.
3. Apply any incremental exports on top (`ApplyExports`).
4. Boot the node against the restored directory.

After the restore, the node rejoins (or initialises) the Raft cluster as a fresh peer. The standard config validation (`internal/bootstrap/config_validation.go`) verifies that the restored `cluster-id` matches the cluster the node is supposed to be joining.

A restore is **a node-level disaster-recovery operation**, not an in-cluster operation — it is not driven by a Raft order. Operators script it (or invoke it via the Operator) when a cluster needs to be rebuilt from cold.

## Scheduling

The Operator's `Backup` CRD (`misc/operator/api/v1alpha1/`) wraps backups behind a `BackupSchedule` with **two separate cron fields** — `Full` and `Incremental` — so operators can run a full checkpoint less often than the incremental segments. The Operator submits a `BackupOrder` (full) or `IncrementalBackupOrder` at each cron tick; the FSM enforces mutual exclusion; the executor on the leader does the upload. One-off backups can also be triggered manually through the same gRPC surface.

`IncrementalBackupOrder` is the right primitive for tight RPO targets — it flushes the in-progress segment without taking a fresh full checkpoint.

## Performance characteristics

- **Pebble checkpoint is hard-link-based.** Almost no I/O cost; the live database keeps serving writes.
- **Uploads are leader-only.** Followers are not involved. This avoids fan-out but means the leader's bandwidth caps backup throughput.
- **Incremental diffing is by SST file identity.** Pebble's compaction rewrites SSTs, so a heavily-churned database may re-upload more than a quiet one — there is no key-level diffing.
- **No backups during snapshot transfer.** A follower receiving a Raft snapshot is in a transient state; the leader does not initiate a backup while a follower is mid-sync.

## What backup doesn't do

- **It does not back up cold storage.** Cold storage is durable by the driver's own guarantees. A node restore reconstructs only the hot Pebble database; archived chapters stay in their cold-storage location and the restored cluster reads them through the same `coldstorage.Reader` interface.
- **It does not provide point-in-time queries.** A backup is a *Pebble* snapshot, not a logical "as of this transaction" snapshot. For point-in-time logical reads, use [query checkpoints](../read-path/query-checkpoints.md) instead.
- **It does not retain by policy.** Retention (how many manifests to keep, how long incremental segments live) is operator-driven. The system will happily back up to the same destination forever.

## Where to look in the code

| Concern | File |
|---------|------|
| FSM lifecycle orders (Backup, IncrementalBackup, Complete, Fail) | `misc/proto/raft_cmd.proto:431-593` |
| FSM-side orchestration | `internal/application/backup/orchestrator.go` |
| Backup manager (Pebble checkpoint, diff, upload) | `internal/infra/backup/manager.go:40-180` |
| Manifest | `internal/infra/backup/manifest.go` |
| Storage abstraction (filesystem / S3 / Azure) | `internal/infra/backup/storage*.go` |
| Restore | `internal/infra/backup/restore.go` |
| `Backup` / `BackupRun` CRDs | `misc/operator/api/v1alpha1/` |
