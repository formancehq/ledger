# Checkpoint Restore Parity

## Invariant

A backup captures a flushed Pebble checkpoint. Every committed effect visible at
the capture point and intended to survive cross-cluster restore must be present
in that checkpoint. A later backup captures a new complete checkpoint; restore
does not replay history between two checkpoints.

```text
live(at capture) == restore(checkpoint(at capture))
```

Compare logical business state, not byte-for-byte Pebble files. Cross-cluster
restore intentionally removes cluster-local identity and caches and re-seeds the
destination genesis. The audit chain, business projections, log, audit items,
applied proposals, and last applied HLC are captured together in one Pebble
checkpoint. The checker must validate the restored business state.

A confirmed write before the backup capture point is included. Writes concurrent
with checkpoint creation may fall on either side of the capture point and appear
no later than the next successful backup. The backup path flushes memtables before
calling Pebble `Checkpoint(...WithFlushedWAL())`, and uploads every checkpoint
file, including a Pebble WAL file if present. Raft WAL files are outside the
backup.

## Publication and restore

The backup enumerates and hashes every checkpoint file, uploads objects absent
from the content-addressed destination, publishes one manifest with the new
checkpoint and sequence metadata, then prunes unreferenced objects. The manifest
write is the publication boundary: an incomplete upload must leave the prior
manifest restorable. A failed prune may leave harmless orphans for a retry.

Both gRPC restore and offline bootstrap require checkpoint files and reject old
manifests with export segments or a separate cluster policy override. The user
must create a new checkpoint backup with the updated binary before restoring
from that destination. Compatibility with the old backup artifact is deliberately
not maintained during the RC; persisted Ledger and Raft data retain their normal
upgrade requirements.

`PrepareForBackup` discards source cluster-local state after the download and
before activation. The audit secret remains with the chain. Source Raft applied
indexes in business records remain provenance, not destination Raft certificates.
The destination genesis and index builders must use the restored checkpoint
boundary as specified in their own subsystem contracts.

## Required regression evidence

For any persisted, audited, or deletion-cascade change with restore impact:

1. Create a checkpoint, perform a meaningful committed mutation, then create a
   second checkpoint at the same destination.
2. Restore **the second checkpoint** through the production restore service or
   offline bootstrap. Do not take a third checkpoint that could mask a missing
   second capture.
3. Compare exact business values and relevant future admission behavior with
   the source, not only row counts or successful checker completion.
4. Run `CheckStore` on the restored store.
5. Where object reuse matters, compare manifest object keys for unchanged
   checkpoint files. Do not require a no-write backup to upload zero files:
   starting the backup itself persists backup-job state.

Test both S3 and Azure publication where provider behavior matters. Operator
E2E must exercise a second checkpoint and verify a post-first-checkpoint
sentinel after restore. Each Antithesis restore cycle captures a quiesced
committed state in a complete checkpoint and verifies it after restore.

## Code and test map

| Concern | Location |
|---|---|
| Backup memtable flush and checkpoint creation | `internal/infra/backup/manager.go`, `internal/storage/dal/store.go` |
| Object upload and manifest publication | `internal/infra/backup/manager.go`, `manifest.go` |
| gRPC restore download | `internal/adapter/grpc/server_restore_download.go` |
| Offline bootstrap | `cmd/ledgerctl/store/bootstrap.go` |
| Live persisted mutations | `internal/domain/processing/`, `internal/infra/state/` |
| Integrity oracle | `internal/application/check/` |
| Cross-lifecycle restore tests | `tests/e2e/cluster/restore*_test.go` |
| Model-driven restore cycles | `tests/antithesis/run_model_test.sh --restore` |

Also read [Audit-Bound vs Technical State](../../audit-vs-technical-state.md)
before deciding whether a value is authoritative, checker-verified, or
deliberately excluded from cross-cluster restore.

## Operational cluster policy

Native policy is stored in Pebble and captured in the same flushed checkpoint as
the rest of the primary store. A policy update after one backup requires a second
checkpoint before it can be restored. The old manifest-level `clusterPolicy`
override is rejected by current restore paths; no separate policy-only backup
publication exists. The manifest and object-store access controls protect this
operational state; it is not authenticated by the business audit chain.
