# Restore Mode

## Overview

Restore mode allows bootstrapping a fresh cluster from a backup. The flow is:

1. Start the server with `--restore` (no Raft, only RestoreService gRPC)
2. Upload a backup tar via `ledgerctl restore upload`
3. Validate the staged data with `ledgerctl restore validate`
4. Preview the staged data with `ledgerctl restore preview`
5. Finalize with `ledgerctl restore finalize` (swaps staging to live, writes marker)
6. Restart without `--restore` (node detects RESTORED marker, bootstraps from restored data)

## Architecture

### Restore Mode Server

When started with `--restore`, the server runs a minimal subset of the normal application:

- **gRPC server**: Only `RestoreService` + health check
- **HTTP server**: Only `/health` endpoint
- **Not started**: WAL, Raft Node, Transport, Spool, Cache, Attributes, KeyStore, Admission, Controller, HealthChecker, ClusterService, BucketService, SnapshotService

The `RestoreModule` (in `internal/application/module_restore.go`) validates that the data directory is fresh (no `CURRENT_CHECKPOINT` file exists).

### Data Flow

```
Client                  Restore Server                     Disk
  |                          |                               |
  |--- UploadBackup -------->|                               |
  |    (streaming tar)       |--- ExtractTar() ------------->|
  |                          |    {dataDir}/restore-staging/  |
  |<-- UploadBackupResponse--|                               |
  |                          |                               |
  |--- ValidateRestore ----->|                               |
  |    (stream events)       |--- check.Checker.Check() --->|
  |<-- progress/error -------|    (read-only staging DB)     |
  |                          |                               |
  |--- PreviewRestore ------>|                               |
  |<-- summary --------------|--- OpenReadOnly(staging) ---->|
  |                          |                               |
  |--- FinalizeRestore ----->|                               |
  |                          |--- Write RESTORED marker ---->|
  |                          |--- HardLink staging --------->|
  |                          |    -> checkpoints/0           |
  |                          |--- Write CURRENT_CHECKPOINT ->|
  |                          |--- Remove staging ----------->|
  |<-- response -------------|                               |
```

After finalize, the server stays running but will refuse new uploads. The Raft loop is not started in restore mode, so the server is effectively idle. Restart without `--restore` to use the restored data.

### RESTORED Marker

The `RESTORED` file is a JSON file written to the data directory during `FinalizeRestore`:

```json
{
  "lastAppliedIndex": 12345,
  "lastAppliedTimestamp": 1700000000000000
}
```

- `lastAppliedIndex`: The Raft index of the last applied entry in the backup
- `lastAppliedTimestamp`: The HLC timestamp (microseconds) of the last applied entry

### Post-Restore Bootstrap

On normal startup (without `--restore`), the node detects the RESTORED marker in `NewNode()`:

1. If WAL is empty (first start) AND `RESTORED` marker exists:
   - Create a WAL snapshot at `marker.LastAppliedIndex` with `ConfState{Voters: [nodeID]}`
   - Remove the marker file
   - Continue with normal Raft startup

2. If WAL is empty AND no marker: fall through to normal bootstrap/join/error logic

This ensures the Raft index is properly aligned with the restored data.

### Safety Guarantees

- **Fresh directory required**: Restore mode refuses to start if `CURRENT_CHECKPOINT` exists
- **SHA256 verification**: Upload verifies the tar archive hash on the server side
- **Integrity check**: `ValidateRestore` runs the full integrity checker (hash chain, volumes, metadata)
- **Atomic finalize**: Checkpoint placement uses `HardLink()` for atomic directory swaps
- **Idempotent marker**: The RESTORED marker is consumed exactly once on the next normal boot

## Files

| File | Description |
|------|-------------|
| `misc/proto/restore.proto` | RestoreService proto definition |
| `internal/proto/restorepb/` | Generated proto code |
| `internal/application/grpc_restore_server.go` | RestoreService gRPC implementation |
| `internal/application/module_restore.go` | Minimal fx module for restore mode |
| `internal/storage/tarutil/extract.go` | Shared tar extraction utility |
| `internal/storage/data/store_readonly.go` | Read-only Pebble store opener |
| `internal/service/node/restored_marker.go` | RESTORED marker read/write/remove |
| `cmd/ledgerctl/restore.go` | CLI parent command + client helper |
| `cmd/ledgerctl/restore_upload.go` | `restore upload` CLI command |
| `cmd/ledgerctl/restore_validate.go` | `restore validate` CLI command |
| `cmd/ledgerctl/restore_preview.go` | `restore preview` CLI command |
| `cmd/ledgerctl/restore_finalize.go` | `restore finalize` CLI command |
