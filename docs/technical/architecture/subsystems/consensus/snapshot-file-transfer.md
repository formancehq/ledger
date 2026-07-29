# Snapshot File Transfer

Follower synchronization uses the internal `SnapshotService` gRPC API to copy
a temporary Pebble checkpoint from a donor node. `PrepareSnapshot` creates the
checkpoint and returns a manifest. Concurrent `FetchFile` calls then stream the
manifest's relative paths before `CloseSession` removes the checkpoint.

## Filesystem containment

`FetchFileRequest.path` crosses an internal network boundary and must never
grant filesystem authority beyond the prepared checkpoint. The server applies
two complementary controls:

1. `filepath.IsLocal` rejects empty, absolute, reserved, and parent-escaping
   paths before streaming starts.
2. `streamOneFile` opens the final file through `os.Root`, rooted at the session
   checkpoint. The rooted open prevents a path from escaping through a symbolic
   link or through a concurrent rename between validation and open.

The rooted open is the authoritative containment control. Lexical validation is
kept at the RPC boundary so malformed paths receive `InvalidArgument` instead
of being interpreted as missing files. Valid nested regular files remain
supported, and symlinks are followed only when their target remains beneath the
checkpoint root.

These checks do not turn the checkpoint into a general sandbox: `os.Root` does
not block bind mounts, device files, or filesystem boundaries. Snapshot
checkpoints are created by the ledger process from Pebble data and must not be
populated from caller-controlled filesystem trees.

## Owning code

| Responsibility | Location |
| --- | --- |
| Snapshot session and lexical path validation | `internal/adapter/grpc/server_snapshot.go` |
| Rooted file open and chunk streaming | `internal/adapter/grpc/file_streaming.go` |
| Snapshot protocol | `misc/proto/snapshot.proto` |
