# Snapshot File Transfer

Follower synchronization uses the internal `SnapshotService` gRPC API to copy
a temporary Pebble checkpoint from a donor node. `PrepareSnapshot` creates the
checkpoint and returns a manifest. Concurrent `FetchFile` calls then stream the
manifest's relative paths before `CloseSession` removes the checkpoint.

## Filesystem containment

`FetchFileRequest.path` crosses an internal network boundary and must never
grant filesystem authority beyond the prepared checkpoint. The server applies
two complementary controls:

1. `filepath.IsLocal` rejects empty, absolute, and parent-escaping paths before
   streaming starts. It also rejects reserved device names on Windows.
2. `streamOneFile` opens the final file through `os.Root`, rooted at the session
   checkpoint. The rooted open prevents a path from escaping through a symbolic
   link or through a concurrent rename between validation and open.

The rooted open is the authoritative containment control. Lexical validation is
kept at the RPC boundary so malformed paths receive `InvalidArgument` instead
of being interpreted as missing files. Valid nested regular files remain
supported. A symlink is followed only when its target is expressed relatively
and remains beneath the checkpoint root; an absolute symlink target is rejected
even when it points inside the root. Pebble checkpoints contain hard links or
copied files rather than symlinks, so this distinction does not arise in normal
checkpoint data.

## Follower-side receiving

The manifest returned by a donor is network-supplied input. The follower
validates every manifest path with `filepath.IsLocal` immediately after
`PrepareSnapshot`, before scanning resume state or requesting a file. A second
validation in the file fetcher protects direct callers.

Resume scans, temporary-file creation, hashing, and the final rename all use an
`os.Root` opened on the follower's staging directory. This is the mirror of the
serving-side containment boundary: neither a manifest traversal nor a symlink
already present in the staging directory can redirect a read or write outside
that directory. Temporary files are removed on stream, hash, close, or rename
failure.

These checks do not turn the checkpoint into a general sandbox: `os.Root` does
not block bind mounts, device files, or filesystem boundaries. Snapshot
checkpoints are created by the ledger process from Pebble data and must not be
populated from caller-controlled filesystem trees.

## Owning code

| Responsibility | Location |
| --- | --- |
| Snapshot session and lexical path validation | `internal/adapter/grpc/server_snapshot.go` |
| Rooted file open and chunk streaming | `internal/adapter/grpc/file_streaming.go` |
| Manifest validation and rooted receive-side writes | `internal/application/ctrl/file_receiver.go`, `internal/application/ctrl/file_fetcher.go` |
| Snapshot protocol | `misc/proto/snapshot.proto` |
