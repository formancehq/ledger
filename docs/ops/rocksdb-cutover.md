# Ledger v3 RocksDB cutover (first version)

Ledger v3 now opens its primary DAL, read index, and usage projection with
RocksDB. RocksDB cannot open Pebble SSTs. This is a coordinated offline
cutover: stop all Ledger nodes, preserve a consistent snapshot of each node's
**whole** data and Raft WAL directories, convert every database directory,
then start the new binary. Do not run mixed Pebble and RocksDB binaries against
one data directory.

The offline converter is a separate module in `tools/pebble-to-rocksdb` so the
server's Go module has no Pebble dependency. It requires Pebble v2 source files
readable by its pinned module and RocksDB 11 development/runtime libraries.
Build it using that module's README. Run it as `pebble-to-rocksdb --stopped
--kind KIND --source SOURCE --destination DEST`. It refuses an existing
`DEST`, writes `DEST.incomplete`, compares ordered key/value counts and SHA-256
on both engines, rescans the source, and publishes `DEST` only after those
checks pass. It never removes the source. An existing `.ready` checkpoint
marker is copied only after verification.

## Directories to convert on each node

| Kind | Source | New destination before cutover |
| --- | --- | --- |
| `main` | `<data-dir>/live` | `<data-dir>/live.rocksdb` |
| `read` | `<read-index-dir>/readindex` (default `<data-dir>/read-indexes/readindex`) | sibling `readindex.rocksdb` |
| `usage` | `<data-dir>/usage/usagedb` | sibling `usagedb.rocksdb` |
| `main` | each numeric `<data-dir>/checkpoints/<id>` | sibling `<id>.rocksdb` |
| `main` | each `<data-dir>/query-checkpoints/<id>/main` | sibling `main.rocksdb` |
| `read` | each `<data-dir>/query-checkpoints/<id>/readindex` | sibling `readindex.rocksdb` |

Check for any other configured read-index directory. Do not convert an
in-flight `*.tmp` directory; resolve or discard it using the existing
checkpoint recovery procedure before cutover. The converter does not migrate
Raft WAL files: preserve them unchanged in the whole-volume snapshot and
leave them in place for the new server. A backup restore archive that contains
Pebble checkpoint files needs conversion before a RocksDB server opens it.

For example, with the server stopped:

```sh
DATA=/path/to/ledger-data
pebble-to-rocksdb --stopped --kind main \
  --source "$DATA/live" --destination "$DATA/live.rocksdb"
pebble-to-rocksdb --stopped --kind read \
  --source "$DATA/read-indexes/readindex" \
  --destination "$DATA/read-indexes/readindex.rocksdb"
pebble-to-rocksdb --stopped --kind usage \
  --source "$DATA/usage/usagedb" --destination "$DATA/usage/usagedb.rocksdb"
```

Repeat for every numbered checkpoint and query checkpoint using the table.
Inspect every converter result and confirm there is no `.incomplete` directory.
While the servers are still stopped, rename each original database directory to
an archival `.pebble` name, then rename its verified `.rocksdb` sibling to the
original path. Keep the whole-volume snapshot outside the active data tree.
The old numbered checkpoints must no longer occupy their numeric names, since
startup scans those names. Start one node, check that it opens all three stores,
that the Raft applied index aligns with its WAL, that query checkpoint reads
work, and that the read/usage projections advance. Then start the remaining
nodes and validate follower catch-up and a new backup/restore on a test target.

If validation fails after new writes have been accepted, restore the **whole**
pre-cutover snapshot (data and Raft WAL together) before returning to a Pebble
binary. Renaming only `live` back would pair an old applied index with a newer
Raft WAL and is unsafe.

## Configuration and behavior limits

The primary store rejects configured `walFailoverDir`, nonzero
`walMinSyncInterval`, enabled `valueSeparation`, and `disableWAL`: their Pebble
behavior has not been qualified on RocksDB. The default configuration works.
The read index and usage store remain rebuildable projections; the read index
still writes without a WAL and flushes before publishing a checkpoint. The
read index currently uses bounded scans without Pebble's ledger-scoped prefix
bloom optimization. RocksDB compression profiles approximate the former
Pebble names (`fastest` and `fast` use Snappy; `balanced` and `good` use Zstd).
The replay checker folds merge operands in Go because the current grocksdb
native merge callback crashes on close; qualify replay throughput before
production use.

The server and CLI require CGO plus a compatible RocksDB 11 library. The
previous CGO-disabled and Windows cross-builds are not available in this
version; see the build workflow for supported targets.
