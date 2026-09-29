# Storage

The persistence layer (`internal/storage/dal`, `internal/storage/wal`, `internal/storage/spool`, `internal/storage/rocksdbcfg`). One RocksDB database backs the main store (WAL enabled); a second backs the read index (WAL disabled, fully rebuildable). The usage projection is a third RocksDB database. The spool sits between Raft commit and FSM apply.

## Documents

| Document | Description |
|----------|-------------|
| [storage.md](storage.md) | WAL, snapshot/compaction boundaries, runtime stores, persistence, and recovery. |
| [follower-sync.md](follower-sync.md) | Checkpoint streaming, session lifetime, SHA-256 verification, retries, and WAL reclamation after snapshot install. |
| [storage-drivers.md](storage-drivers.md) | RocksDB storage driver characteristics, configuration, and write session ownership. |
| [spool.md](spool.md) | Committed entry buffer between Raft and FSM synchronization. |
| [range-bounds.md](range-bounds.md) | Exclusive upper bounds on sequence-keyed prefix scans, and the `+1` overflow guards that go with them. |

## Related

- [Consensus](../consensus/) — Raft layer that writes the WAL and consumes the spool.
- [FSM](../fsm/) — apply path that turns committed entries into RocksDB writes.
- [Attributes](../attributes/) — caches in front of the RocksDB main store.
