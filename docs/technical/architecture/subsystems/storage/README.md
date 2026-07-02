# Storage

The persistence layer (`internal/storage/dal`, `internal/storage/wal`, `internal/storage/spool`, `internal/storage/pebblecfg`). One Pebble database backs the main store (WAL enabled); a second backs the read store (WAL disabled, fully rebuildable). The spool sits between Raft commit and FSM apply.

## Documents

| Document | Description |
|----------|-------------|
| [storage.md](storage.md) | WAL, snapshots, runtime stores, persistence, and recovery. |
| [storage-drivers.md](storage-drivers.md) | Pebble storage driver characteristics and configuration. |
| [spool.md](spool.md) | Committed entry buffer between Raft and FSM synchronization. |

## Related

- [Consensus](../consensus/) — Raft layer that writes the WAL and consumes the spool.
- [FSM](../fsm/) — apply path that turns committed entries into Pebble writes.
- [Attributes](../attributes/) — caches in front of the Pebble main store.
