# Consensus

The replication and ordering layer (`internal/infra/node`, `internal/infra/transport`). A single Raft group orders every mutation across the cluster; the global log enables system-level atomic bulk operations over multiple ledgers; the hybrid logical clock provides monotonic timestamps across leader changes.

## Documents

| Document | Description |
|----------|-------------|
| [raft-consensus.md](raft-consensus.md) | Raft consensus implementation, leader election, log replication, and snapshot transfer. |
| [global-log.md](global-log.md) | Two-level log architecture enabling system-level atomic bulk operations. |
| [hybrid-logical-clock.md](hybrid-logical-clock.md) | Monotonic HLC timestamps across leader changes and clock skew. |
| [removed-member-registry.md](removed-member-registry.md) | Replicated `(nodeID, instanceID)` set that prevents a removed member from silently rejoining and being auto-promoted. |

## Related

- [FSM](../fsm/) — what every committed log entry feeds into.
- [Storage](../storage/) — WAL, snapshots, and spool that back the Raft layer.
