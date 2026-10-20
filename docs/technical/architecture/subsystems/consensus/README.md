# Consensus

The replication and ordering layer (`internal/infra/node`, `internal/infra/transport`). A single Raft group orders every mutation across the cluster; the global log enables system-level atomic bulk operations over multiple ledgers; the hybrid logical clock provides monotonic timestamps across leader changes.

## Documents

| Document | Description |
|----------|-------------|
| [raft-consensus.md](raft-consensus.md) | Raft consensus implementation, CP failure model, quorum sizing, leader election and generation-fenced readiness, retained-log catch-up, snapshot transfer, stalled-stream detection and reconnection, failed-start cleanup, deadline-independent shutdown requests, hook joins, commit drain and terminal readiness callback cancellation and joins, transport shutdown and cleanup, and fail-stop on force-removal persistence failure. |
| [write-admission-and-pebble-readiness.md](write-admission-and-pebble-readiness.md) | Why Raft WAL quorum and Pebble readiness differ, the checkpointable-quorum invariant, joint-consensus rules, `go.etcd.io/raft/v3` storage acknowledgements, and the remaining admission race. |
| [global-log.md](global-log.md) | Two-level log architecture enabling system-level atomic bulk operations. |
| [hybrid-logical-clock.md](hybrid-logical-clock.md) | Monotonic HLC timestamps across leader changes and clock skew. |
| [removed-member-registry.md](removed-member-registry.md) | Universal member-identity invariant, committed-only peer routing, and replicated `(nodeID, instanceID)` set that prevents a removed member from silently rejoining and being auto-promoted; force-removal write ordering and durable restart states. |

## Related

- [FSM](../fsm/) — what every committed log entry feeds into.
- [Storage](../storage/) — WAL, snapshots, and spool that back the Raft layer.
