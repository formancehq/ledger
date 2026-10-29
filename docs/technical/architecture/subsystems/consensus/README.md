# Consensus

The replication and ordering layer (`internal/infra/node`, `internal/infra/transport`). A single Raft group orders every mutation across the cluster; the global log enables system-level atomic bulk operations over multiple ledgers; the hybrid logical clock provides monotonic timestamps across leader changes. Membership retries retain their operation's committed waiter through WAL persistence and durable FSM application.

## Documents

| Document | Description |
|----------|-------------|
| [raft-consensus.md](raft-consensus.md) | Raft consensus implementation, CP failure model, quorum sizing, leader election and generation-fenced readiness, restart-safe ReadIndex request correlation, retained-log catch-up, snapshot transfer and received-snapshot suffix durability, stalled-stream detection and reconnection, failed-start cleanup, deadline-independent shutdown requests, hook joins, commit drain and terminal readiness callback cancellation and joins, transport shutdown and cleanup, and fail-stop on force-removal persistence failure. |
| [global-log.md](global-log.md) | Two-level log architecture enabling system-level atomic bulk operations. |
| [hybrid-logical-clock.md](hybrid-logical-clock.md) | Monotonic HLC timestamps across leader changes and clock skew. |
| [removed-member-registry.md](removed-member-registry.md) | Universal member-identity invariant, committed-only peer routing, and replicated `(nodeID, instanceID)` set that prevents a removed member from silently rejoining and being auto-promoted; replay-safe per-event admission barriers, lookup costs, removal write ordering and durable restart states. |
| [controlled-shutdown.md](controlled-shutdown.md) | Raft-ordered admission cutoff, local persistence confirmation, restart behavior, rolling-upgrade capability gate, and operator completion contract. |

## Related

- [FSM](../fsm/) — what every committed log entry feeds into.
- [Storage](../storage/) — WAL, snapshots, and spool that back the Raft layer.
