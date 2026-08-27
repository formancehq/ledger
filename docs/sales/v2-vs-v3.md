# Problems Solved from Ledger v2

This document summarizes the main architectural and operational differences between Ledger v2 and Ledger v3. It is intentionally high level. For implementation details, follow the linked technical documentation rather than treating this page as an engineering source of truth.

## Summary

| Category | v2 | v3 |
|---|---|---|
| Database | PostgreSQL dependency | Embedded Pebble storage |
| Replication | PostgreSQL-centered consistency and failover | Native Raft consensus |
| Deployment | Application plus external database | Self-contained ledger nodes |
| Ordering | Per-ledger API model | One global Raft ordering domain |
| Recovery | Database-oriented recovery | Application-level snapshots, WAL, checkpoints, and synchronization |
| Observability | Application and database telemetry split across systems | Integrated OpenTelemetry metrics/tracing and optional profiling |
| Balance state | SQL rows updated under database concurrency control | Deterministic FSM-maintained volume projections in Pebble |

For the current architecture, start with the [canonical architecture overview](../technical/architecture/overview.md).

---

## 1. Embedded storage instead of PostgreSQL

### v2

Ledger v2 depended on PostgreSQL for persistent state. Operating Ledger therefore also meant provisioning, upgrading, monitoring, backing up, and scaling a database service.

### v3

Ledger v3 embeds Pebble and manages its own persistent state locally. Raft WAL data, FSM state, projections, checkpoints, and related lifecycle data are owned by the ledger process rather than an external SQL database.

This reduces external infrastructure and keeps persistence behavior under the same operational boundary as the ledger itself.

See [Storage and Persistence](../technical/architecture/subsystems/storage/) for the current storage model.

---

## 2. Native distributed consensus with Raft

### v2

Consistency, replication, and failover depended primarily on the PostgreSQL deployment and its surrounding operational setup.

### v3

Ledger v3 uses a single Raft group. Accepted mutations are ordered by consensus and applied through the deterministic FSM on every node.

This gives the ledger explicit ownership of:

- leader election;
- replication order;
- quorum behavior;
- snapshot and WAL coordination;
- membership changes;
- follower catch-up.

See [Consensus](../technical/architecture/subsystems/consensus/) for the detailed design.

---

## 3. One global ordering domain

Ledger v3 uses one Raft group for all ledgers managed by the cluster. This gives mutations a single replicated ordering domain and enables commands to contain actions for more than one ledger when the API surface supports it.

This is particularly relevant to the gRPC `Apply` API, which can submit multiple actions targeting different ledgers as one atomic command.

See [Global Log Architecture](../technical/architecture/subsystems/consensus/global-log.md).

---

## 4. Self-contained builds and deployments

Pebble is embedded in the Go process and does not require a separate PostgreSQL server. This simplifies local development, container deployment, and infrastructure topology.

Operationally, this does **not** mean persistence is trivial: WAL placement, checkpoints, backups, storage capacity, and restore procedures remain important. Those concerns are documented under [Operations](../ops/) and the [storage subsystem](../technical/architecture/subsystems/storage/).

---

## 5. Write path and deterministic state updates

Ledger v3 separates admission from deterministic application:

1. the leader validates and prepares a proposal;
2. required state is resolved before proposal where needed;
3. Raft orders and replicates the command;
4. the FSM applies the accepted command deterministically;
5. persisted projections are updated as part of the controlled apply path.

A follower receiving a write request forwards it to the current leader before admission.

See [Admission](../technical/architecture/subsystems/admission/) and [FSM](../technical/architecture/subsystems/fsm/).

---

## 6. Recovery and synchronization

Ledger v3 owns recovery at the application level. The system combines Raft WAL/snapshot state, Pebble checkpoints, synchronization with peers, and spool/replay mechanisms to restore and catch up a node.

The exact lifecycle is intentionally documented outside this sales comparison because it evolves with the storage and chapter-management implementation.

See:

- [Storage and Persistence](../technical/architecture/subsystems/storage/)
- [Chapters](../technical/architecture/subsystems/chapters/)
- [Consensus](../technical/architecture/subsystems/consensus/)

---

## 7. Integrated observability

Ledger v3 exposes OpenTelemetry-compatible metrics and tracing, with optional continuous profiling support. Ledger-specific instrumentation covers areas such as admission, Raft, storage, queues, HTTP handling, and background processing.

For metric names, dashboards, naming modes, profiling, and operational guidance, use [Monitoring](../ops/monitoring.md).

---

## 8. Current balance storage

One important difference from earlier v3 prototypes is the current balance representation.

Ledger v3 does **not** model the current balance as an application-level append-only chain of per-transaction balance diffs that must be summed on every read.

Instead, the FSM maintains current input/output volumes as a persisted projection. For a given ledger/account/asset key, the current `VolumePair` is updated deterministically as transactions are applied. Later updates replace the previous logical value for that key; historical business events remain represented by the audit/log history rather than by exposing every old balance value as a current-balance row.

Consequences include:

- current balance reads do not require an O(n) scan over all historical transaction diffs;
- state updates are serialized by Raft/FSM ordering instead of PostgreSQL row locks;
- the current balance projection remains bounded per account/asset key;
- audit history and current balance state remain separate concerns.

See [Storage and Persistence](../technical/architecture/subsystems/storage/storage.md#what-the-store-persists) and [FSM](../technical/architecture/subsystems/fsm/).

---

## 9. Transaction volume snapshots

Ledger v3 keeps `postCommitVolumes` on transactions as the historical post-commit snapshot associated with that transaction. Pre-commit/effective volume variants from the older API model are not all embedded in every transaction response.

This keeps historical transaction responses stable without requiring those values to be recomputed from later account state.

For API behavior, use the generated API documentation and the relevant [API subsystem documentation](../technical/architecture/subsystems/api/).

---

## 10. HTTP bulk versus gRPC cross-ledger apply

The API surfaces do not have identical scope.

### gRPC

The gRPC `Apply` call can carry multiple actions targeting different ledgers in one command. When accepted as one command, those actions share the same atomic FSM application.

### HTTP

The HTTP bulk route is ledger-scoped. The `ledgerName` comes from the request path and is applied to every bulk element by the handler.

`atomic=true` therefore means **atomic within that HTTP route's ledger**. It does not make an HTTP bulk request cross-ledger.

For cross-ledger atomic batches, use the gRPC `Apply` surface.

See [HTTP API](../technical/architecture/subsystems/api/http-api.md), [gRPC API](../technical/architecture/subsystems/api/grpc-api.md), and [Global Log Architecture](../technical/architecture/subsystems/consensus/global-log.md).

---

## 11. Read consistency is explicit

Ledger v3 supports different read-consistency modes. Consistent reads establish the required Raft barrier and wait for local application to catch up. Requests using `x-consistency: stale` may serve local state without that barrier and can therefore lag the leader.

See [Read Path](../technical/architecture/subsystems/read-path/).

---

## 12. Rebuildable read-side projections

Some query-oriented state is maintained asynchronously as rebuildable projections rather than being treated as the authoritative business source of truth.

This includes indexing and usage-oriented stores. The audit chain and the primary replicated state remain the reference for business integrity.

See:

- [Indexer](../technical/architecture/subsystems/indexer/)
- [Usage](../technical/architecture/subsystems/usage/)
- [Audit vs Technical State](../technical/architecture/audit-vs-technical-state.md)

---

## Migration considerations

Moving from v2 to v3 is not only a database substitution. Review at least:

1. API differences and client behavior;
2. data migration/import strategy;
3. cluster and quorum topology;
4. persistent volume and WAL placement;
5. backup and restore procedures;
6. monitoring and alerting;
7. consistency expectations for reads;
8. whether callers require cross-ledger atomic operations and therefore need the gRPC API.

The authoritative operational and technical details live under [`docs/technical/`](../technical/) and [`docs/ops/`](../ops/).

## Conclusion

Ledger v3 moves responsibility for ordering, persistence, recovery, and much of the operational lifecycle from an external SQL database into the ledger system itself. The result is a substantially different architecture: embedded storage, native Raft consensus, deterministic FSM application, explicit read consistency, and rebuildable read-side projections.

This document should remain a product-level comparison. When implementation details matter, follow the subsystem links above rather than extending this page into a second architecture specification.
