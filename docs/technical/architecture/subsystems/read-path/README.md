# Read Path

The CQRS read side (`internal/application/ctrl` reads, `internal/query`,
`internal/storage/readstore`). Default live reads routed through `readCtrl` use
a `ReadIndex` quorum barrier to establish an applied-state horizon. Point reads
and unfiltered account/transaction queries then use the main store only.
Filtered account/transaction queries and every log query additionally wait for
the read index to align with that horizon before iterating it. `stale` removes
only the Raft barrier, checkpoint pairs are already frozen, and audit-index and
usagestore exceptions are documented in the
[consensus matrix](../consensus/raft-consensus.md#linearizable-reads-via-readindex)
and the pipeline pages.

## Documents

| Document | Description |
|----------|-------------|
| [query-pipeline.md](query-pipeline.md) | End-to-end read flow: ReadIndex barrier, fixed Raft projection horizon, Pebble snapshots, iterator algebra, pagination, streaming. |
| [iterator-seek-contract.md](iterator-seek-contract.md) | Absolute SeekGE/SeekLE semantics across the iterator algebra, and the seekFloor/seekCeil exhaustion-proof cache. |
| [read-snapshot-consistency.md](read-snapshot-consistency.md) | Single-snapshot rule for controller reads that stitch LedgerInfo with attribute data. |
| [readstore-event-keys.md](readstore-event-keys.md) | Append-only metadata/existence event resolution, lease-bounded reclamation, and edge-triggered GC cycles. |
| [prepared-queries.md](prepared-queries.md) | Named pre-validated query templates: lifecycle, filter DSL, execution, bloom acceleration. |
| [query-checkpoints.md](query-checkpoints.md) | Point-in-time snapshots of main store and read index for historical queries. |
| [typed-metadata.md](typed-metadata.md) | Typed values, immutable primary metadata, declared schemas, and versioned index coercion. |
| [query-filter.md](query-filter.md) | Canonical HTTP QueryFilter surface: dual-format filter, parameter classification, textual/structured asymmetries, date coercion, AND-combination, audit text-only. |
| [query-profile.md](query-profile.md) | Per-request read diagnostics: server-side phase breakdown (prepare/execute/barrier/deliver), why barrier and delivery are excluded from the server total, gRPC/HTTP parity, iterator tree. |

## Related

- [Indexer](../indexer/) — populates the read store the query path consumes.
- [Consensus](../consensus/) — `ReadIndex` quorum that gates default live reads (with stale/checkpoint exceptions).
- [FSM](../fsm/) — durable applied index used as the common projection horizon.
