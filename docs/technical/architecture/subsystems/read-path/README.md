# Read Path

The CQRS read side (`internal/application/ctrl` reads, `internal/query`, `internal/storage/readstore`). Indexed entity-list reads go through a `ReadIndex` quorum check (linearizability barrier), then iterate over the inverted index in the read store, enriching candidate entity IDs with volumes and metadata from the main store. Point reads and main-store-only queries use their own main-store read path and do not iterate the inverted index.

## Documents

| Document | Description |
|----------|-------------|
| [query-pipeline.md](query-pipeline.md) | End-to-end read flow: ReadIndex barrier, min_log_sequence, Pebble snapshot, iterator algebra, pagination, streaming. |
| [iterator-seek-contract.md](iterator-seek-contract.md) | Absolute SeekGE/SeekLE semantics across the iterator algebra, and the seekFloor/seekCeil exhaustion-proof cache. |
| [read-snapshot-consistency.md](read-snapshot-consistency.md) | Single-snapshot rule for controller reads that stitch LedgerInfo with attribute data. |
| [prepared-queries.md](prepared-queries.md) | Named pre-validated query templates: lifecycle, filter DSL, execution, bloom acceleration. |
| [query-checkpoints.md](query-checkpoints.md) | Point-in-time snapshots of main store and read index for historical queries. |
| [typed-metadata.md](typed-metadata.md) | Typed metadata values, per-ledger schema, and hybrid conversion strategy. |
| [query-filter.md](query-filter.md) | Canonical HTTP QueryFilter surface: dual-format filter, parameter classification, textual/structured asymmetries, date coercion, AND-combination, audit text-only. |

## Related

- [Indexer](../indexer/) — populates the read store the query path consumes.
- [Consensus](../consensus/) — `ReadIndex` quorum that gates indexed entity-list reads.
- [FSM](../fsm/) — what the read path waits to catch up to via `min_log_sequence`.
