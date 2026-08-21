# Read Path

The CQRS read side (`internal/application/ctrl` reads, `internal/query`, `internal/storage/readstore`). Every read goes through a `ReadIndex` quorum check (linearizability barrier), then iterates over the inverted index in the read store, enriching candidate entity IDs with volumes and metadata from the main store.

## Documents

| Document | Description |
|----------|-------------|
| [query-pipeline.md](query-pipeline.md) | End-to-end read flow: ReadIndex barrier, min_log_sequence, Pebble snapshot, iterator algebra, pagination, streaming. |
| [iterator-seek-contract.md](iterator-seek-contract.md) | Absolute SeekGE/SeekLE semantics across the iterator algebra, and the seekFloor/seekCeil exhaustion-proof cache. |
| [read-snapshot-consistency.md](read-snapshot-consistency.md) | Single-snapshot rule for controller reads that stitch LedgerInfo with attribute data. |
| [prepared-queries.md](prepared-queries.md) | Named pre-validated query templates: lifecycle, filter DSL, execution, bloom acceleration. |
| [query-checkpoints.md](query-checkpoints.md) | Pre-created applied-state snapshots of the main store and read index. |
| [historical-balances.md](historical-balances.md) | Contract, data flow, Pebble key/value layout, client configuration, and fail-closed behavior for monetary balance history. |
| [historical-balances-performance.md](historical-balances-performance.md) | Cost model, trade-offs, benchmark command, and performance acceptance criteria. |
| [typed-metadata.md](typed-metadata.md) | Typed metadata values, per-ledger schema, and hybrid conversion strategy. |
| [query-filter.md](query-filter.md) | Canonical HTTP QueryFilter surface: dual-format filter, parameter classification, textual/structured asymmetries, date coercion, AND-combination, audit text-only. |

## Related

- [Indexer](../indexer/) — populates the read store the query path consumes.
- [Consensus](../consensus/) — `ReadIndex` quorum that gates every read.
- [FSM](../fsm/) — what the read path waits to catch up to via `min_log_sequence`.
