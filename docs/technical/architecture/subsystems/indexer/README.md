# Indexer

## Ephemeral account purge

`LedgerLog.purged_accounts` is the durable boundary between atomic primary
deletion and the per-replica read projection. For each address, the indexer
removes current account metadata memberships (forward, existence, and reverse
limbs) and has-asset rows in the same local batch that advances projection.
Has-asset writes maintain an account-first purge companion, so lifecycle cleanup
scans only the purged account's asset cells rather than the ledger-wide
asset-first query index. This guarantees atomic publication within the read
projection. An
aligned snapshot may use a projection already ahead of its main-store pin, so
the has-asset exception and its mixed-horizon implications remain as documented
in [read-consistency-projections.md](../../../audits/read-consistency-projections.md).

Account-to-transaction, source, and destination mappings are immutable history
and are deliberately retained. Physical deletion across the main and read-store
Pebble databases is asynchronous; atomicity is expressed by the committed purge
signal and by withholding aligned progress until its local cascade commits.

The background workers (`internal/application/indexbuilder` and
`internal/application/auditindexer`) that turn committed main-store logs and
audit entries into queryable read-store keyspaces. They run independently on
every leader and follower and remain outside the FSM hot path. Both retain a
native resume cursor and publish a separate Raft applied-index certificate for
cross-store read alignment. `InspectIndex` is a projection consumer under the
same contract: it certifies the fixed main horizon, resolves the servable index
version at that pin, and ignores membership events committed after it.

## Documents

| Document | Description |
|----------|-------------|
| [indexes.md](indexes.md) | Strict index creation, index definition (`commonpb.Index`), durable EMPTY/NON_EMPTY ledger-history classification, per-replica `IndexVersionState`, on-demand statistics, and checker coverage. |
| [indexer.md](indexer.md) | Indexer pipeline: builder loop, atomic tracker/version/task/cursor commits, boot recovery, handlers, event-GC scheduling, read-store and field/version-first reverse-map key layouts, atomic switch, schema rewrite. |

## Related

- [Read path](../read-path/) — query consumer of the inverted index.
- [Storage](../storage/) — the read store is a separate Pebble DB with WAL disabled.
- [Attributes](../attributes/) — the same source attributes the indexer projects from.
