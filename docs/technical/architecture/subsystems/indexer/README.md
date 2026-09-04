# Indexer

The background workers (`internal/application/indexbuilder` and
`internal/application/auditindexer`) that turn committed main-store logs and
audit entries into queryable read-store keyspaces. They run independently on
every leader and follower and remain outside the FSM hot path. Both retain a
native resume cursor and publish a separate Raft applied-index certificate for
cross-store read alignment.

## Documents

| Document | Description |
|----------|-------------|
| [indexes.md](indexes.md) | Index definition (`commonpb.Index`), per-replica `IndexVersionState`, on-demand statistics, and checker coverage. |
| [indexer.md](indexer.md) | Indexer pipeline: builder loop, two-pass commit, handlers, event-GC scheduling, read-store key layout, atomic switch, schema rewrite. |

## Related

- [Read path](../read-path/) — query consumer of the inverted index.
- [Storage](../storage/) — the read store is a separate Pebble DB with WAL disabled.
- [Attributes](../attributes/) — the same source attributes the indexer projects from.
