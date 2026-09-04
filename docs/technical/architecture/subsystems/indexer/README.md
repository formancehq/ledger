# Indexer

The background worker (`internal/application/indexbuilder`) that turns committed audit logs into the inverted, queryable keyspaces consumed by the read API. Runs on every node — leader and followers — independently. Decoupled from the FSM hot path: the FSM commits and signals; the indexer reads from its own Pebble read handle and writes to the read store.

## Documents

| Document | Description |
|----------|-------------|
| [indexes.md](indexes.md) | Index definition (`commonpb.Index`), per-replica `IndexVersionState` with its schema-revision binding and the one-revision serving window, on-demand statistics, and checker coverage. |
| [indexer.md](indexer.md) | Indexer pipeline: builder loop, two-pass commit, handlers, event-GC scheduling, read-store key layout, atomic switch, schema rewrite. A binding more than one schema revision behind (a rewound read store mid-rebuild) reads as retryable `INDEX_BUILDING`, forwarded from followers to the leader. |

## Related

- [Read path](../read-path/) — query consumer of the inverted index.
- [Storage](../storage/) — the read store is a separate Pebble DB with WAL disabled.
- [Attributes](../attributes/) — the same source attributes the indexer projects from.
