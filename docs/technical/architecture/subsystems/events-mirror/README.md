# Events & Mirror

Two leader-only background workers that tail the global log.

- **Events** (`internal/application/events`) — emits domain events (transaction created, metadata changed, etc.) to external sinks: NATS, Kafka, ClickHouse, Databricks, HTTP webhooks. At-least-once delivery, Raft-replicated cursors.
- **Mirror** (`internal/application/mirror`) — runs per-ledger ingest from an external source (HTTP or PostgreSQL) and translates the source's logs into `MirrorIngest` Raft commands.

## Documents

| Document | Description |
|----------|-------------|
| [events.md](events.md) | Domain event types, event sinks, nested ClickHouse account metadata and table upgrades, NATS ledger-name token encoding, and HTTP acknowledgement and redirect rules. |
| [mirror.md](mirror.md) | Mirror worker, PostgreSQL visibility and permanent-gap synchronization, source prerequisites and sequence-cache transition evidence, safe HTTP URL and PostgreSQL DSN parsing diagnostics, ascending HTTP continuation, v2 source payload identity contract, and promotion at cutover. |
| [cel-rewrite.md](cel-rewrite.md) | CEL rewrite engine that transforms transactions during mirror translation (rename addresses, transform metadata, drop transactions). |

Mirror [source initialization and cancellation](mirror.md#source-initialization-and-cancellation)
documents generation ownership, shutdown ordering, and source cleanup when
PostgreSQL startup is blocked.

Events [sink initialization and cancellation](events.md#sink-initialization-and-cancellation-en-2921)
documents generation-scoped construction, cleanup ownership, and the remaining
driver cancellation limits during leadership loss and shutdown.

## Related

- [Consensus](../consensus/) — both workers tail the Raft log, both checkpoint via Raft.
