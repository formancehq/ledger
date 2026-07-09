# Events & Mirror

Two leader-only background workers that tail the global log.

- **Events** (`internal/application/events`) — emits domain events (transaction created, metadata changed, etc.) to external sinks: NATS, Kafka, ClickHouse, Databricks, HTTP webhooks. At-least-once delivery, Raft-replicated cursors.
- **Mirror** (`internal/application/mirror`) — runs per-ledger ingest from an external source (HTTP or PostgreSQL) and translates the source's logs into `MirrorIngest` Raft commands.

## Documents

| Document | Description |
|----------|-------------|
| [events.md](events.md) | Domain event types and event sink system (NATS, Kafka, ClickHouse, HTTP). |
| [mirror.md](mirror.md) | Mirror worker that ingests Ledger v2 logs (HTTP or PostgreSQL) into a v3 mirror ledger, with promotion to normal mode at cutover. |
| [cel-rewrite.md](cel-rewrite.md) | CEL rewrite engine that transforms transactions during mirror translation (rename addresses, transform metadata, drop transactions). |

## Related

- [Consensus](../consensus/) — both workers tail the Raft log, both checkpoint via Raft.
