# Events & Mirror

Two leader-only background workers that tail the global log, plus an inbound
acknowledged gRPC log subscription.

- **Events** (`internal/application/events`) — emits domain events (transaction created, target-scoped metadata saved, account metadata updated by Numscript, etc.) to external sinks: NATS, Kafka, ClickHouse, Databricks, HTTP webhooks. At-least-once delivery, Raft-replicated cursors. Created-ledger events use a detached credential-redacted log projection; see [events](events.md#event-structure).
- **Mirror** (`internal/application/mirror`) — runs per-ledger ingest from an external source (HTTP or PostgreSQL) and translates the source's logs into `MirrorIngest` Raft commands.
- **gRPC log sink** (`internal/adapter/grpc/server_bucket_log_sink.go`) — a configured client opens a bidirectional stream for one ledger, receives committed `common.Log` values, and advances a separate Raft-replicated cursor only after acknowledgement.

## Documents

| Document | Description |
|----------|-------------|
| [events.md](events.md) | Domain event types, event sinks, exact ClickHouse UInt256 amount transport, nested account metadata and table upgrades, NATS ledger-name token encoding, and HTTP acknowledgement and redirect rules. |
| [mirror.md](mirror.md) | Mirror worker, PostgreSQL visibility and permanent-gap synchronization, source prerequisites and sequence-cache transition evidence, safe HTTP URL and PostgreSQL DSN parsing diagnostics, HTTP transport diagnostics for parseable and malformed redirects, ascending HTTP continuation, v2 source payload identity contract, asset mappings and stored-asset inventory, and promotion at cutover. |
| [cel-rewrite.md](cel-rewrite.md) | CEL rewrite engine that transforms transactions during mirror translation (rename addresses, transform metadata, drop transactions). |

Mirror [source initialization and cancellation](mirror.md#source-initialization-and-cancellation)
documents generation ownership, shutdown ordering, and source cleanup when
PostgreSQL startup is blocked.

Events [sink initialization and cancellation](events.md#sink-initialization-and-cancellation-en-2921)
documents generation-scoped construction, cleanup ownership, and the remaining
driver cancellation limits during leadership loss and shutdown.

## Related

- [Consensus](../consensus/) — both workers tail the Raft log, both checkpoint via Raft.
