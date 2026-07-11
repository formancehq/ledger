# Event System Specification

This document specifies the event system for Ledger v3, providing at-least-once delivery of domain events derived from the global log.

## Overview

The event system emits domain events for every mutation committed to the ledger. Events are **derived from the global log** (not published inline during the write path), ensuring at-least-once delivery with no event loss across restarts or leader changes.

### Design Principles

1. **At-least-once delivery**: Every committed log produces exactly one event. After crashes or leader changes, events may be re-delivered but never lost.
2. **Ordered**: Events are emitted in global log sequence order. Consumers can rely on the `sequence` field for ordering and deduplication.
3. **Leader-only**: Only the Raft leader emits events, avoiding duplicates across cluster nodes.
4. **Decoupled from write path**: Event emission does not block or slow down the Raft consensus/apply path.
5. **Configurable format**: Event payloads are serialized as JSON or Protobuf, configured per-sink.

## Event Types

Events map directly to the global log payload types:

| Event Type | Trigger | Log Payload |
|---|---|---|
| `COMMITTED_TRANSACTION` | Transaction created | `CreatedTransaction` |
| `REVERTED_TRANSACTION` | Transaction reverted | `RevertedTransaction` |
| `SAVED_METADATA` | Metadata saved on account or transaction | `SavedMetadata` |
| `DELETED_METADATA` | Metadata key deleted | `DeletedMetadata` |
| `CREATED_LEDGER` | Ledger created | `CreatedLedgerLog` |
| `DELETED_LEDGER` | Ledger deleted | `DeletedLedgerLog` |
| `SKIPPED_ORDER` | Batch order skipped on a whitelisted business failure (`skippable_reasons`) | `OrderSkippedLog` (carries `skippedReason` + `skippedContext`) |

### Compatibility with Reference Implementation

The event types align with `github.com/formancehq/ledger` where applicable:

| Reference Event | v3 Event | Notes |
|---|---|---|
| `COMMITTED_TRANSACTIONS` | `COMMITTED_TRANSACTION` | Singular (one event per log) |
| `SAVED_METADATA` | `SAVED_METADATA` | Identical semantics |
| `REVERTED_TRANSACTION` | `REVERTED_TRANSACTION` | Identical semantics |
| `DELETED_METADATA` | `DELETED_METADATA` | Identical semantics |
| `INSERTED_SCHEMA` | `CREATED_LEDGER` | Renamed (no schema concept in v3) |
| _(none)_ | `DELETED_LEDGER` | New in v3 |

## Event Structure

### Envelope

Every event shares a common envelope:

```protobuf
message Event {
  EventType type = 1;                // Event type enum
  string ledger = 2;                 // Ledger name (empty for system events like CREATED_LEDGER)
  common.Timestamp date = 3;         // Event timestamp (HLC)
  fixed64 log_sequence = 4;          // Global log sequence (monotonic, unique)
  common.Log log = 5;                // Full log entry with payload
}
```

The event carries the full `Log` entry, which contains the typed payload (transaction, metadata change, etc.) in its `LedgerLogPayload` oneof. This avoids duplicating payload definitions and ensures events always carry the complete log data.

### JSON Format

When `format=json` in the events config, the event is serialized as JSON using `protojson`. Property names follow protobuf JSON mapping conventions (camelCase):

```json
{
  "type": "COMMITTED_TRANSACTION",
  "ledger": "orders",
  "date": "2026-02-18T10:30:00.000Z",
  "logSequence": "42",
  "log": {
    "data": {
      "createdTransaction": {
        "transaction": {
          "id": "7",
          "postings": [
            {
              "source": "world",
              "destination": "user:123",
              "amount": { "lo": "1000" },
              "asset": "USD/2"
            }
          ],
          "metadata": {},
          "timestamp": "2026-02-18T10:30:00.000Z",
          "reference": "order-456"
        }
      }
    },
    "date": "2026-02-18T10:30:00.000Z"
  }
}
```

### Protobuf Format

When `format=protobuf` in the events config, the event is serialized as binary protobuf using vtprotobuf (`MarshalVT`) for zero-allocation performance. Consumers must use the `Event` proto definition to deserialize.

## Architecture

### Components: Manager and Emitters

The event system consists of two main components in `internal/application/events/`:

- **Manager**: Manages the lifecycle of **one Emitter per named sink** based on the Raft-replicated per-sink configurations. It reconciles state on leadership changes and config updates, and fans out log notifications to all active emitters.
- **Emitter**: Tails the global log from PebbleDB, converts logs to events, publishes them to its associated sink, and advances its per-sink cursor via Raft. On publish failure, it reports the error via Raft so it is visible cluster-wide.

```
┌───────────────────────────────────────────────────────────────────────┐
│                          Raft Node (Leader)                            │
├───────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  AddEventsSink ───► Raft ──► FSM ──► PebbleDB ([0x01][0x08][name])     │
│                                  │                                     │
│                          config change signal                          │
│                                  ▼                                     │
│                         ┌────────────────┐                             │
│                         │    Manager     │ ◄── leadership change        │
│                         └───────┬────────┘                             │
│                                 │ reconcile()                          │
│                                 │ (one emitter per named sink)         │
│                                 ▼                                      │
│  FSM applies entry ──► logs committed to PebbleDB                      │
│                              │                                         │
│                              │  log notify (fan-out to all emitters)   │
│                     ┌────────┴────────┐                                │
│                     ▼                 ▼                                 │
│              ┌────────────────────┐  ┌────────────────────┐              │
│              │ Emitter "a"        │  │ Emitter "b"        │  ...        │
│              │ cursor: [06][08]   │  │ cursor: [06][08]   │              │
│              │ status: [06][0A]   │  │ status: [06][0A]   │              │
│              └──────────┬─────────┘  └──────────┬─────────┘              │
│                     ▼                 ▼                                 │
│              ┌────────────┐    ┌────────────┐                          │
│              │ NATS "a"   │    │ Kafka "b"  │                          │
│              └────────────┘    └────────────┘                          │
│                                                                        │
└───────────────────────────────────────────────────────────────────────┘
```

Each emitter tracks its own cursor and error status independently. A failing sink does not block other sinks from making progress.

### Per-Sink Cursor Persistence

Each sink has its own cursor (last successfully emitted sequence) stored in PebbleDB under a dedicated key prefix and **replicated via Raft**. After publishing a batch, the emitter proposes a lightweight Raft command (a `Proposal` with `EventsSinkUpdate`) to advance the cursor and optionally clear any previous error. The FSM writes the cursor to the PebbleDB batch alongside normal state, ensuring all nodes share the same per-sink cursor positions.

This ensures:

- **Crash recovery**: On restart, each emitter resumes from its own last persisted cursor.
- **Leader change**: A new leader reads the Raft-replicated per-sink cursors and only emits events beyond them.
- **Independent progress**: A slow or failing sink does not block other sinks from advancing.
- **At-least-once**: Since the cursor is advanced after sink acknowledgment, a crash between publish and cursor commit may re-emit a small number of events (bounded by batch size).

Key formats:
- Per-sink cursor: `[0x06][0x08][sink_name]` (`ZoneGlobal` + `SubGlobSinkCursor`) → `uint64` (big-endian encoded sequence)
- Per-sink error status: `[0x06][0x0A][sink_name]` (`ZoneGlobal` + `SubGlobSinkStatus`) → `SinkStatus` protobuf

### Sink Error Status

When a sink publish fails, the emitter reports the error via Raft by proposing an `EventsSinkUpdate` with the error details. The FSM stores a `SinkStatus` protobuf under `[0x06][0x0A][sink_name]` (`ZoneGlobal` + `SubGlobSinkStatus`). On subsequent successful publish, the emitter proposes an update with `clear_error = true`, which deletes the status entry.

The `GetEventsSinks` gRPC endpoint returns all sink configs and their statuses, allowing operators to monitor sink health cluster-wide:

```protobuf
message SinkStatus {
  string sink_name = 1;
  fixed64 cursor = 2;        // Last published sequence
  SinkError error = 3;       // Most recent error (nil = healthy)
}

message SinkError {
  string message = 1;
  Timestamp occurred_at = 2;
}
```

### Notification Mechanism

To minimize latency, the FSM notifies the `EventEmitter` via a Go channel after applying entries. The emitter:

1. Receives notification that new logs are available
2. Reads logs from PebbleDB starting at `cursor + 1`
3. Converts each log to an event
4. Publishes to the sink
5. Advances the cursor

If no notification arrives (e.g., after restart), the emitter polls at a configurable interval as fallback.

### Leader-Only Emission

The event system is gated by both the node's leader status and the presence of sink configs:

- **On becoming leader**: The `Manager.OnLeadershipChange(true)` is called. If any sinks are configured, the Manager starts one Emitter per sink.
- **On losing leadership**: The `Manager.OnLeadershipChange(false)` is called. The Manager tears down all Emitters and Sinks.
- **On config change (while leader)**: The FSM signals the Manager via a `Signal` notification. The Manager reconciles by diffing the desired sink configs against the currently running emitters — only sinks that were added, removed, or changed are affected; unchanged sinks keep running.
- **Followers**: Never emit events, regardless of config.

This is managed via the existing `SoftState` leader detection in the Raft node, with the `Manager` acting as the orchestrator.

### Emitter Delivery Flow

```mermaid
sequenceDiagram
    participant FSM as FSM (Leader)
    participant Mgr as Manager
    participant E as Emitter
    participant Sink as External Sink
    participant Raft as Raft

    Note over Mgr: On leadership change
    Mgr->>Mgr: reconcile() — start/stop emitters<br/>based on Raft-replicated sink configs

    FSM->>Mgr: NotifyLogsCommitted(lastSeq)
    Mgr->>E: signal (non-blocking)

    E->>E: ReadLogsSince(cursor)
    E->>E: Filter by event types, batch by size
    E->>Sink: Publish(events)

    alt Success
        E->>Raft: proposeSinkUpdate(cursor, clearError)
        Raft->>FSM: Persist cursor at {0x06, 0x08}
    else Failure
        E->>Raft: reportError(message, timestamp)
        Raft->>FSM: Persist error at {0x06, 0x0A}
        E->>E: Retry with backoff
    end
```

The Manager reconciles emitter lifecycles on leadership changes and config updates. Each emitter independently tails the log, publishes to its sink, and advances its cursor via Raft. Failed publishes are recorded as sink errors in Pebble (visible via `GetEventsSinks`), and the emitter retries with exponential backoff.

## Sink Interface

The sink is the pluggable transport layer for event delivery:

```go
// Sink publishes events to an external system.
// Implementations must be safe for concurrent use.
type Sink interface {
    // Publish sends events to the external system.
    // Returns an error if any event could not be delivered.
    // Events are provided in sequence order.
    Publish(ctx context.Context, events []*eventspb.Event) error

    // Close releases resources held by the sink.
    Close() error
}
```

### Batching

The emitter batches events for efficiency:

- **Max batch size**: Configurable (default: 64 events)
- **Max batch delay**: Configurable (default: 10ms)
- **Flush triggers**: Batch full, delay expired, or shutdown signal

The cursor is advanced only after the entire batch is successfully published to the sink.

### Sink Implementations

| Sink | Config Value | Description |
|---|---|---|
| **NATS JetStream** | `nats` | Publishes to NATS JetStream for durable consumption. Requires `url` and `topic`. |
| **ClickHouse** | `clickhouse` | Inserts events into a ClickHouse table for analytics. Requires `dsn`. Optional `table` (default: `ledger_events`). Uses experimental JSON type with Variant support. Always uses ClickHouse-native JSON format (ignores `format` setting). Table uses `ReplacingMergeTree` keyed on `(ledger, log_sequence)`: because delivery is at-least-once, a redelivered batch produces byte-identical duplicate rows that this engine collapses on background merges. Dedup is eventual, so exact queries must use `FINAL` or `GROUP BY`. |
| **Kafka** | `kafka` | Publishes to Apache Kafka topics. Requires `brokers` and `topic`. Optional TLS and SASL authentication (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512). Uses synchronous producer for delivery guarantees. |
| **HTTP Webhooks** | `http` | Sends individual HTTP POST requests per event. Requires `endpoint` URL. Optional `secret` for HMAC-SHA256 request signing (`X-Webhook-Signature` header). |
| **Databricks** | `databricks` | Inserts events into a Databricks SQL Warehouse table via Unity Catalog. Requires `server_hostname`, `http_path`, `catalog`, `schema`, `table`, and exactly one auth method (`token` for a Personal Access Token, or `oauth_m2m` with `client_id`/`client_secret` for OAuth M2M / service principal). |

New sink types can be added by implementing the `Sink` interface and adding a variant to `SinkConfig.oneof type`.

### Topic/Subject Mapping

Events are published to a configurable topic/subject per sink type:

- **NATS**: Configurable via `topic` field in `NatsSinkConfig`.
- **Kafka**: Configurable via `topic` field in `KafkaSinkConfig`.
- **HTTP**: Events are sent to the configured `endpoint` URL.
- **ClickHouse**: Events are inserted into the configured `table`.
- **Databricks**: Events are inserted into the configured `catalog.schema.table`.

## Configuration

### Raft-Replicated Configuration via gRPC

The event system is configured at runtime via gRPC RPCs that go through Raft consensus, providing:

- **Runtime configurability**: Change settings without restarting the service
- **Cluster-wide consistency**: Config is replicated to all nodes via Raft
- **Granular add/remove**: Add or remove individual sinks without affecting others
- **Implicit enable/disable**: Having at least one sink = events enabled; zero sinks = events disabled

#### SinkConfig Message

```protobuf
message SinkConfig {
  string name = 1;                       // Stable identifier for per-sink cursor/status keys
  oneof type {
    NatsSinkConfig nats = 2;             // NATS JetStream sink
    ClickHouseSinkConfig clickhouse = 6; // ClickHouse analytics sink
    KafkaSinkConfig kafka = 7;           // Apache Kafka sink
    HttpSinkConfig http = 8;             // HTTP webhook sink
    DatabricksSinkConfig databricks = 10; // Databricks SQL Warehouse sink
  }
  string format = 3;                     // "json" or "protobuf" (default: "json")
  int32 batch_size = 4;                  // Max events per batch (default: 64)
  int64 batch_delay_ms = 5;              // Max delay before flush in ms (default: 10)
  repeated EventType event_types = 9;    // Empty = all events (default)
}

message NatsSinkConfig {
  string url = 1;                // NATS server URL
  string topic = 2;              // Topic/subject for events
}

message ClickHouseSinkConfig {
  string dsn = 1;                // e.g. "clickhouse://user:pass@host:9000/db"
  string table = 2;              // Table name (default: "ledger_events")
}

message KafkaSinkConfig {
  repeated string brokers = 1;   // e.g. ["localhost:9092"]
  string topic = 2;              // Kafka topic name
  bool tls = 3;                  // Enable TLS
  string sasl_mechanism = 4;     // SASL mechanism: "", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"
  string sasl_username = 5;
  string sasl_password = 6;
}

message HttpSinkConfig {
  string endpoint = 1;           // Target URL (e.g. "https://example.com/webhooks/ledger")
  string secret = 2;             // Optional HMAC-SHA256 secret for X-Webhook-Signature header
}

message DatabricksSinkConfig {
  string server_hostname = 1;    // e.g. "adb-123456.azuredatabricks.net"
  string http_path = 2;          // SQL Warehouse HTTP path (e.g. "/sql/1.0/warehouses/abc123")
  string catalog = 4;            // Unity Catalog name (e.g. "main")
  string schema = 5;             // Schema name (e.g. "default")
  string table = 6;              // Table name (default: "ledger_events")
  int32 port = 7;                // Port number (default: 443)
  oneof auth {                   // Exactly one auth method must be set
    string token = 3;                  // Personal Access Token (PAT)
    DatabricksOAuthM2M oauth_m2m = 8;  // OAuth M2M / service principal
  }
}

message DatabricksOAuthM2M {
  string client_id = 1;
  string client_secret = 2;
}
```

Each `SinkConfig` carries its own `format`, `batch_size`, `batch_delay_ms`, and `event_types` — there is no global events config. The Manager creates **one Emitter per named sink**, each with its own cursor (`[0x06][0x08][name]`) and status (`[0x06][0x0A][name]`). Sinks progress independently — a failing sink does not block others. New sink types can be added as additional variants in the `SinkConfig.oneof type`.

#### Adding and Removing Sinks

Use `AddEventsSink` and `RemoveEventsSink` via the `Apply` RPC:

```bash
# Add a NATS sink with default settings
ledgerctl events add-sink --name primary --nats-url nats://localhost:4222 --nats-topic ledger.events

# Add a NATS sink with custom batch settings and protobuf format
ledgerctl events add-sink --name primary --nats-url nats://localhost:4222 --nats-topic ledger.events \
  --format protobuf --batch-size 128 --batch-delay-ms 50

# Add a ClickHouse sink for analytics
ledgerctl events add-sink --name analytics --ch-dsn clickhouse://user:pass@localhost:9000/db

# Add a ClickHouse sink with custom table name
ledgerctl events add-sink --name analytics --ch-dsn clickhouse://user:pass@localhost:9000/db --ch-table my_events

# Add a Kafka sink
ledgerctl events add-sink --name streaming --kafka-brokers localhost:9092 --kafka-topic ledger-events

# Add a Kafka sink with SASL authentication
ledgerctl events add-sink --name streaming --kafka-brokers broker1:9092,broker2:9092 --kafka-topic ledger-events \
  --kafka-tls --kafka-sasl-mechanism SCRAM-SHA-256 --kafka-sasl-username user --kafka-sasl-password pass

# Add an HTTP webhook sink
ledgerctl events add-sink --name webhook --http-endpoint https://example.com/webhooks/ledger

# Add an HTTP webhook sink with HMAC signature verification
ledgerctl events add-sink --name webhook --http-endpoint https://example.com/webhooks/ledger --http-secret my-secret

# Remove a sink (events implicitly disabled when all sinks removed)
ledgerctl events remove-sink --name streaming
```

#### Reading Sink Configuration

Use `ledgerctl events list` to read all sink configurations and per-sink statuses:

```bash
ledgerctl events list
```

The response includes a list of `SinkConfig` entries and a list of `SinkStatus` entries showing each sink's cursor position and any active error.

#### Config Persistence

Each sink config is stored in PebbleDB under key `[0x01][0x08][canonical_key]` (`ZoneAttributes` + `SubAttrSinkConfig`) and replicated via Raft. Sink configs follow the same **admission preload** pattern as other attributes (volumes, ledgers, etc.): they are cached in the dual-generation `AttributeCache` and preloaded from PebbleDB on demand by the admission layer when not guaranteed in cache. This avoids PebbleDB reads on the FSM hot path. On cache generation rotation, evicted sink configs are re-preloaded when next needed.

The `Manager` component watches for config changes via a channel from the FSM and reconciles the emitters/sinks accordingly.

### Data Flow

```
Client → gRPC Apply(AddEventsSinkRequest) → Admission → Raft consensus
  → FSM Apply → WriteSet.Merge() → PebbleDB ([0x01][0x08][canonical_key])
  → config change signal → Manager.reconcile()
  → Start/Stop Emitter + Sink

Client → gRPC Apply(RemoveEventsSinkRequest) → Admission → Raft consensus
  → FSM Apply → WriteSet.Merge() → PebbleDB (delete [0x01][0x08][canonical_key])
  → config change signal → Manager.reconcile()
  → Tear down Emitter + Sink
```

## At-Least-Once Guarantee

### How It Works

```
                     Committed Log (PebbleDB)
                     ┌─────────────────────────────────────┐
                     │ seq=1 │ seq=2 │ seq=3 │ seq=4 │ ... │
                     └───────────────┬─────────────────────┘
                                     │
            Cursor (persisted) ──►   │ seq=2
                                     │
                           ┌─────────┴─────────┐
                           │  EventEmitter      │
                           │  reads seq=3,4,... │
                           │  publishes batch   │
                           │  advances cursor   │
                           └─────────┬──────────┘
                                     │
                                     ▼
                              Sink (NATS/Kafka/HTTP/ClickHouse/Databricks)
```

1. **Normal operation**: Emitter tails the log, publishes events, advances cursor via Raft.
2. **Crash before cursor advance**: On restart, emitter replays from old cursor → at most one batch re-delivered (at-least-once).
3. **Leader change**: New leader reads the Raft-replicated cursor and resumes from where the old leader left off, avoiding re-emission of the entire history.

### Consumer Deduplication

Consumers should use the `sequence` field for idempotent processing:

```
if event.sequence <= lastProcessedSequence {
    skip  // Already processed
}
```

The global sequence is monotonically increasing and unique, making it a natural deduplication key.

## Integration Points

### Write Path (No Changes)

The write path (API → Controller → Admission → Raft → FSM → PebbleDB) is **not modified**. Events are derived asynchronously from committed logs.

### FSM Notification Hook

The FSM notifies the event system via a `signal.Notifications` struct (from `internal/pkg/signal/`) shared between the FSM and the Manager. Both the events and mirror Managers receive their own `*signal.Notifications` instance, disambiguated via fx named tags (`name:"events"` / `name:"mirror"`). Notifications use a coalescing `Signal` type (buffered channel of size 1) — multiple rapid notifications are collapsed into a single wake-up, and the send is always non-blocking:

```go
// After committing a batch with new logs:
fsm.notifier.NotifyLogsCommitted(lastSeq)
if eventsConfigChanged {
    fsm.notifier.NotifyConfigChanged()
}
```

The FSM emits to a single `Notifier` interface; the bootstrap layer wires a `signal.FanOut` that dispatches to three independent `*signal.Notifications` instances: `events`, `mirror`, and `index`. The `signal.Notifications` struct is created independently (no dependency on Node or Manager) to break the fx circular dependency between the Node (which contains the FSM) and the Manager (which needs the Node as a Proposer).

### fx Integration

The event system is wired into the application via `bootstrap.Module()`:

```go
// internal/bootstrap/module.go
fx.Provide(
    fx.Annotate(signal.NewNotifications, fx.ResultTags(`name:"events"`)),
    fx.Annotate(events.NewManager, fx.ParamTags(``, ``, ``, `name:"events"`)),
)
fx.Invoke(func(lc fx.Lifecycle, manager *events.Manager) {
    // Start/Stop the Manager background goroutine
})
```

Named fx tags (`name:"events"` / `name:"mirror"` / `name:"index"`) disambiguate the three `*signal.Notifications` instances without requiring wrapper types. The Manager only activates emitters when the node becomes leader AND at least one sink is configured.

## Package Structure

```
internal/application/events/
  manager.go                            # Manager: one emitter per named sink, lifecycle, smart reconcile
  emitter.go                            # Emitter: tails log, converts, publishes, per-sink cursor
  sink.go                               # Sink interface definition
  sink_nats.go                          # NATS JetStream sink
  sink_clickhouse.go                    # ClickHouse analytics sink
  sink_kafka.go                         # Apache Kafka sink
  sink_http.go                          # HTTP webhook sink
  sink_databricks.go                    # Databricks SQL Warehouse sink
  sink_data_common.go                   # Shared data sink utilities (ClickHouse/Databricks)
  registry.go                           # Sink registry (maps config type to constructor)
  event.go                              # Event type, conversion from Log, serialization
  manager_test.go                       # Manager lifecycle and config change tests
  emitter_test.go                       # Emitter lifecycle tests
  emitter_integration_test.go           # Emitter integration tests with PebbleDB
  event_test.go                         # LogToEvent + serialization tests
  sink_nats_integration_test.go         # NATS sink integration tests
  sink_clickhouse_integration_test.go   # ClickHouse sink integration tests
  sink_kafka_integration_test.go        # Kafka sink integration tests
  sink_http_integration_test.go         # HTTP sink integration tests

misc/proto/events.proto                 # Event protobuf definition
internal/proto/eventspb/                # Generated protobuf code
```

## Observability

### Metrics

| Metric | Type | Description |
|---|---|---|
| `ledger.events.emitted` | Counter | Total events emitted (by type) |
| `ledger.events.errors` | Counter | Total emission errors (by sink) |
| `ledger.events.lag` | Gauge | Sequence lag (head - cursor) |
| `ledger.events.batch_size` | Histogram | Events per batch |
| `ledger.events.latency` | Histogram | Time from log commit to event emission |

### Logging

- `INFO`: Emitter started/stopped, cursor position on startup
- `WARN`: Sink publish error (will retry)
- `DEBUG`: Each batch published (sequence range, count)

## Sequence Diagram: Normal Flow

```mermaid
sequenceDiagram
    participant Client
    participant API
    participant Admission
    participant Raft
    participant FSM
    participant PebbleDB
    participant Emitter as EventEmitter
    participant Sink as Sink (NATS/Kafka/HTTP/ClickHouse/Databricks)

    Client->>API: CreateTransaction
    API->>Admission: Admit()
    Admission->>Raft: Propose
    Raft->>FSM: Apply entry
    FSM->>PebbleDB: Commit batch (log seq=42)
    FSM->>Emitter: notify (channel)
    FSM-->>Admission: Future resolves (log)
    Admission-->>API: Log
    API-->>Client: Response

    Note over Emitter: Async, non-blocking
    Emitter->>PebbleDB: Read logs since cursor (seq=41)
    PebbleDB-->>Emitter: [Log seq=42]
    Emitter->>Emitter: Convert to Event
    Emitter->>Sink: Publish([Event seq=42])
    Sink-->>Emitter: ACK
    Emitter->>Raft: Propose EventsSinkUpdate(cursor=42, clear_error)
    Raft->>FSM: Apply → PebbleDB [0x06][0x08][sink_name] = 42
```

## Sequence Diagram: Crash Recovery

```mermaid
sequenceDiagram
    participant PebbleDB
    participant Emitter as EventEmitter
    participant Sink as Sink (NATS/Kafka/HTTP/ClickHouse/Databricks)

    Note over Emitter: Node restarts, becomes leader
    Emitter->>PebbleDB: Read cursor
    PebbleDB-->>Emitter: cursor = seq=40

    Emitter->>PebbleDB: Read logs since seq=40
    PebbleDB-->>Emitter: [Log seq=41, seq=42, seq=43]

    Note over Emitter: Re-emit (at-least-once)
    Emitter->>Sink: Publish([Event seq=41, 42, 43])
    Sink-->>Emitter: ACK
    Emitter->>Raft: Propose EventsSinkUpdate(cursor=43, clear_error)
    Raft->>FSM: Apply → PebbleDB [0x06][0x08][sink_name] = 43

    Note over Emitter: Switch to live tailing mode
```

## Future Considerations

- **Exactly-once semantics**: Could be achieved with transactional sinks (e.g., Kafka transactions) combined with the sequence-based deduplication.
- **Event filtering**: Allow consumers to subscribe to specific event types or ledgers.
- **Event replay API**: A gRPC endpoint to replay events from a given sequence (useful for bootstrapping new consumers).
- **Backpressure**: If the sink is slow, the emitter should apply backpressure (bounded buffer) rather than consuming unbounded memory.

### HTTP Webhook Details

The HTTP sink sends each event as an individual HTTP POST request with the following headers:

| Header | Description |
|---|---|
| `Content-Type` | `application/json` or `application/protobuf` depending on format setting |
| `X-Event-Type` | Event type (e.g. `committed_transaction`) |
| `X-Ledger` | Ledger name |
| `X-Log-Sequence` | Global log sequence number |
| `X-Webhook-Signature` | `sha256=<hex>` HMAC-SHA256 signature (only when `secret` is configured) |

### ClickHouse Sink Details

The ClickHouse sink auto-creates the target table using the experimental JSON type with Variant support (ClickHouse 24.x-25.x compatibility). The `format` setting is ignored — events are always inserted as ClickHouse-native JSON for optimal query performance.
