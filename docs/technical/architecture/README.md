# Architecture Documentation

25 architecture documents organized into four topic areas.

## Core

Fundamental design: consensus, state machine, and timing guarantees.

| Document | Description |
|----------|-------------|
| [architecture.md](core/architecture.md) | System components, main interactions, and high-level design |
| [raft-consensus.md](core/raft-consensus.md) | Raft consensus implementation and leader management |
| [deterministic-fsm.md](core/deterministic-fsm.md) | Deterministic FSM with generation-based caching and preloading |
| [fsm-cache-layers.md](core/fsm-cache-layers.md) | FSM-side read/write layering: WriteSet → DerivedKeyStore → Plan → KeyStore → AttributeCache |
| [global-log.md](core/global-log.md) | Two-level log architecture enabling system-level atomic bulk operations |
| [hybrid-logical-clock.md](core/hybrid-logical-clock.md) | Monotonic HLC timestamps across leader changes and clock skew |

## Storage

Pebble engine, key layout, caching, and synchronization buffers.

| Document | Description |
|----------|-------------|
| [storage.md](storage/storage.md) | WAL, snapshots, runtime stores, persistence, and recovery |
| [storage-drivers.md](storage/storage-drivers.md) | Pebble storage driver characteristics and configuration |
| [attributes.md](storage/attributes.md) | System attributes (volumes, metadata, reversions, idempotency), storage format, and caching |
| [attribute-key-hashing.md](storage/attribute-key-hashing.md) | U128 hashing scheme for attribute keys and collision detection |
| [spool.md](storage/spool.md) | Committed entry buffer during FSM synchronization |

## Data Model

Ledgers, transactions, chapters, events, and wire formats.

| Document | Description |
|----------|-------------|
| [buckets-ledgers.md](data-model/buckets-ledgers.md) | Ledger system, transaction management, and data organization |
| [data-flows.md](data-model/data-flows.md) | Data flow diagrams for ledger creation, transactions, and other operations |
| [chapters.md](data-model/chapters.md) | Chapter lifecycle (OPEN, CLOSING, CLOSED), sealing hash, and crash recovery |
| [idempotency.md](data-model/idempotency.md) | Idempotency key mechanism, hash-based conflict detection, and storage |
| [typed-metadata.md](data-model/typed-metadata.md) | Typed metadata values, per-ledger schema, and hybrid conversion strategy |
| [uint256-wire-format.md](data-model/uint256-wire-format.md) | Fixed-size Uint256 wire format for monetary amounts |
| [query-checkpoints.md](data-model/query-checkpoints.md) | Point-in-time snapshots of main store and read index for historical queries |
| [indexes.md](data-model/indexes.md) | Index definition, per-replica version state, on-demand statistics, and rewrite lifecycle |
| [indexer.md](data-model/indexer.md) | Indexer pipeline: builder loop, two-pass commit, handlers, read-store key layout, atomic switch |
| [events.md](data-model/events.md) | Domain event types and event sink system (NATS, Kafka, ClickHouse, HTTP) |

## API

Client-facing interfaces and scripting.

| Document | Description |
|----------|-------------|
| [grpc-api.md](api/grpc-api.md) | gRPC service, methods, request/response types, and client examples |
| [grpc-connections.md](api/grpc-connections.md) | gRPC connection mechanics, reconnection, and rolling deployment optimizations |
| [api.md](api/api.md) | HTTP REST API endpoints, response formats, and error handling |
| [numscript-library.md](api/numscript-library.md) | Global repository for reusable numscript programs with semantic versioning |
