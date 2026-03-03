# Architecture Documentation

Core architecture and design documentation for Ledger v3 POC.

## Documents

### [Architecture Overview](./overview.md)
**Start here!** High-level overview with Mermaid diagrams showing component interactions, package dependencies, entity relationships, and data flows.

### [Architecture Details](./architecture.md)
General system architecture, main components, and their interactions.

### [Raft Consensus](./raft-consensus.md)
In-depth details on the Raft consensus implementation and leader management.

### [Deterministic FSM](./deterministic-fsm.md)
Technical documentation on the deterministic Finite State Machine design with generation-based caching, preloading, and concurrent load coordination (AttributeLoader).

### [Attributes](./attributes.md)
System attributes (volumes, metadata, reversions, idempotency keys), their storage format, computation rules, and caching.

### [Attribute Key Hashing](./attribute-key-hashing.md)
U128 hashing scheme for attribute keys and collision detection.

### [Ledgers](./buckets-ledgers.md)
Explanation of the ledger system, transaction management, and data organization.

### [Global Log Architecture](./global-log.md)
Two-level log architecture (global log vs ledger log) and how it enables system-level atomic bulk operations.

### [Spool](./spool.md)
Technical documentation for the Spool component (committed entry buffer during FSM synchronization).

### [Data Flows](./data-flows.md)
Diagrams and explanations of data flows for main operations (ledger creation, transactions, etc.).

### [Storage](./storage.md)
Details on storage systems (WAL, snapshots, runtime stores), data persistence, and recovery after failures.

### [Storage Drivers](./storage-drivers.md)
Detailed documentation on the Pebble storage driver, its characteristics, configuration, and use cases.

### [HTTP API](./api.md)
Documentation of HTTP REST API, endpoints, response formats, and error handling.

### [gRPC API](./grpc-api.md)
Documentation of gRPC service, methods, request/response types, and client examples.

### [gRPC Connections](./grpc-connections.md)
gRPC connection mechanics, reconnection strategies, and optimizations for rolling deployments.

### [Idempotency](./idempotency.md)
Idempotency key mechanism for safe request retries, hash-based conflict detection, and storage architecture.

### [Periods](./periods.md)
Period lifecycle (OPEN -> CLOSING -> CLOSED), two-step close process with background sealing, BLAKE3 sealing hash computation, crash recovery, and JWT transaction receipts.

### [Events](./events.md)
Domain event types and event sink system (NATS, Kafka, ClickHouse, HTTP).

### [Hybrid Logical Clock](./hybrid-logical-clock.md)
Monotonic timestamp generation using a Hybrid Logical Clock (HLC) in the FSM to guarantee strictly increasing timestamps across leader changes and clock skew.

### [Uint256 Wire Format](./uint256-wire-format.md)
Why monetary amounts use fixed-size Uint256 instead of BigInt.

### [Typed Metadata](./typed-metadata.md)
Typed metadata values (string, int64, uint64, bool, NullValue), per-ledger metadata schema declaration, type conversion matrix, and hybrid conversion strategy (lazy reads + automatic background batches).

### [Numscript Library](./numscript-library.md)
Global repository for storing, retrieving, and managing reusable numscript programs with semantic versioning. Covers version resolution, script references in transactions, storage layout, and admission preloading.

## Operations-Related Architecture

These documents have been moved to the [Operations Guide](../../ops/) for better discoverability by sysadmins:

- [Backup and Restore](../../ops/backup-restore.md) - Backup pipeline and restore workflow
- [Cluster Operations](../../ops/cluster-operations.md) - Bootstrap, join, synchronization, and learner promotion
- [Disk Space Limiting](../../ops/disk-space.md) - Cluster-wide disk space monitoring and write rejection
