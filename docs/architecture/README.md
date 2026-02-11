# Architecture Documentation

This directory contains the core architecture and design documentation for Ledger v3 POC.

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

### [Ledgers](./buckets-ledgers.md)
Explanation of the ledger system, transaction management, and data organization.

### [Global Log Architecture](./global-log.md)
Explanation of the two-level log architecture (global log vs ledger log) and how it enables system-level atomic bulk operations.

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

### [Disk Space Limiting](./disk-space-limiting.md)
Cluster-wide disk space monitoring with per-volume thresholds that automatically reject write operations when storage usage is too high.

### [Hybrid Logical Clock](./hybrid-logical-clock.md)
Monotonic timestamp generation using a Hybrid Logical Clock (HLC) in the FSM to guarantee strictly increasing timestamps across leader changes and clock skew.
