# Technical Documentation - Ledger v3 POC

Welcome to the technical documentation for the Ledger v3 POC project. This documentation provides a comprehensive overview of the architecture, components, and system operation.

## Overview

Ledger v3 POC is a distributed ledger system using the Raft consensus protocol to ensure data consistency across a cluster of nodes. The system uses a **single Raft group** to manage all ledgers and their transactions, with all data stored in a unified storage layer.

### Key Features

- **Distributed Consensus**: Uses etcd/raft for strong consistency across cluster nodes
- **Single Raft Architecture**: All ledgers managed by one Raft group for simplicity and atomic operations
- **Multiple Storage Backends**: SQLite (mattn/modern) and Pebble for different use cases
- **Numscript Support**: Full support for Numscript transaction scripting
- **Idempotency**: Built-in idempotency key support for safe retries
- **Bulk Operations**: Process multiple transactions in a single request
- **OpenTelemetry**: Comprehensive observability with traces, metrics, and logs
- **Pure Go Options**: Pebble and sqlite-modern drivers require no CGO (sqlite-mattn requires CGO)

## Documentation Structure

### 📚 [General Architecture](./architecture.md)
Overview of the system architecture, main components, and their interactions.

### 🎯 [Raft Consensus](./raft-consensus.md)
In-depth details on the Raft consensus implementation and leader management.

### 📖 [Ledgers](./buckets-ledgers.md)
Explanation of the ledger system, transaction management, and data organization.

### 🔌 [API and Interfaces](./api.md)
Documentation of HTTP and gRPC APIs, service interfaces, and data formats.

### 🔄 [API Comparison](./api-comparison.md)
Comparison of the write API between this POC and the original ledger (`github.com/formancehq/ledger`), including missing and intentionally removed features.

### 💾 [Storage and Persistence](./storage.md)
Details on storage systems (WAL, snapshots, runtime stores), data persistence, and recovery after failures.

### 🗄️ [Storage Drivers](./storage-drivers.md)
Detailed documentation on each storage driver (SQLite mattn, SQLite modern, Pebble), their characteristics, configuration, and use cases.

### 🚀 [Deployment](./deployment.md)
Deployment guide, Kubernetes/Helm configuration, and environment management.

### 🌐 [Dev Environment URLs](./devenv-ledger-exp.md)
Useful URLs and endpoints for the `ledger-exp` development environment.

### 🛠️ [Development](./development.md)
Developer guide: code structure, conventions, testing, and contribution.

### 🔄 [Data Flows](./data-flows.md)
Diagrams and explanations of data flows for main operations (ledger creation, transactions, etc.).

### 🧪 [Testing](./testing.md)
Testing strategy, unit tests, integration, and end-to-end tests.

### 📊 [Metrics](./metrics.md)
Application metrics reference: Raft, transport, storage, and alerting recommendations.

### 📝 [Spool](./spool.md)
Technical documentation for the Spool component (committed entry buffer).

## Key Concepts

### Ledgers
A **ledger** is an accounting book containing transactions. All ledgers are managed by a single Raft group and share the same storage.

### Transactions
A **transaction** represents an accounting operation with postings (accounting entries) or a Numscript script. This project uses the new Numscript interpreter and does not allow runtime selection.

### Single Raft Architecture
The system uses a **single Raft group** that manages:
- **Ledger operations**: Create and delete ledgers
- **Transaction operations**: Create transactions, save metadata, revert transactions for all ledgers

This architecture simplifies operations while maintaining strong consistency guarantees.

## Technologies Used

| Technology | Purpose |
|------------|---------|
| **Go 1.25+** | Main programming language |
| **etcd/raft** | Raft consensus library (same as used by etcd) |
| **gRPC** | Inter-node communication and request forwarding |
| **HTTP/REST** | Public API with OpenAPI specification |
| **Protocol Buffers** | Data serialization for Raft entries and storage |
| **SQLite** | Relational storage option (mattn: CGO, modern: pure Go) |
| **Pebble** | LSM-tree storage option (CockroachDB's engine, pure Go) |
| **fx (Uber)** | Dependency injection and lifecycle management |
| **OpenTelemetry** | Observability: traces, metrics, and logs |
| **Speakeasy** | SDK generation from OpenAPI specification |

## Storage Architecture

The system provides a unified `Store` interface with multiple backend implementations:

```go
type Store interface {
    // Log operations
    AppendLogs(ctx, lastAppliedIndex, logs...) error
    GetAllLogs(ctx, ledger, from, to) (Cursor, error)
    GetLogByID(ctx, ledger, id) (*Log, error)
    
    // Runtime queries
    GetBalances(ctx, ledger, query) (Balances, error)
    GetAccountMetadata(ctx, ledger, accounts) (Metadata, error)
    
    // Idempotency and transaction tracking
    GetLogIDForIdempotencyKey(ctx, ledger, key) (uint64, error)
    GetLogIDForTransactionID(ctx, ledger, txID) (uint64, error)
    IsTransactionReverted(ctx, ledger, txID) (bool, error)
    
    // Lifecycle and snapshots
    CreateSnapshot(ctx) error
    GetLastAppliedIndex() (uint64, error)
    DeleteLedger(name) error
    Close(ctx) error
}
```

See [Storage Drivers](./storage-drivers.md) for detailed backend comparison.

## Quick Start

To get started quickly with the project, see the [main README](../README.md).

To understand the architecture in depth, start with [General Architecture](./architecture.md).
