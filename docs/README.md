# Technical Documentation - Ledger v3 POC

Welcome to the technical documentation for the Ledger v3 POC project. This documentation provides a comprehensive overview of the architecture, components, and system operation.

## Overview

Ledger v3 POC is a distributed ledger system using the Raft consensus protocol to ensure data consistency across a cluster of nodes. The system uses a **single Raft group** to manage all ledgers and their transactions, with all data stored in a unified storage layer.

### Key Features

- **Distributed Consensus**: Uses etcd/raft for strong consistency across cluster nodes
- **Single Raft Architecture**: All ledgers managed by one Raft group for simplicity and atomic operations
- **Pebble Storage**: High-performance LSM-tree storage engine from CockroachDB
- **Numscript Support**: Full support for Numscript transaction scripting
- **Idempotency**: Built-in idempotency key support for safe retries
- **Bulk Operations**: Process multiple transactions in a single request
- **OpenTelemetry**: Comprehensive observability with traces, metrics, and logs
- **Continuous Profiling**: Pyroscope integration for CPU, memory, and goroutine profiling
- **Request Signing**: Ed25519 request signing for authenticity, integrity, and non-repudiation
- **Maintenance Mode**: Cluster-wide write blocking for planned maintenance with dual enforcement (admission + FSM)
- **Pure Go**: Pebble requires no CGO, enabling easy cross-compilation and minimal Docker images

## Documentation Structure

### 📁 [Architecture](./architecture/)
Core design and technical architecture documentation.

- [Overview](./architecture/architecture.md) - System architecture and component interactions
- [Cluster Lifecycle](./architecture/cluster-lifecycle.md) - Bootstrap, join, synchronization, and learner promotion
- [Raft Consensus](./architecture/raft-consensus.md) - Raft consensus implementation details
- [Deterministic FSM](./architecture/deterministic-fsm.md) - FSM design with generation-based caching and AttributeLoader
- [Attributes](./architecture/attributes.md) - System attributes (volumes, metadata, reversions, idempotency)
- [Ledgers](./architecture/buckets-ledgers.md) - Ledger system and transaction management
- [Global Log](./architecture/global-log.md) - Two-level log architecture
- [Storage](./architecture/storage.md) - Storage systems and persistence
- [HTTP API](./architecture/api.md) - REST API documentation
- [gRPC API](./architecture/grpc-api.md) - gRPC service and client examples
- [Idempotency](./architecture/idempotency.md) - Safe request retries with hash-based conflict detection
- [Uint256 Wire Format](./architecture/uint256-wire-format.md) - Why monetary amounts use fixed-size Uint256 instead of BigInt

### 📁 [Benchmarks](./benchmarks/)
Performance benchmarks and analysis.

- [2026-01-29 Staging](./benchmarks/2026-01-29-staging.md) - Staging environment benchmark

### 📁 [Drafts](./drafts/)
Draft documents, RFCs, and exploratory designs.

- [Problems Solved from v2](./drafts/v2-problems-solved.md) - Issues addressed from v2
- [Limitations](./drafts/limitations.md) - Current system limitations
- [Numscript Static Inputs RFC](./drafts/numscript-static-inputs-rfc.md) - RFC for static inputs

### Operations & Development

| Document | Description |
|----------|-------------|
| [CLI Reference](./cli.md) | CLI client (ledgerctl) documentation |
| [API Comparison](./api-comparison.md) | Feature parity comparison with original ledger |
| [Deployment](./deployment.md) | Deployment guide and Kubernetes configuration |
| [Development](./development.md) | Developer guide and contribution |
| [Dev Environment](./devenv-ledger-exp.md) | Development environment URLs and endpoints |
| [Testing](./testing.md) | Testing strategy and guidelines |
| [Metrics](./metrics.md) | Application metrics reference |
| [Correctness](./correctness.md) | Log integrity, hash chaining, and store verification |
| [Request Signing](./signing.md) | Ed25519 request signing and key management |

### Numscript

| Resource | Description |
|----------|-------------|
| [Numscript Guide](./numscript.md) | Complete guide to Numscript support and features |
| [Numscript Examples](../numscript/examples/README.md) | Example Numscript files for common patterns |

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
| **Pebble** | LSM-tree storage engine (CockroachDB's engine, pure Go) |
| **fx (Uber)** | Dependency injection and lifecycle management |
| **OpenTelemetry** | Observability: traces, metrics, and logs |
| **Pyroscope** | Continuous profiling: CPU, memory, goroutines |

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

See [Storage Drivers](./architecture/storage-drivers.md) for detailed backend comparison.

## Quick Start

To get started quickly with the project, see the [main README](../README.md).

To understand the architecture in depth, start with [Architecture Overview](./architecture/architecture.md).
