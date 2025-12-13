# Technical Documentation - Ledger v3 POC

Welcome to the technical documentation for the Ledger v3 POC project. This documentation provides a comprehensive overview of the architecture, components, and system operation.

## Overview

Ledger v3 POC is a distributed ledger system using the Raft consensus protocol to ensure data consistency across a cluster of nodes. The system allows managing buckets (containers) containing ledgers (accounting books) with financial transactions.

## Documentation Structure

### 📚 [General Architecture](./architecture.md)
Overview of the system architecture, main components, and their interactions.

### 🎯 [Raft Consensus](./raft-consensus.md)
In-depth details on the Raft consensus implementation, multiple Raft groups, and leader management.

### 🪣 [Buckets and Ledgers](./buckets-ledgers.md)
Explanation of the buckets and ledgers system, their hierarchical organization, and transaction management.

### 🔌 [API and Interfaces](./api.md)
Documentation of HTTP and gRPC APIs, service interfaces, and data formats.

### 💾 [Storage and Persistence](./storage.md)
Details on storage systems (WAL, snapshots, log stores), data persistence, and recovery after failures.

### 🚀 [Deployment](./deployment.md)
Deployment guide, Kubernetes/Helm configuration, and environment management.

### 🛠️ [Development](./development.md)
Developer guide: code structure, conventions, testing, and contribution.

### 🔄 [Data Flows](./data-flows.md)
Diagrams and explanations of data flows for main operations (bucket creation, transactions, etc.).

### 🧪 [Testing](./testing.md)
Testing strategy, unit tests, integration, and end-to-end tests.

## Key Concepts

### Buckets
A **bucket** is an isolated container that has its own Raft group. Each bucket can contain multiple ledgers and has its own storage configuration (SQLite).

### Ledgers
A **ledger** is an accounting book containing transactions. Each ledger belongs to a bucket and has a unique identifier within that bucket.

### Transactions
A **transaction** represents an accounting operation with postings (accounting entries) or a Numscript script.

### Raft Groups
The system uses **two levels of Raft groups**:
- **System group**: Manages buckets
- **Bucket groups**: One Raft group per bucket to manage ledgers and transactions

## Technologies Used

- **Go 1.25+** : Main programming language
- **etcd/raft** : Raft consensus library
- **gRPC** : Inter-node communication
- **HTTP/REST** : Public API
- **Protocol Buffers** : Data serialization
- **SQLite**: Transaction log storage
- **fx (Uber)** : Dependency injection
- **OpenTelemetry** : Observability and tracing

## Quick Start

To get started quickly with the project, see the [main README](../README.md).

To understand the architecture in depth, start with [General Architecture](./architecture.md).
