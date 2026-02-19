# Ledger v3 POC - Raft Cluster

Distributed ledger system using the Raft consensus protocol to ensure data consistency across a cluster of nodes. The system uses a **single Raft group** to manage all ledgers and their transactions, providing strong consistency and simplified operations.

## Key Features

| Feature | Description |
|---------|-------------|
| **Distributed Consensus** | Uses etcd/raft for strong consistency across cluster nodes |
| **Single Raft Architecture** | All ledgers managed by one Raft group for atomic operations |
| **Pebble Storage** | High-performance LSM-tree storage engine (CockroachDB), pure Go |
| **Numscript Support** | Full support for Numscript transaction scripting |
| **Idempotency** | Built-in idempotency key support for safe retries |
| **Bulk Operations** | Process multiple transactions in a single request |
| **Observability** | OpenTelemetry (traces, metrics, logs) + Pyroscope profiling |
| **Request Signing** | Ed25519 request signing for authenticity and integrity |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Client Applications                      │
│              (HTTP REST / gRPC / CLI Client)                │
└─────────────────────────────────────────────────────────────┘
                              │
         ┌────────────────────┼────────────────────┐
         ▼                    ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│     Node 1      │  │     Node 2      │  │     Node 3      │
│   (Leader)      │  │   (Follower)    │  │   (Follower)    │
├─────────────────┤  ├─────────────────┤  ├─────────────────┤
│ HTTP :9000      │  │ HTTP :9000      │  │ HTTP :9000      │
│ gRPC :8888      │  │ gRPC :8888      │  │ gRPC :8888      │
├─────────────────┤  ├─────────────────┤  ├─────────────────┤
│  Single Raft    │◄─┼─Raft Protocol──►│◄─┤  Single Raft    │
│     Group       │  │                 │  │     Group       │
├─────────────────┤  ├─────────────────┤  ├─────────────────┤
│ FSM (All Ledgers)│ │ FSM (All Ledgers)│ │ FSM (All Ledgers)│
├─────────────────┤  ├─────────────────┤  ├─────────────────┤
│  Store (Pebble) │  │  Store (Pebble) │  │  Store (Pebble) │
└─────────────────┘  └─────────────────┘  └─────────────────┘
```

## Quick Start

**Prerequisites:** Go 1.25+, [Just](https://github.com/casey/just), Nix with Flakes enabled.

```bash
# Setup (first time only)
nix flake update && direnv allow

# Start a single node
just run
```

For cluster deployment and Kubernetes, see the [Deployment Guide](./docs/ops/deployment.md).

## Development

```bash
just build           # Build the application
just test            # Run tests
just test-e2e        # Run end-to-end tests
just generate-proto  # Generate protobuf code
just pre-commit      # Run all checks (generate, tidy, lint)
```

For more details, see the [Developer Guide](./docs/dev/).

## Documentation

| Guide | Audience |
|-------|----------|
| [Operations](./docs/ops/) | Deploy, monitor, backup, CLI reference |
| [Development](./docs/dev/) | Architecture, conventions, testing |
| [Product Overview](./docs/sales/) | Features, benchmarks, v2 vs v3 |
| [Drafts & RFCs](./docs/drafts/) | Experimental designs |

See [docs/README.md](./docs/README.md) for full navigation.
