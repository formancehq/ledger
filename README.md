# Ledger v3 POC - Raft Cluster

Distributed ledger system using the Raft consensus protocol to ensure data consistency across a cluster of nodes. The system uses a **single Raft group** to manage all ledgers and their transactions, providing strong consistency and simplified operations.

## Key Features

| Feature | Description |
|---------|-------------|
| **Distributed Consensus** | Uses etcd/raft for strong consistency across cluster nodes |
| **Single Raft Architecture** | All ledgers managed by one Raft group for atomic operations |
| **Multiple Storage Backends** | SQLite (mattn/modern) and Pebble for different use cases |
| **Numscript Support** | Full support for Numscript transaction scripting |
| **Idempotency** | Built-in idempotency key support for safe retries |
| **Bulk Operations** | Process multiple transactions in a single request |
| **OpenTelemetry** | Comprehensive observability with traces, metrics, and logs |
| **Continuous Profiling** | Pyroscope integration for CPU, memory, and goroutine profiling |
| **Pure Go Options** | Pebble and sqlite-modern drivers require no CGO |

## Architecture Overview

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
│  Store (SQLite/ │  │  Store (SQLite/ │  │  Store (SQLite/ │
│   Pebble)       │  │   Pebble)       │  │   Pebble)       │
└─────────────────┘  └─────────────────┘  └─────────────────┘
```

### Storage Backends

| Driver | Library | CGO | Best For |
|--------|---------|-----|----------|
| `sqlite-mattn` | github.com/mattn/go-sqlite3 | Yes | Production (best performance) |
| `sqlite-modern` | modernc.org/sqlite | No | Cross-compilation, Docker scratch |
| `pebble` | github.com/cockroachdb/pebble | No | High-throughput workloads |

## Documentation

For detailed technical documentation, architecture overview, API reference, and development guides, see the [Technical Documentation](./docs/README.md).

For a summary of problems from Ledger v2 that have been solved in this version, see [Problems Solved from v2](./docs/v2-problems-solved.md).

For a comparison of the write API between this POC and the original ledger (`github.com/formancehq/ledger`), including missing and intentionally removed features, see the [API Comparison](./docs/api-comparison.md).


## Prerequisites

- Go 1.25 or higher (provided by Nix)
- Just (command runner) - [Installation](https://github.com/casey/just)
- Nix with Flakes enabled (required)

## Installation

### With Nix

This project uses Nix flakes for a reproducible development environment. We recommend using `direnv` to automatically load the environment when entering the project directory.

**Prerequisites:**
- Install [direnv](https://direnv.net/) and [hook it into your shell](https://direnv.net/docs/hook.html)
- Install Nix with Flakes enabled

**Setup:**

```bash
# Generate flake.lock file (first time only)
nix flake update

# Allow direnv to load the environment (first time only)
direnv allow
```

After setup, `direnv` will automatically load the Nix development environment whenever you `cd` into the project directory. All dependencies (Go, Just, etc.) will be available in your shell.

**Note:** The `flake.lock` file will be automatically generated on first use and should be committed to ensure build reproducibility.

## Usage

### Local mode (single node)

```bash
# Start a single node with default settings
just run

# Or manually with specific storage driver
go run . run \
  --node-id 1 \
  --bind-addr 127.0.0.1:8888 \
  --data-dir ./data/node-1 \
  --http-port 9000 \
  --storage-type pebble  # or sqlite-mattn, sqlite-modern
```

### Storage Configuration

Choose your storage backend based on your needs:

```bash
# SQLite with CGO (best performance, requires C compiler)
./ledger-v3-poc run --storage-type sqlite-mattn

# SQLite pure Go (no CGO, works with scratch Docker images)
./ledger-v3-poc run --storage-type sqlite-modern

# Pebble (high-throughput LSM-tree, no CGO)
./ledger-v3-poc run --storage-type pebble
```

### Development Environment with Pulumi

Deploy a complete development environment on Kubernetes using Pulumi, including the Ledger v3 POC application and the full observability stack (Grafana, VictoriaMetrics, Loki, Tempo, Pyroscope, OpenTelemetry Collector, and k6-operator).

**Available Stacks** (managed under the `formance` organization on Pulumi Cloud):

| Stack | Description |
|-------|-------------|
| `devenv-ledger-exp` | Development environment on Waays (Tailscale) |
| `staging` | Staging environment on AWS (formance.cloud) |

**Prerequisites:**
- [Pulumi CLI](https://www.pulumi.com/docs/get-started/install/) installed
- Access to the target Kubernetes cluster with `kubectl` configured
- Go 1.21 or higher

**Quick Start:**

```bash
# Navigate to the Pulumi application directory
cd misc/devenv

# Install Go dependencies
go mod download

# Login to Pulumi (if not already done)
pulumi login

# Select an existing stack
pulumi stack select formance/ledger-exp-devenv/devenv-ledger-exp
# or
pulumi stack select formance/ledger-exp-devenv/staging

# Preview the deployment
pulumi preview

# Deploy the stack
pulumi up
```

**Configuration:**

The deployment configuration is stored in `Pulumi.<stack>.yaml` (e.g., `Pulumi.staging.yaml`). You can customize:
- Kubernetes context and namespace
- Docker registry settings
- Optional components (k6-operator, benchmark-operator)
- Helm values for each service (VictoriaMetrics, Grafana, Loki, Tempo, OTLP, Ledger)
- Grafana dashboards and datasources

**Accessing Services:**

After deployment, services are available on the **ledger-exp** cluster:

**Public URLs (via Ingress):**
- **Grafana**: https://grafana-ledger-exp.v2.formance.dev
- **Ledger API**: https://ledger-exp.v2.formance.dev

**Internal Services (via kubectl port-forward):**
- **Ledger API**: `kubectl port-forward -n monitoring svc/ledger-v3-poc 9000:9000` then http://localhost:9000
- **VictoriaMetrics**: `kubectl port-forward -n monitoring svc/vm-victoria-metrics-single-server 8428:8428` then http://localhost:8428
- **Loki**: `kubectl port-forward -n monitoring svc/loki 3100:3100` then http://localhost:3100
- **Tempo**: `kubectl port-forward -n monitoring svc/tempo 3200:3200` then http://localhost:3200

**Destroying the Environment:**

```bash
# Remove all resources
pulumi destroy
```

**Creating a new environment:**

See the [Pulumi Development Environment README](misc/devenv/README.md) for detailed instructions on creating a new environment.

For other deployment options and Kubernetes configuration, see the [Deployment Guide](./docs/deployment.md).

## API Quick Reference

### Ledger Operations

```bash
# Create a ledger
curl -X POST http://localhost:9000/my-ledger \
  -H "Content-Type: application/json" \
  -d '{"metadata": {"description": "My ledger"}}'

# Create a transaction
curl -X POST http://localhost:9000/my-ledger/transactions \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: unique-key-123" \
  -d '{
    "postings": [
      {"source": "world", "destination": "bank", "amount": 100, "asset": "USD"}
    ]
  }'

# Check cluster state
curl http://localhost:9000/cluster/state
```

### API Versioning

All endpoints support an optional `/v2` prefix for future versioning:
- `GET /` and `GET /v2/` are equivalent
- `POST /{ledger}/transactions` and `POST /v2/{ledger}/transactions` are equivalent

For complete API documentation, see the [API Reference](./docs/api.md).

## Development

```bash
# Build the application
just build

# Run tests
just test

# Run end-to-end tests
just test-e2e

# Generate protobuf code
just generate-proto

# Generate SDK from OpenAPI
just generate-sdk

# Clean build artifacts
just clean
```

**Note:** With `direnv` configured, the development environment is automatically loaded when you enter the project directory. All dependencies (Go, Just, etc.) are available in your shell.

For more information about development, see the [Development Guide](./docs/development.md).
