# Ledger v3 POC - Raft Cluster

Proof of concept for a basic Raft cluster, inspired by the guidelines from the [formancehq/ledger](https://github.com/formancehq/ledger) repository.

## Description

This application allows you to start a Raft cluster with multiple nodes to test distributed consensus. The project uses HashiCorp Raft for the consensus protocol implementation.

## Project Structure

```
.
├── cmd/
│   └── server/          # Application entry point
├── internal/
│   ├── config/          # Configuration management
│   └── raft/            # Raft cluster implementation
├── Dockerfile           # Docker image for the application
├── docker-compose.yml   # Multi-node cluster configuration
├── flake.nix            # Nix configuration to pin dependency versions
├── justfile             # Useful commands
└── go.mod               # Go dependencies
```

## Prerequisites

- Go 1.25 or higher (or use Nix for a reproducible environment)
- Just (command runner) - [Installation](https://github.com/casey/just)
- Docker and Docker Compose (for multi-node cluster)
- Nix with Flakes enabled (optional, for reproducible environment)

## Installation

### With Nix (recommended)

```bash
# Generate flake.lock file (first time only)
nix flake update

# Enter Nix development environment
nix develop

# Build the application
nix build

# The binary will be available in ./result/bin/ledger-v3-poc
```

**Note:** The `flake.lock` file will be automatically generated on first use and should be committed to ensure build reproducibility.

### Without Nix

```bash
# Download dependencies
go mod download
```

## Usage

### Local mode (single node)

```bash
# Start a single node
just run

# Or manually
go run ./cmd/server --node-id node-1 --bind-addr 127.0.0.1:8888 --data-dir ./data/node-1
```

### Multi-node cluster with Docker

The Docker Compose configuration uses the current source code directly (mounted as a volume) rather than building an image. This allows for live development and testing.

```bash
# Start the cluster (3 nodes)
just docker-up

# View logs
just docker-logs

# Stop the cluster
just docker-down
```

### Configuration Options

The application can be configured via:
- Command line arguments
- Environment variables (no prefix, use underscores instead of hyphens)

Available options:
- `--node-id` / `NODE_ID`: Unique node identifier (required)
- `--bind-addr` / `BIND_ADDR`: Listen address (default: `127.0.0.1:8888`)
- `--advertise-addr` / `ADVERTISE_ADDR`: Address to advertise to other nodes (defaults to bind-addr)
- `--data-dir` / `DATA_DIR`: Data storage directory (default: `./data`)
- `--peers` / `PEERS`: List of peer addresses (comma-separated)
- `--debug` / `DEBUG`: Enable debug logging (default: `false`)
- `--bootstrap` / `BOOTSTRAP`: Bootstrap the cluster (only set on the first node, default: `false`)

## Architecture

### FSM (Finite State Machine)

The application implements a minimal FSM that satisfies the Raft interface requirements. The FSM currently has no business logic and simply logs applied entries.

### Storage

- **Log Store**: Stores Raft log entries (BoltDB)
- **Stable Store**: Stores stable metadata (BoltDB)
- **Snapshot Store**: Stores filesystem snapshots

## Development

### With Nix

```bash
# Enter development environment
nix develop

# All dependencies (Go, Just, etc.) are available
just build
just test
```

### Without Nix

```bash
# Build the application
just build

# Run tests
just test

# Clean build artifacts
just clean
```

### Development Environment

The `flake.nix` provides a reproducible development environment with:
- Go 1.25
- Just
- gopls (Language Server Protocol for Go)
- gotools and go-tools (Go development tools)

To use it:
```bash
nix develop
```

## Notes

- The first node must use the `--bootstrap` flag to bootstrap the cluster
- Subsequent nodes must specify addresses of existing peers
- The cluster requires a majority of nodes to function (3 nodes = tolerance to 1 failure)
