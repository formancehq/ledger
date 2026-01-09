# Ledger v3 POC - Raft Cluster

Distributed ledger system using the Raft consensus protocol to ensure data consistency across a cluster of nodes. The system allows managing ledgers (accounting books) with financial transactions, where each ledger has its own independent Raft group.


## Documentation

For detailed technical documentation, architecture overview, API reference, and development guides, see the [Technical Documentation](./docs/README.md).


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
# Start a single node
just run

# Or manually
go run ./cmd/server --node-id node-1 --bind-addr 127.0.0.1:8888 --data-dir ./data/node-1
```

For detailed deployment options and Kubernetes configuration, see the [Deployment Guide](./docs/deployment.md).

## Development

```bash
# Build the application
just build

# Run tests
just test

# Clean build artifacts
just clean
```

**Note:** With `direnv` configured, the development environment is automatically loaded when you enter the project directory. All dependencies (Go, Just, etc.) are available in your shell.

For more information about development, see the [Development Guide](./docs/development.md).
