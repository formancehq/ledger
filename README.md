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

### Development Environment with Pulumi

Deploy a complete development environment on Kubernetes using Pulumi, including the Ledger v3 POC application and the full observability stack (Grafana, VictoriaMetrics, Loki, Tempo, OpenTelemetry Collector, and k6-operator).

**Note:** The Pulumi stack `gfyrag` deploys to the **ledger-exp** development environment cluster.

**Prerequisites:**
- [Pulumi CLI](https://www.pulumi.com/docs/get-started/install/) installed
- Access to the ledger-exp Kubernetes cluster with `kubectl` configured
- Go 1.21 or higher

**Quick Start (stack: `gfyrag`):**

```bash
# Navigate to the Pulumi application directory
cd misc/devenv

# Install Go dependencies
go mod download

# Login to Pulumi (if not already done)
pulumi login

# Initialize or select the gfyrag stack
pulumi stack init gfyrag
# or
pulumi stack select gfyrag

# Preview the deployment
pulumi preview

# Deploy the stack
pulumi up
```

**Configuration:**

The deployment configuration is stored in `Pulumi.gfyrag.yaml`. You can customize:
- Namespace (default: `monitoring`)
- Helm values for each service (VictoriaMetrics, Grafana, Loki, Tempo, OTLP, Ledger, k6-operator)
- Grafana dashboards and datasources

**Accessing Services:**

After deployment, services are available on the **ledger-exp** cluster:

**Public URLs (via Ingress):**
- **Grafana**: https://grafana.ledger-exp.v2.formance.dev
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

**Creating a new Pulumi stack:**

```bash
# Create a new stack
pulumi stack init <stack-name>

# Copy the default config and adjust values
cp Pulumi.gfyrag.yaml Pulumi.<stack-name>.yaml

# Edit Pulumi.<stack-name>.yaml and update registry/values as needed

# Select the new stack and deploy
pulumi stack select <stack-name>
pulumi preview
pulumi up
```

For detailed documentation, see the [Pulumi Development Environment README](misc/devenv/README.md).

For other deployment options and Kubernetes configuration, see the [Deployment Guide](./docs/deployment.md).

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
