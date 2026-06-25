# Ledger Operator

Kubernetes operator for deploying and managing high-availability [Formance Ledger](https://github.com/formancehq/ledger) instances using Raft consensus.

## Overview

The Ledger Operator manages `LedgerService` custom resources to automate the lifecycle of distributed ledger clusters on Kubernetes. It handles:

- **StatefulSet management** with Raft-based consensus (odd replica counts)
- **Persistent storage** for WAL and data volumes
- **Observability** with OpenTelemetry traces, Prometheus metrics, and Pyroscope profiling
- **Security** with TLS, OIDC authentication, and Ed25519 response signing
- **Cold storage** archival to S3-compatible backends
- **Agent credentials** for application-level access control

## Custom Resources

| Resource | Scope | Description |
|----------|-------|-------------|
| `LedgerService` | Namespaced | Main resource - deploys a ledger cluster |
| `LedgerClusterAgent` | Cluster | Cluster-level API credentials |

## Quick Start

### Prerequisites

- Kubernetes cluster (1.28+)
- Helm 3
- [Nix](https://nixos.org/) (optional, for development)

### Install the Operator

```bash
helm install ledger-operator ./chart \
  --namespace ledger-system \
  --create-namespace
```

### Deploy a Ledger Cluster

```yaml
apiVersion: ledger.formance.com/v1alpha1
kind: LedgerService
metadata:
  name: my-ledger
spec:
  replicas: 3
  image:
    repository: ghcr.io/formancehq/ledger
    tag: latest
  config:
    clusterID: default
    pebble:
      memTableSize: 268435456
      cacheSize: 1073741824
  # Cache and bloom parameters are part of the Raft-replicated ClusterConfig.
  # Editing them triggers a rolling restart of the StatefulSet; convergence
  # is deterministic via applyClusterConfig (cache reset + bloom rebuild) and
  # bounded by one election cycle after the last pod restarts.
  cache:
    rotationThreshold: 1000
  bloom:
    volumes:
      expectedKeys: 100000000
      fpRate: "0.01"
    ledgerMetadata:
      expectedKeys: 1000000
      fpRate: "0.001"
    preparedQueries:
      expectedKeys: 1000000
      fpRate: "0.001"
  persistence:
    wal:
      size: 5Gi
    data:
      size: 10Gi
  resources:
    requests:
      cpu: "4000m"
      memory: "2048Mi"
    limits:
      cpu: "4000m"
      memory: "2048Mi"
```

## Helm Values

| Key | Default | Description |
|-----|---------|-------------|
| `image.repository` | `ghcr.io/formancehq/ledger-operator` | Operator image |
| `image.tag` | `latest` | Operator image tag |
| `ledgerImage.registry` | `ghcr.io` | Default ledger image registry |
| `ledgerImage.name` | `formancehq/ledger` | Default ledger image name |
| `ledgerImage.tag` | `latest` | Default ledger image tag |
| `replicaCount` | `1` | Operator replicas |
| `leaderElection` | `true` | Enable HA leader election |
| `watchNamespace` | `""` | Namespace to watch (empty = all) |

## kubectl Plugin

The `kubectl-ledger` plugin provides a CLI for managing LedgerService resources.

### Installation

**From source (requires Go 1.26+):**

```bash
go build -o $(go env GOPATH)/bin/kubectl-ledger ./cmd/kubectl-ledger
```

Or using `just`:

```bash
just install-plugin
```

Once installed, kubectl discovers it automatically:

```bash
kubectl ledger --help
```

### Commands

```
kubectl ledger list [-A]                  # List all LedgerServices
kubectl ledger get <name>                 # Show detailed status
kubectl ledger create <name>              # Create a new LedgerService (interactive)
kubectl ledger delete <name> [-y]         # Delete a LedgerService
kubectl ledger scale <name> --replicas=5  # Scale replicas (must be odd)
kubectl ledger restart <name>             # Rolling restart
kubectl ledger logs <name>                # Stream pod logs
kubectl ledger portforward <name>         # Port-forward to a pod
kubectl ledger config view <name>         # View configuration
kubectl ledger config edit <name>         # Edit configuration
kubectl ledger explain [field.path]       # Explore the CRD schema
kubectl ledger agents list                # List cluster agents
kubectl ledger agents create <name>       # Create agent with API key
kubectl ledger agents get-key <name>      # Retrieve agent API key
kubectl ledger version                    # Print version info
```

### Examples

```bash
# List all ledger services across namespaces
kubectl ledger list -A

# Inspect a specific service
kubectl ledger get my-ledger

# Explore CRD schema for Raft configuration
kubectl ledger explain spec.config.raft

# Create with flags (non-interactive)
kubectl ledger create my-ledger \
  --replicas 5 \
  --image ghcr.io/formancehq/ledger \
  --tag v2.0.0 \
  --cpu 4000m \
  --memory 2048Mi \
  --wal-size 10Gi \
  --data-size 50Gi

# Scale up
kubectl ledger scale my-ledger --replicas 7

# Rolling restart
kubectl ledger restart my-ledger -y
```

## Development

### Setup

The project uses [Nix](https://nixos.org/) for reproducible development environments:

```bash
# Enter the dev shell (automatic with direnv)
nix develop

# Or manually
nix develop --impure
```

### Build & Test

```bash
just build          # Build operator binary
just test           # Run tests
just generate       # Regenerate CRDs, RBAC, and Helm chart
just pre-commit     # Run all checks (generate + tidy + build)
just build-plugin   # Build kubectl plugin
just install-plugin # Install kubectl plugin to $GOPATH/bin
```

### Project Structure

```
cmd/
  operator/          # Operator entrypoint
  kubectl-ledger/    # kubectl plugin
api/v1alpha1/        # CRD type definitions
internal/controller/ # Reconciliation logic
chart/               # Helm chart
config/
  crd/bases/         # Generated CRD manifests
  rbac/              # Generated RBAC rules
  samples/           # Example custom resources
```

## License

Proprietary - Formance
