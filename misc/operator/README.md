# Ledger Operator

Kubernetes operator for deploying and managing high-availability [Formance Ledger](https://github.com/formancehq/ledger) instances using Raft consensus.

## Overview

The Ledger Operator manages `Cluster` custom resources to automate the lifecycle of distributed ledger clusters on Kubernetes. It handles:

- **StatefulSet management** with Raft-based consensus (odd replica counts)
- **Persistent storage** for WAL and data volumes
- **Observability** with OpenTelemetry traces, Prometheus metrics, and Pyroscope profiling
- **Security** with TLS, OIDC authentication, and Ed25519 response signing
- **Cold storage** archival to S3-compatible backends
- **Credentials** for application-level access control

## Custom Resources

| Resource | Scope | Description |
|----------|-------|-------------|
| `Cluster` | Namespaced | Main resource - deploys a ledger cluster |
| `Credentials` | Cluster | Cluster-level API credentials |

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
kind: Cluster
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
| `pvcProtection.enabled` | `true` | Install the cluster-scoped ValidatingAdmissionPolicy that blocks accidental deletion of ledger PVCs/PVs (requires Kubernetes >= 1.30). On by default; set `false` to opt the cluster out. Arming only — a ledger is selected via `spec.persistence.deletionProtection` (also on by default) |
| `pvcProtection.allowDeletionAnnotation` | `formance.com/allow-deletion` | Annotation key whose value `true` opts a volume out of deletion protection |
| `pvcProtection.additionalExemptServiceAccounts` | `[]` | Extra ServiceAccount usernames (`system:serviceaccount:<ns>:<name>`) exempt from the policies — sibling operator releases managing protected ledgers, or managed workload/GitOps controllers |

## Volume Deletion Protection

Protection is **on by default**: a freshly deployed ledger is protected without
any extra configuration, and both layers below must be explicitly turned off to
opt out. Deletion protection has three independent layers, so the choice of
*which* ledgers are protected lives with the ledger owner (per-CR), while
*whether the mechanism exists at all* stays a cluster-admin decision:

1. **`pvcProtection.enabled` (Helm value, cluster-admin consent).** Installs two
   cluster-scoped `ValidatingAdmissionPolicy` objects (`failurePolicy: Fail`) that
   reject `DELETE` of selected ledger PVCs/PVs. **On by default** but **requires
   Kubernetes >= 1.30** (ValidatingAdmissionPolicy GA). On an older cluster the
   chart detects that the `ValidatingAdmissionPolicy` kind is absent and **skips
   these objects** so the default install/upgrade still succeeds (it prints a
   NOTES warning); a Cluster with `deletionProtection: true` then reports the
   runtime `DeletionProtectionInactive` warning because no policy acts on its
   volumes. `helm template` run offline uses Helm's built-in capability list, so
   pass `--api-versions admissionregistration.k8s.io/v1/ValidatingAdmissionPolicy`
   to force-render the policy there. Installing the policy does **not** protect
   anything on its own — the policy bindings only select volumes carrying the
   `ledger.formance.com/deletion-protection: enabled` label.

   The policy is a **cluster-wide singleton** — enable `pvcProtection.enabled` on
   **at most one** operator release per cluster. The cluster-scoped policy objects
   have fixed, release-independent names, so a second release with
   `pvcProtection.enabled=true` fails its `helm install`/`upgrade` with an ownership
   conflict by design, rather than installing a second policy that would cross-apply
   to and block legitimate deletes on the first release's volumes. **Because the value
   now defaults to `true`, in a multi-release cluster you must set
   `pvcProtection.enabled=false` on all but one release** — otherwise the second
   install fails. In a multi-release cluster where *other* releases also manage
   ledgers with `deletionProtection: true`, list those releases' operator
   ServiceAccounts in `pvcProtection.additionalExemptServiceAccounts` on the owning
   release, so their operators' scale-down deletes are not blocked by the singleton
   policy.
2. **`spec.persistence.deletionProtection` (per-Cluster, default `true`).**
   Protected by default: the operator stamps that label on the ledger's PVCs and
   their bound PVs, so the cluster policy selects them. Set it explicitly to `false`
   to opt out — the label is removed and protection is lifted. This is versioned
   alongside the ledger and toggleable without a `helm upgrade`.
3. **`formance.com/allow-deletion=true` annotation (per-volume override).** A
   protected volume can still be deleted on purpose by annotating it first.

To delete a protected volume on purpose:

```bash
kubectl annotate pvc <name> formance.com/allow-deletion=true --overwrite
kubectl delete pvc <name>
```

If a Cluster sets `deletionProtection: true` while no cluster-scoped protection
policy is installed on the cluster, the label is still stamped but no policy acts on it;
the operator surfaces this as a `DeletionProtectionInactive` warning event and status
condition on the CR rather than silently leaving the volumes unprotected. The operator
detects this by probing for the policy's `ValidatingAdmissionPolicyBinding` directly, so
the condition stays correct in a multi-release cluster: a sibling release with
`pvcProtection.enabled=false` whose ledgers are protected by the owning release's
singleton policy is **not** falsely warned.

Exemptions: the operator ServiceAccount (its own raft scale-down deletes) and the
kube-controller-manager garbage collector are exempt, so the StatefulSet
`retentionPolicy: Delete` path (`persistence.retentionPolicy.whenScaled` /
`whenDeleted=Delete`) continues to work with protection enabled.

No other identity is exempt. A workload/GitOps controller (ArgoCD, Flux, Velero
restore, etc.) that deletes a protected `Cluster` **and** its PVCs/PVs in a
single managed teardown runs under its own ServiceAccount, so once
`pvcProtection.enabled=true` those deletes are blocked just like a manual one. To
allow such a teardown, either annotate the volumes with the allow-deletion key
first (as above) or, for a recurring controller, add its ServiceAccount username to
`pvcProtection.additionalExemptServiceAccounts` (full form
`system:serviceaccount:<namespace>:<name>`), which appends it to the policies'
`matchConditions` exemptions.

The PV policy only protects **Bound** PVs (volumes holding live ledger data). Once
a PVC is deleted — which itself goes through the PVC policy above — its PV becomes
`Released` and the reclaim path proceeds normally: with `persistentVolumeReclaimPolicy:
Delete` (the default for most cloud StorageClasses) the PV controller / CSI
external-provisioner deletes the volume without being blocked, and with `Retain` an
admin can delete the orphaned `Released` PV directly. Deleting a live, Bound PV by
hand is still rejected unless it carries the allow-deletion annotation. (A PV that is
orphaned in the `Released` state keeps the protection label it last held, because the
operator only reconciles the label on live PVCs; this is harmless since the policy
guards Bound PVs only.)

## kubectl Plugin

The `kubectl-ledger` plugin provides a CLI for managing Cluster resources.

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
kubectl ledger list [-A]                  # List all Clusters
kubectl ledger get <name>                 # Show detailed status
kubectl ledger create <name>              # Create a new Cluster (interactive)
kubectl ledger delete <name> [-y]         # Delete a Cluster
kubectl ledger scale <name> --replicas=5  # Scale replicas (must be odd)
kubectl ledger restart <name>             # Rolling restart
kubectl ledger logs <name>                # Stream pod logs
kubectl ledger portforward <name>         # Port-forward to a pod
kubectl ledger config view <name>         # View configuration
kubectl ledger config edit <name>         # Edit configuration
kubectl ledger explain [field.path]       # Explore the CRD schema
kubectl ledger credentials list           # List cluster credentials
kubectl ledger credentials create <name>  # Create credentials with API key
kubectl ledger credentials get-key <name> # Retrieve credentials API key
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
